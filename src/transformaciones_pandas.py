"""Transformaciones pandas equivalentes para la práctica PE-U4.

El módulo solo define T1--T5. No lee archivos ni ejecuta transformaciones al
importarse, de modo que la carga y la medición puedan controlarse externamente.
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


FECHA_INICIO_T1 = "2024-07-01"
FECHA_FIN_T1 = "2025-06-30"
MINIMO_PARTICIPANTES_T1 = 25
TOP_N = 20


def _requerir_columnas(
    dataframe: pd.DataFrame, columnas: Iterable[str], transformacion: str
) -> None:
    """Genera un error claro si el DataFrame no contiene el esquema requerido."""
    faltantes = sorted(set(columnas).difference(dataframe.columns))
    if faltantes:
        raise ValueError(
            f"{transformacion} requiere columnas ausentes: {', '.join(faltantes)}"
        )


def t1_filtrado_compuesto(solicitudes: pd.DataFrame) -> pd.DataFrame:
    """T1: selecciona solicitudes aprobadas, numerosas y en el periodo definido.

    Condición exacta:
      estado == APROBADA
      AND numero_participantes >= 25
      AND fecha_reserva entre 2024-07-01 y 2025-06-30, inclusive.
    """
    columnas_salida = [
        "solicitud_id",
        "laboratorio_id",
        "fecha_reserva",
        "hora_inicio",
        "hora_fin",
        "numero_participantes",
        "estado",
    ]
    _requerir_columnas(solicitudes, columnas_salida, "T1")

    fechas = pd.to_datetime(solicitudes["fecha_reserva"], errors="raise")
    condicion = (
        solicitudes["estado"].eq("APROBADA")
        & solicitudes["numero_participantes"].ge(MINIMO_PARTICIPANTES_T1)
        & fechas.ge(pd.Timestamp(FECHA_INICIO_T1))
        & fechas.le(pd.Timestamp(FECHA_FIN_T1))
    )
    return solicitudes.loc[condicion, columnas_salida].copy()


def t2_agregaciones_por_laboratorio(
    solicitudes: pd.DataFrame,
) -> pd.DataFrame:
    """T2: agrupa por laboratorio y calcula conteo, promedio y máximo."""
    _requerir_columnas(
        solicitudes,
        ("solicitud_id", "laboratorio_id", "numero_participantes"),
        "T2",
    )

    resultado = (
        solicitudes.groupby("laboratorio_id", as_index=False, sort=True)
        .agg(
            cantidad_solicitudes=("solicitud_id", "count"),
            promedio_participantes=("numero_participantes", "mean"),
            maximo_participantes=("numero_participantes", "max"),
        )
    )
    return resultado


def t3_join_laboratorios(
    solicitudes: pd.DataFrame, laboratorios: pd.DataFrame
) -> pd.DataFrame:
    """T3: realiza un inner join N:1 mediante laboratorio_id.

    Conserva todas las columnas de solicitudes e incorpora código, nombre,
    capacidad, estado y vigencia del laboratorio con nombres no ambiguos.
    """
    _requerir_columnas(solicitudes, ("laboratorio_id",), "T3 solicitudes")
    columnas_laboratorio = [
        "laboratorio_id",
        "codigo",
        "nombre",
        "capacidad",
        "estado",
        "activo",
    ]
    _requerir_columnas(laboratorios, columnas_laboratorio, "T3 laboratorios")

    dimension = laboratorios.loc[:, columnas_laboratorio].rename(
        columns={
            "codigo": "codigo_laboratorio",
            "nombre": "nombre_laboratorio",
            "estado": "estado_laboratorio",
            "activo": "laboratorio_activo",
        }
    )
    return solicitudes.merge(
        dimension,
        how="inner",
        on="laboratorio_id",
        sort=False,
        validate="many_to_one",
    )


def t4_prioridad_demanda(solicitudes_con_laboratorio: pd.DataFrame) -> pd.DataFrame:
    """T4: añade puntaje_prioridad mediante una regla determinista.

    puntaje = participantes * duración_minutos
              + bono_ocupación + bono_anticipación
              + bono_fin_semana + bono_aprobada

    Bono de ocupación: 1000 si ocupación >= 90 %, 500 si >= 75 %, 0 si no.
    Bono de anticipación: 300 si la reserva se creó con 7 días o menos.
    Bono de fin de semana: 200 para sábado o domingo.
    Bono de estado: 100 si la solicitud está APROBADA.
    """
    requeridas = (
        "numero_participantes",
        "capacidad",
        "hora_inicio",
        "hora_fin",
        "fecha_reserva",
        "creada_en",
        "estado",
    )
    _requerir_columnas(solicitudes_con_laboratorio, requeridas, "T4")

    resultado = solicitudes_con_laboratorio.copy()
    inicio = pd.to_timedelta(resultado["hora_inicio"], errors="raise")
    fin = pd.to_timedelta(resultado["hora_fin"], errors="raise")
    duracion_minutos = ((fin - inicio).dt.total_seconds() // 60).astype("int64")

    fecha_reserva = pd.to_datetime(
        resultado["fecha_reserva"], errors="raise"
    ).dt.normalize()
    fecha_creacion = (
        pd.to_datetime(resultado["creada_en"], errors="raise", utc=True)
        .dt.tz_localize(None)
        .dt.normalize()
    )
    antelacion_dias = (fecha_reserva - fecha_creacion).dt.days

    participantes = resultado["numero_participantes"]
    capacidad = resultado["capacidad"]
    ocupacion_por_cien = participantes * 100
    bono_ocupacion = pd.Series(0, index=resultado.index, dtype="int64")
    bono_ocupacion = bono_ocupacion.mask(
        ocupacion_por_cien >= capacidad * 75, 500
    )
    bono_ocupacion = bono_ocupacion.mask(
        ocupacion_por_cien >= capacidad * 90, 1000
    )

    bono_antelacion = antelacion_dias.le(7).astype("int64") * 300
    bono_fin_semana = fecha_reserva.dt.dayofweek.ge(5).astype("int64") * 200
    bono_aprobada = resultado["estado"].eq("APROBADA").astype("int64") * 100

    resultado["puntaje_prioridad"] = (
        participantes * duracion_minutos
        + bono_ocupacion
        + bono_antelacion
        + bono_fin_semana
        + bono_aprobada
    ).astype("int64")
    return resultado


def t5_top_demanda(
    solicitudes: pd.DataFrame, n: int = TOP_N
) -> pd.DataFrame:
    """T5: devuelve las N solicitudes con mayor demanda y desempate estable.

    Orden: participantes DESC, duración DESC, fecha ASC, solicitud_id ASC.
    """
    requeridas = (
        "solicitud_id",
        "laboratorio_id",
        "fecha_reserva",
        "hora_inicio",
        "hora_fin",
        "numero_participantes",
        "estado",
    )
    _requerir_columnas(solicitudes, requeridas, "T5")
    if n <= 0:
        raise ValueError("T5 requiere un valor de n mayor que cero")

    resultado = solicitudes.loc[:, list(requeridas)].copy()
    inicio = pd.to_timedelta(resultado["hora_inicio"], errors="raise")
    fin = pd.to_timedelta(resultado["hora_fin"], errors="raise")
    resultado["duracion_minutos"] = (
        (fin - inicio).dt.total_seconds() // 60
    ).astype("int64")
    resultado["fecha_reserva"] = pd.to_datetime(
        resultado["fecha_reserva"], errors="raise"
    )

    resultado = resultado.sort_values(
        by=[
            "numero_participantes",
            "duracion_minutos",
            "fecha_reserva",
            "solicitud_id",
        ],
        ascending=[False, False, True, True],
        kind="mergesort",
    )
    return resultado.head(n).reset_index(drop=True)
