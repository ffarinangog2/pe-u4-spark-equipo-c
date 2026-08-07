"""Transformaciones PySpark equivalentes a las T1--T5 de pandas.

Este módulo no crea una SparkSession, no lee archivos y no desencadena acciones.
Las funciones reciben y devuelven DataFrames para permitir que la ejecución y
la medición sean controladas desde otros componentes.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


FECHA_INICIO_T1 = "2024-07-01"
FECHA_FIN_T1 = "2025-06-30"
MINIMO_PARTICIPANTES_T1 = 25
TOP_N = 20


def _requerir_columnas(
    dataframe: DataFrame, columnas: Iterable[str], transformacion: str
) -> None:
    """Genera un error claro si el DataFrame no contiene el esquema requerido."""
    faltantes = sorted(set(columnas).difference(dataframe.columns))
    if faltantes:
        raise ValueError(
            f"{transformacion} requiere columnas ausentes: {', '.join(faltantes)}"
        )


def _duracion_en_minutos(hora_inicio: str, hora_fin: str):
    """Construye la expresión Spark de minutos enteros entre dos horas."""
    formato = "yyyy-MM-dd HH:mm:ss"
    inicio = F.to_timestamp(
        F.concat(F.lit("1970-01-01 "), F.col(hora_inicio)), formato
    ).cast("long")
    fin = F.to_timestamp(
        F.concat(F.lit("1970-01-01 "), F.col(hora_fin)), formato
    ).cast("long")
    return F.floor((fin - inicio) / F.lit(60)).cast("long")


def t1_filtrado_compuesto(solicitudes: DataFrame) -> DataFrame:
    """T1: filtra por estado, participantes y rango inclusivo de fechas."""
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

    fecha_reserva = F.to_date(F.col("fecha_reserva"))
    condicion = (
        (F.col("estado") == F.lit("APROBADA"))
        & (F.col("numero_participantes") >= F.lit(MINIMO_PARTICIPANTES_T1))
        & (fecha_reserva >= F.to_date(F.lit(FECHA_INICIO_T1)))
        & (fecha_reserva <= F.to_date(F.lit(FECHA_FIN_T1)))
    )
    return solicitudes.filter(condicion).select(*columnas_salida)


def t2_agregaciones_por_laboratorio(solicitudes: DataFrame) -> DataFrame:
    """T2: agrupa por laboratorio y calcula conteo, promedio y máximo."""
    _requerir_columnas(
        solicitudes,
        ("solicitud_id", "laboratorio_id", "numero_participantes"),
        "T2",
    )

    return (
        solicitudes.groupBy("laboratorio_id")
        .agg(
            F.count("solicitud_id").alias("cantidad_solicitudes"),
            F.avg("numero_participantes").alias("promedio_participantes"),
            F.max("numero_participantes").alias("maximo_participantes"),
        )
        .orderBy(F.col("laboratorio_id").asc())
    )


def t3_join_laboratorios(
    solicitudes: DataFrame, laboratorios: DataFrame
) -> DataFrame:
    """T3: realiza el inner join N:1 mediante laboratorio_id."""
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

    dimension = laboratorios.select(
        F.col("laboratorio_id"),
        F.col("codigo").alias("codigo_laboratorio"),
        F.col("nombre").alias("nombre_laboratorio"),
        F.col("capacidad"),
        F.col("estado").alias("estado_laboratorio"),
        F.col("activo").alias("laboratorio_activo"),
    )
    return solicitudes.join(dimension, on="laboratorio_id", how="inner")


def t4_prioridad_demanda(solicitudes_con_laboratorio: DataFrame) -> DataFrame:
    """T4: añade puntaje_prioridad con la misma fórmula entera de pandas."""
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

    duracion_minutos = _duracion_en_minutos("hora_inicio", "hora_fin")
    # Los CSV usan ISO 8601. Extraer YYYY-MM-DD reproduce la normalización UTC
    # de pandas y evita que spark.sql.session.timeZone altere la anticipación.
    fecha_reserva = F.to_date(
        F.substring(F.col("fecha_reserva").cast("string"), 1, 10),
        "yyyy-MM-dd",
    )
    fecha_creacion = F.to_date(
        F.substring(F.col("creada_en").cast("string"), 1, 10),
        "yyyy-MM-dd",
    )
    antelacion_dias = F.datediff(fecha_reserva, fecha_creacion)

    ocupacion_por_cien = F.col("numero_participantes") * F.lit(100)
    bono_ocupacion = (
        F.when(ocupacion_por_cien >= F.col("capacidad") * F.lit(90), F.lit(1000))
        .when(ocupacion_por_cien >= F.col("capacidad") * F.lit(75), F.lit(500))
        .otherwise(F.lit(0))
    )
    bono_antelacion = F.when(antelacion_dias <= F.lit(7), F.lit(300)).otherwise(
        F.lit(0)
    )
    # dayofweek: domingo=1 y sábado=7 en Spark.
    bono_fin_semana = F.when(
        F.dayofweek(fecha_reserva).isin(1, 7), F.lit(200)
    ).otherwise(F.lit(0))
    bono_aprobada = F.when(F.col("estado") == F.lit("APROBADA"), F.lit(100)).otherwise(
        F.lit(0)
    )

    puntaje = (
        F.col("numero_participantes") * duracion_minutos
        + bono_ocupacion
        + bono_antelacion
        + bono_fin_semana
        + bono_aprobada
    ).cast("long")
    return solicitudes_con_laboratorio.withColumn("puntaje_prioridad", puntaje)


def t5_top_demanda(solicitudes: DataFrame, n: int = TOP_N) -> DataFrame:
    """T5: devuelve el Top-N con los cuatro criterios de orden de pandas."""
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

    resultado = (
        solicitudes.select(*requeridas)
        .withColumn(
            "duracion_minutos",
            _duracion_en_minutos("hora_inicio", "hora_fin"),
        )
        .withColumn("fecha_reserva", F.to_timestamp(F.col("fecha_reserva")))
    )
    return resultado.orderBy(
        F.col("numero_participantes").desc(),
        F.col("duracion_minutos").desc(),
        F.col("fecha_reserva").asc(),
        F.col("solicitud_id").asc(),
    ).limit(n)
