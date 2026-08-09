"""Protocolo experimental para comparar pandas y PySpark en PE-U4.

PySpark se conecta a un clúster Spark Standalone previamente iniciado. Cada
configuración ``executors[N]`` usa N procesos executor JVM de un core; nunca se
usa ``local[N]`` para representar executors.

Importar este módulo no ejecuta benchmarks. El protocolo completo solo se
inicia mediante :func:`ejecutar_protocolo` o con ``--ejecutar`` desde la CLI.
"""

from __future__ import annotations

import argparse
import csv
import gc
import os
import statistics
import time
import webbrowser
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

try:
    from . import carga_datos
    from . import transformaciones_pandas as tp
    from . import transformaciones_spark as ts
except ImportError:
    import carga_datos
    import transformaciones_pandas as tp
    import transformaciones_spark as ts


REPETICIONES_OFICIALES = 5
EXECUTORS_T3 = (1, 2, 4)
EXECUTORS_BASE = 4
SPARK_MASTER = os.environ.get("PE_U4_SPARK_MASTER", "spark://127.0.0.1:7077")

RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
RUTA_TIEMPOS_CRUDOS = RAIZ_PROYECTO / "resultados" / "tiempos_crudos.csv"
RUTA_TIEMPOS_RESUMEN = RAIZ_PROYECTO / "resultados" / "tiempos_resumen.csv"
RUTA_METRICAS = RAIZ_PROYECTO / "resultados" / "metricas_derivadas.csv"

COLUMNAS_TIEMPOS_CRUDOS = (
    "motor",
    "transformacion",
    "configuracion",
    "repeticion",
    "tiempo_segundos",
)
COLUMNAS_TIEMPOS_RESUMEN = (
    "motor",
    "transformacion",
    "configuracion",
    "repeticiones",
    "mediana_segundos",
    "speedup",
)
COLUMNAS_METRICAS = (
    "tipo_metrica",
    "transformacion",
    "configuracion_n",
    "valor",
    "unidad_interpretacion",
)


@dataclass(frozen=True)
class MedicionCruda:
    motor: str
    transformacion: str
    configuracion: str
    repeticion: int
    tiempo_segundos: float


@dataclass(frozen=True)
class MedicionResumen:
    motor: str
    transformacion: str
    configuracion: str
    repeticiones: int
    mediana_segundos: float
    speedup: float | str = ""


@dataclass(frozen=True)
class CalentamientoDescartado:
    motor: str
    transformacion: str
    configuracion: str
    tiempo_segundos: float
    estado: str = "DESCARTADO"


@dataclass(frozen=True)
class TiempoPreparacion:
    """Tiempo separado de carga o materialización, no mezclado con T1--T5."""

    motor: str
    configuracion: str
    etapa: str
    tiempo_segundos: float


@dataclass(frozen=True)
class ResultadoProtocolo:
    mediciones: tuple[MedicionCruda, ...]
    resumenes: tuple[MedicionResumen, ...]
    preparaciones: tuple[TiempoPreparacion, ...]
    calentamientos: tuple[CalentamientoDescartado, ...]


def _validar_repeticiones(repeticiones: int) -> None:
    if repeticiones <= 0:
        raise ValueError("El número de repeticiones debe ser mayor que cero")


def _materializar_pandas(resultado: pd.DataFrame) -> None:
    """Acceso mínimo; pandas ya evaluó la transformación de forma eager."""
    _ = resultado.shape


def _materializar_spark(resultado: DataFrame) -> None:
    """Fuerza todo el plan y todas sus columnas sin imprimir ni guardar datos.

    El origen ``noop`` de Spark es un sumidero de descarte: desencadena una
    acción real, pero no crea archivos de salida.
    """
    resultado.write.format("noop").mode("overwrite").save()


def _abrir_spark_ui_para_evidencia(spark: SparkSession) -> None:
    """Abre opcionalmente páginas de Spark UI para capturas manuales.

    No crea ni modifica imágenes. ``webbrowser`` selecciona el navegador del
    sistema, por lo que esta ayuda no depende de Edge ni de rutas de Windows.
    Los nombres publicados coinciden con las capturas conservadas por el
    proyecto.
    """
    base_ui = spark.sparkContext.uiWebUrl
    if not base_ui:
        raise RuntimeError("Spark UI no está disponible para esta aplicación")
    destinos = {
        "spark_ui_executors.png": f"{base_ui}/executors/",
        "spark_ui_jobs.png": f"{base_ui}/jobs/",
        "spark_ui_stages.png": f"{base_ui}/stages/",
        "spark_ui_t3.png": f"{base_ui}/jobs/",
    }
    for nombre, url in destinos.items():
        print(f"CAPTURA_MANUAL_OPCIONAL,{nombre},{url}")
        webbrowser.open_new_tab(url)


def medir_repeticiones(
    *,
    motor: str,
    transformacion: str,
    configuracion: str,
    operacion: Callable[[], Any],
    materializar: Callable[[Any], None],
    registro_calentamientos: list[CalentamientoDescartado],
    repeticiones: int = REPETICIONES_OFICIALES,
) -> list[MedicionCruda]:
    """Mide una operación cinco veces por defecto con reloj monotónico."""
    _validar_repeticiones(repeticiones)
    mediciones: list[MedicionCruda] = []

    # Calentamiento obligatorio: se materializa una ejecución completa antes de
    # iniciar el reloj de las cinco repeticiones oficiales y no se registra.
    # Así se evita que la inicialización diferida del motor contamine la serie.
    inicio_calentamiento = time.perf_counter()
    resultado_calentamiento = operacion()
    materializar(resultado_calentamiento)
    tiempo_calentamiento = time.perf_counter() - inicio_calentamiento
    registro_calentamientos.append(
        CalentamientoDescartado(
            motor=motor,
            transformacion=transformacion,
            configuracion=configuracion,
            tiempo_segundos=tiempo_calentamiento,
        )
    )
    del resultado_calentamiento
    gc.collect()

    for numero in range(1, repeticiones + 1):
        gc.collect()
        inicio = time.perf_counter()
        resultado = operacion()
        materializar(resultado)
        tiempo_segundos = time.perf_counter() - inicio
        mediciones.append(
            MedicionCruda(
                motor=motor,
                transformacion=transformacion,
                configuracion=configuracion,
                repeticion=numero,
                tiempo_segundos=tiempo_segundos,
            )
        )
        del resultado

    return mediciones


def resumir_mediciones(
    mediciones: Iterable[MedicionCruda],
) -> list[MedicionResumen]:
    """Agrupa series homogéneas y calcula la mediana de sus repeticiones."""
    grupos: dict[tuple[str, str, str], list[float]] = {}
    for medicion in mediciones:
        clave = (
            medicion.motor,
            medicion.transformacion,
            medicion.configuracion,
        )
        grupos.setdefault(clave, []).append(medicion.tiempo_segundos)

    return [
        MedicionResumen(
            motor=motor,
            transformacion=transformacion,
            configuracion=configuracion,
            repeticiones=len(tiempos),
            mediana_segundos=statistics.median(tiempos),
        )
        for (motor, transformacion, configuracion), tiempos in sorted(grupos.items())
    ]


def calcular_speedup(tiempo_base: float, tiempo_comparado: float) -> float:
    """Calcula S = T_base / T_comparado con tiempos reales positivos."""
    if tiempo_base <= 0 or tiempo_comparado <= 0:
        raise ValueError("Los tiempos para speedup deben ser positivos")
    return tiempo_base / tiempo_comparado


def calcular_speedup_pandas_pyspark(
    mediana_pandas: float, mediana_pyspark: float
) -> float:
    """Speedup de PySpark respecto de pandas: T_pandas / T_pyspark."""
    return calcular_speedup(mediana_pandas, mediana_pyspark)


def calcular_speedup_t3(
    mediana_executor_1: float, mediana_executor_n: float
) -> float:
    """Speedup real de T3: T_executors[1] / T_executors[n]."""
    return calcular_speedup(mediana_executor_1, mediana_executor_n)


def calcular_eficiencia_paralela(speedup_observado: float, unidades: int) -> float:
    """Calcula E = S(N) / N para N procesos executor."""
    if speedup_observado <= 0:
        raise ValueError("El speedup debe ser positivo")
    if unidades <= 0:
        raise ValueError("Las unidades de ejecución deben ser positivas")
    return speedup_observado / unidades


def calcular_fraccion_serial_amdahl(
    speedup_observado: float, unidades: int
) -> float:
    """Despeja la fracción serial f de S_p = 1 / (f + (1-f)/p)."""
    if speedup_observado <= 0:
        raise ValueError("El speedup debe ser positivo")
    if unidades <= 1:
        raise ValueError("Amdahl requiere más de una unidad de ejecución")
    return (unidades / speedup_observado - 1.0) / (unidades - 1.0)


def calcular_speedup_gustafson(fraccion_serial: float, unidades: int) -> float:
    """Calcula el speedup escalado de Gustafson: S_G = p - f(p - 1)."""
    if not 0 <= fraccion_serial <= 1:
        raise ValueError("La fracción serial debe estar entre cero y uno")
    if unidades <= 0:
        raise ValueError("Las unidades de ejecución deben ser positivas")
    return unidades - fraccion_serial * (unidades - 1)


def _medir_carga_pandas() -> tuple[pd.DataFrame, pd.DataFrame, TiempoPreparacion]:
    inicio = time.perf_counter()
    solicitudes = carga_datos.cargar_solicitudes_pandas()
    laboratorios = carga_datos.cargar_laboratorios_pandas()
    tiempo = time.perf_counter() - inicio
    return (
        solicitudes,
        laboratorios,
        TiempoPreparacion("pandas", "pandas", "carga_csv", tiempo),
    )


def medir_transformaciones_pandas(
    repeticiones: int = REPETICIONES_OFICIALES,
) -> tuple[list[MedicionCruda], list[TiempoPreparacion], list[CalentamientoDescartado]]:
    """Mide T1--T5 en pandas sin incluir carga ni preparación de T4."""
    solicitudes, laboratorios, carga = _medir_carga_pandas()
    mediciones: list[MedicionCruda] = []
    preparaciones = [carga]
    calentamientos: list[CalentamientoDescartado] = []

    operaciones: tuple[tuple[str, Callable[[], pd.DataFrame]], ...] = (
        ("T1", lambda: tp.t1_filtrado_compuesto(solicitudes)),
        ("T2", lambda: tp.t2_agregaciones_por_laboratorio(solicitudes)),
        ("T3", lambda: tp.t3_join_laboratorios(solicitudes, laboratorios)),
    )
    for nombre, operacion in operaciones:
        mediciones.extend(
            medir_repeticiones(
                motor="pandas",
                transformacion=nombre,
                configuracion="pandas",
                operacion=operacion,
                materializar=_materializar_pandas,
                registro_calentamientos=calentamientos,
                repeticiones=repeticiones,
            )
        )

    inicio_preparacion = time.perf_counter()
    solicitudes_con_laboratorio = tp.t3_join_laboratorios(
        solicitudes, laboratorios
    )
    preparaciones.append(
        TiempoPreparacion(
            "pandas",
            "pandas",
            "entrada_T4_join_materializado",
            time.perf_counter() - inicio_preparacion,
        )
    )
    mediciones.extend(
        medir_repeticiones(
            motor="pandas",
            transformacion="T4",
            configuracion="pandas",
            operacion=lambda: tp.t4_prioridad_demanda(
                solicitudes_con_laboratorio
            ),
            materializar=_materializar_pandas,
            registro_calentamientos=calentamientos,
            repeticiones=repeticiones,
        )
    )
    del solicitudes_con_laboratorio
    gc.collect()

    mediciones.extend(
        medir_repeticiones(
            motor="pandas",
            transformacion="T5",
            configuracion="pandas",
            operacion=lambda: tp.t5_top_demanda(solicitudes),
            materializar=_materializar_pandas,
            registro_calentamientos=calentamientos,
            repeticiones=repeticiones,
        )
    )
    del solicitudes, laboratorios
    gc.collect()
    return mediciones, preparaciones, calentamientos


def crear_spark_session(executors: int) -> SparkSession:
    """Conecta con Spark Standalone y exige N procesos executor registrados."""
    if executors <= 0:
        raise ValueError("La cantidad de executors debe ser positiva")
    spark = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(f"PE-U4-benchmark-executors-{executors}")
        .config("spark.executor.instances", str(executors))
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", str(executors))
        .config("spark.executor.memory", "512m")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(executors))
        .config("spark.default.parallelism", str(executors))
        .getOrCreate()
    )
    limite = time.monotonic() + 60
    while time.monotonic() < limite:
        # StatusTracker incluye al driver; solo los demás son procesos executor.
        registrados = max(
            0,
            len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1,
        )
        if registrados == executors:
            break
        time.sleep(1)
    else:
        spark.stop()
        raise RuntimeError(
            f"Spark Standalone no registró {executors} executors dentro de 60 segundos"
        )
    conf = dict(spark.sparkContext.getConf().getAll())
    if conf.get("spark.master", "").startswith("local"):
        spark.stop()
        raise RuntimeError("Configuración inválida: master local[N] no es un clúster real")
    print(f"EXECUTORS_VERIFICADOS={registrados}; CONFIGURACION=executors[{executors}]")
    for clave in (
        "spark.master", "spark.executor.instances", "spark.executor.cores",
        "spark.cores.max", "spark.executor.memory", "spark.app.id",
    ):
        print(f"{clave}={conf.get(clave)}")
    return spark


def _cargar_y_materializar_spark(
    spark: SparkSession, configuracion: str
) -> tuple[DataFrame, DataFrame, TiempoPreparacion]:
    inicio = time.perf_counter()
    solicitudes = carga_datos.cargar_solicitudes_spark(spark).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    laboratorios = carga_datos.cargar_laboratorios_spark(spark).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    solicitudes.count()
    laboratorios.count()
    tiempo = time.perf_counter() - inicio
    return (
        solicitudes,
        laboratorios,
        TiempoPreparacion("pyspark", configuracion, "carga_csv_y_cache", tiempo),
    )


def medir_transformaciones_spark_standalone(
    executors: int,
    *,
    solo_t3: bool,
    solo_t1: bool = False,
    abrir_spark_ui: bool = False,
    repeticiones: int = REPETICIONES_OFICIALES,
) -> tuple[list[MedicionCruda], list[TiempoPreparacion], list[CalentamientoDescartado]]:
    """Mide Spark Standalone y garantiza siempre el cierre de la aplicación."""
    if solo_t1 and solo_t3:
        raise ValueError("solo_t1 y solo_t3 no pueden activarse simultaneamente")
    spark: SparkSession | None = None
    solicitudes: DataFrame | None = None
    laboratorios: DataFrame | None = None
    entrada_t4: DataFrame | None = None
    configuracion = f"executors[{executors}]"
    mediciones: list[MedicionCruda] = []
    preparaciones: list[TiempoPreparacion] = []
    calentamientos: list[CalentamientoDescartado] = []

    try:
        spark = crear_spark_session(executors)
        spark.sparkContext.setLogLevel("WARN")
        solicitudes, laboratorios, preparacion = _cargar_y_materializar_spark(
            spark, configuracion
        )
        preparaciones.append(preparacion)

        if not solo_t3:
            operaciones: tuple[tuple[str, Callable[[], DataFrame]], ...] = (
                ("T1", lambda: ts.t1_filtrado_compuesto(solicitudes)),
            )
            if not solo_t1:
                operaciones += (
                    ("T2", lambda: ts.t2_agregaciones_por_laboratorio(solicitudes)),
                )
            for nombre, operacion in operaciones:
                mediciones.extend(
                    medir_repeticiones(
                        motor="pyspark",
                        transformacion=nombre,
                        configuracion=configuracion,
                        operacion=operacion,
                        materializar=_materializar_spark,
                        registro_calentamientos=calentamientos,
                        repeticiones=repeticiones,
                    )
                )

        if not solo_t1:
            mediciones.extend(
                medir_repeticiones(
                    motor="pyspark",
                    transformacion="T3",
                    configuracion=configuracion,
                    operacion=lambda: ts.t3_join_laboratorios(
                        solicitudes, laboratorios
                    ),
                    materializar=_materializar_spark,
                    registro_calentamientos=calentamientos,
                    repeticiones=repeticiones,
                )
            )

        if abrir_spark_ui and executors == EXECUTORS_BASE and not solo_t3 and not solo_t1:
            _abrir_spark_ui_para_evidencia(spark)

        if not solo_t3 and not solo_t1:
            inicio_preparacion = time.perf_counter()
            entrada_t4 = ts.t3_join_laboratorios(
                solicitudes, laboratorios
            ).persist(StorageLevel.MEMORY_AND_DISK)
            entrada_t4.count()
            preparaciones.append(
                TiempoPreparacion(
                    "pyspark",
                    configuracion,
                    "entrada_T4_join_materializado",
                    time.perf_counter() - inicio_preparacion,
                )
            )
            mediciones.extend(
                medir_repeticiones(
                    motor="pyspark",
                    transformacion="T4",
                    configuracion=configuracion,
                    operacion=lambda: ts.t4_prioridad_demanda(entrada_t4),
                    materializar=_materializar_spark,
                    registro_calentamientos=calentamientos,
                    repeticiones=repeticiones,
                )
            )
            entrada_t4.unpersist(blocking=True)
            entrada_t4 = None

            mediciones.extend(
                medir_repeticiones(
                    motor="pyspark",
                    transformacion="T5",
                    configuracion=configuracion,
                    operacion=lambda: ts.t5_top_demanda(solicitudes),
                    materializar=_materializar_spark,
                    registro_calentamientos=calentamientos,
                    repeticiones=repeticiones,
                )
            )
        return mediciones, preparaciones, calentamientos
    finally:
        if entrada_t4 is not None:
            entrada_t4.unpersist(blocking=False)
        if solicitudes is not None:
            solicitudes.unpersist(blocking=False)
        if laboratorios is not None:
            laboratorios.unpersist(blocking=False)
        if spark is not None:
            spark.catalog.clearCache()
            spark.stop()
        gc.collect()


def _escribir_csv_atomico(
    ruta: Path, columnas: Sequence[str], filas: Iterable[dict[str, Any]]
) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    temporal = ruta.with_suffix(ruta.suffix + ".tmp")
    try:
        with temporal.open("w", encoding="utf-8", newline="") as archivo:
            escritor = csv.DictWriter(archivo, fieldnames=columnas)
            escritor.writeheader()
            for fila in filas:
                serializada = dict(fila)
                for clave, valor in serializada.items():
                    if isinstance(valor, float):
                        serializada[clave] = format(valor, ".9f")
                escritor.writerow(serializada)
        os.replace(temporal, ruta)
    finally:
        if temporal.exists():
            temporal.unlink()


def guardar_resultados(
    mediciones: Sequence[MedicionCruda],
    resumenes: Sequence[MedicionResumen],
    metricas: Sequence[dict[str, Any]],
) -> None:
    """Guarda atómicamente mediciones, resumen y métricas ya calculadas."""
    _escribir_csv_atomico(
        RUTA_TIEMPOS_CRUDOS,
        COLUMNAS_TIEMPOS_CRUDOS,
        (asdict(medicion) for medicion in mediciones),
    )
    _escribir_csv_atomico(
        RUTA_TIEMPOS_RESUMEN,
        COLUMNAS_TIEMPOS_RESUMEN,
        (asdict(resumen) for resumen in resumenes),
    )
    _escribir_csv_atomico(RUTA_METRICAS, COLUMNAS_METRICAS, metricas)


def completar_resumen_y_metricas(
    resumenes: Sequence[MedicionResumen],
) -> tuple[list[MedicionResumen], list[dict[str, Any]]]:
    """Añade speedups y deriva todas las métricas solo desde las medianas reales."""
    medianas = {
        (r.motor, r.transformacion, r.configuracion): r.mediana_segundos
        for r in resumenes
    }
    t3_base = medianas[("pyspark", "T3", "executors[1]")]
    completados: list[MedicionResumen] = []
    for resumen in resumenes:
        speedup: float | str = ""
        if resumen.motor == "pyspark":
            speedup = calcular_speedup_pandas_pyspark(
                medianas[("pandas", resumen.transformacion, "pandas")],
                resumen.mediana_segundos,
            )
        completados.append(
            MedicionResumen(
                motor=resumen.motor,
                transformacion=resumen.transformacion,
                configuracion=resumen.configuracion,
                repeticiones=resumen.repeticiones,
                mediana_segundos=resumen.mediana_segundos,
                speedup=speedup,
            )
        )

    metricas: list[dict[str, Any]] = []
    for transformacion in ("T1", "T2", "T3", "T4", "T5"):
        valor = calcular_speedup_pandas_pyspark(
            medianas[("pandas", transformacion, "pandas")],
            medianas[("pyspark", transformacion, f"executors[{EXECUTORS_BASE}]")],
        )
        metricas.append({
            "tipo_metrica": "speedup_pandas_pyspark",
            "transformacion": transformacion,
            "configuracion_n": f"executors[{EXECUTORS_BASE}]",
            "valor": valor,
            "unidad_interpretacion": "razon; >1 PySpark mas rapido, <1 pandas mas rapido",
        })

    speedups_t3: dict[int, float] = {}
    for executors in EXECUTORS_T3:
        speedup = calcular_speedup_t3(
            t3_base, medianas[("pyspark", "T3", f"executors[{executors}]")]
        )
        speedups_t3[executors] = speedup
        eficiencia = calcular_eficiencia_paralela(speedup, executors)
        metricas.extend((
            {"tipo_metrica": "speedup_experimental_t3", "transformacion": "T3", "configuracion_n": f"executors[{executors}]", "valor": speedup, "unidad_interpretacion": "razon respecto de executors[1]"},
            {"tipo_metrica": "eficiencia_paralela_t3", "transformacion": "T3", "configuracion_n": f"executors[{executors}]", "valor": eficiencia, "unidad_interpretacion": "proporcion S(N)/N"},
            {"tipo_metrica": "eficiencia_porcentaje_t3", "transformacion": "T3", "configuracion_n": f"executors[{executors}]", "valor": eficiencia * 100, "unidad_interpretacion": "porcentaje"},
        ))

    fraccion_serial = calcular_fraccion_serial_amdahl(speedups_t3[4], 4)
    if not 0 <= fraccion_serial <= 1:
        raise RuntimeError(
            f"Fracción serial observada fuera de [0,1]: {fraccion_serial}; no se fabricarán métricas"
        )
    metricas.append({"tipo_metrica": "fraccion_serial_observada", "transformacion": "T3", "configuracion_n": "N=4_vs_N=1", "valor": fraccion_serial, "unidad_interpretacion": "proporcion no escalable observada; incluye overhead de Spark"})
    for executors in EXECUTORS_T3:
        amdahl = 1 / (fraccion_serial + (1 - fraccion_serial) / executors)
        gustafson = calcular_speedup_gustafson(fraccion_serial, executors)
        metricas.extend((
            {"tipo_metrica": "speedup_teorico_amdahl", "transformacion": "T3", "configuracion_n": f"N={executors}", "valor": amdahl, "unidad_interpretacion": "razon teorica"},
            {"tipo_metrica": "speedup_gustafson", "transformacion": "T3", "configuracion_n": f"N={executors}", "valor": gustafson, "unidad_interpretacion": "razon escalada"},
        ))
    metricas.append({"tipo_metrica": "limite_maximo_amdahl", "transformacion": "T3", "configuracion_n": "N=infinito", "valor": 1 / fraccion_serial, "unidad_interpretacion": "razon teorica maxima"})
    return completados, metricas


def ejecutar_protocolo(
    repeticiones: int = REPETICIONES_OFICIALES,
    *,
    guardar: bool = True,
    abrir_spark_ui: bool = False,
) -> ResultadoProtocolo:
    """Ejecuta pandas, Spark con 4 executors y T3 con 1, 2 y 4 executors."""
    _validar_repeticiones(repeticiones)
    mediciones: list[MedicionCruda] = []
    preparaciones: list[TiempoPreparacion] = []
    calentamientos: list[CalentamientoDescartado] = []

    nuevas, preparacion, nuevos_calentamientos = medir_transformaciones_pandas(repeticiones)
    mediciones.extend(nuevas)
    preparaciones.extend(preparacion)
    calentamientos.extend(nuevos_calentamientos)

    nuevas, preparacion, nuevos_calentamientos = medir_transformaciones_spark_standalone(
        EXECUTORS_BASE,
        solo_t3=False,
        abrir_spark_ui=abrir_spark_ui,
        repeticiones=repeticiones,
    )
    mediciones.extend(nuevas)
    preparaciones.extend(preparacion)
    calentamientos.extend(nuevos_calentamientos)

    for cantidad in (1, 2):
        nuevas, preparacion, nuevos_calentamientos = medir_transformaciones_spark_standalone(
            cantidad, solo_t3=True, repeticiones=repeticiones
        )
        mediciones.extend(nuevas)
        preparaciones.extend(preparacion)
        calentamientos.extend(nuevos_calentamientos)

    resumenes, metricas = completar_resumen_y_metricas(resumir_mediciones(mediciones))
    if guardar:
        guardar_resultados(mediciones, resumenes, metricas)
    return ResultadoProtocolo(
        mediciones=tuple(mediciones),
        resumenes=tuple(resumenes),
        preparaciones=tuple(preparaciones),
        calentamientos=tuple(calentamientos),
    )


def analizar_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ejecutar",
        action="store_true",
        help="confirma explícitamente la ejecución del protocolo completo",
    )
    parser.add_argument(
        "--repeticiones",
        type=int,
        default=REPETICIONES_OFICIALES,
        help="repeticiones por serie (protocolo oficial: 5)",
    )
    parser.add_argument(
        "--sin-guardar",
        action="store_true",
        help="ejecuta sin escribir los CSV de resultados",
    )
    parser.add_argument(
        "--abrir-spark-ui",
        action="store_true",
        help="abre páginas de Spark UI para capturas manuales; no crea imágenes",
    )
    return parser.parse_args()


def main() -> None:
    argumentos = analizar_argumentos()
    if not argumentos.ejecutar:
        raise SystemExit(
            "No se ejecutó el protocolo. Use --ejecutar para confirmarlo."
        )
    resultado = ejecutar_protocolo(
        repeticiones=argumentos.repeticiones,
        guardar=not argumentos.sin_guardar,
        abrir_spark_ui=argumentos.abrir_spark_ui,
    )
    for calentamiento in resultado.calentamientos:
        print(
            "CALENTAMIENTO_DESCARTADO,"
            f"{calentamiento.motor},{calentamiento.transformacion},"
            f"{calentamiento.configuracion},{calentamiento.tiempo_segundos:.9f}"
        )
    for medicion in resultado.mediciones:
        print(
            "REPETICION_OFICIAL,"
            f"{medicion.motor},{medicion.transformacion},{medicion.configuracion},"
            f"{medicion.repeticion},{medicion.tiempo_segundos:.9f}"
        )


if __name__ == "__main__":
    main()
