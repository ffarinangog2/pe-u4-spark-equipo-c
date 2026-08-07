"""Genera reproduciblemente las salidas CSV de T1--T5.

Este comando no forma parte del intervalo de medición. Ejecuta cada
transformación desde los datos originales y reemplaza únicamente su directorio
o archivo de salida correspondiente.
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

try:
    from . import carga_datos
    from . import transformaciones_pandas as tp
    from . import transformaciones_spark as ts
except ImportError:
    import carga_datos
    import transformaciones_pandas as tp
    import transformaciones_spark as ts


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
DIRECTORIO_PANDAS = RAIZ_PROYECTO / "data" / "pandas"
DIRECTORIO_SPARK = RAIZ_PROYECTO / "data" / "spark"

NOMBRES_SALIDA = {
    "T1": "T1_filtrado",
    "T2": "T2_agrupacion",
    "T3": "T3_join",
    "T4": "T4_columna_derivada",
    "T5": "T5_top20",
}


def _escribir_salida_spark_portable(resultado, destino: Path) -> None:
    """Persiste un DataFrame Spark sin depender de winutils.exe en Windows.

    La transformación y su materialización siguen ocurriendo en PySpark. Las
    filas se reciben por partición mediante ``toLocalIterator`` y se escriben
    en flujo para no reunir el DataFrame completo en memoria.
    """
    destino.mkdir(parents=True)
    parte = destino / "part-00000.csv"
    with parte.open("w", encoding="utf-8", newline="") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(resultado.columns)
        for fila in resultado.toLocalIterator(prefetchPartitions=True):
            escritor.writerow(fila)
    (destino / "_SUCCESS").touch()


def generar_salidas_pandas() -> None:
    """Calcula T1--T5 independientemente y guarda un CSV por transformación."""
    solicitudes = carga_datos.cargar_solicitudes_pandas()
    laboratorios = carga_datos.cargar_laboratorios_pandas()
    operaciones = {
        "T1": lambda: tp.t1_filtrado_compuesto(solicitudes),
        "T2": lambda: tp.t2_agregaciones_por_laboratorio(solicitudes),
        "T3": lambda: tp.t3_join_laboratorios(solicitudes, laboratorios),
        "T4": lambda: tp.t4_prioridad_demanda(
            tp.t3_join_laboratorios(solicitudes, laboratorios)
        ),
        "T5": lambda: tp.t5_top_demanda(solicitudes),
    }
    DIRECTORIO_PANDAS.mkdir(parents=True, exist_ok=True)
    for transformacion, operacion in operaciones.items():
        destino = DIRECTORIO_PANDAS / f"{NOMBRES_SALIDA[transformacion]}.csv"
        temporal = destino.with_suffix(".csv.tmp")
        operacion().to_csv(temporal, index=False)
        temporal.replace(destino)


def generar_salidas_spark(master: str) -> None:
    """Calcula T1--T5 independientemente y guarda directorios CSV de Spark."""
    spark = SparkSession.builder.master(master).appName("PE-U4-salidas-T1-T5").getOrCreate()
    try:
        solicitudes = carga_datos.cargar_solicitudes_spark(spark)
        laboratorios = carga_datos.cargar_laboratorios_spark(spark)
        operaciones = {
            "T1": lambda: ts.t1_filtrado_compuesto(solicitudes),
            "T2": lambda: ts.t2_agregaciones_por_laboratorio(solicitudes),
            "T3": lambda: ts.t3_join_laboratorios(solicitudes, laboratorios),
            "T4": lambda: ts.t4_prioridad_demanda(
                ts.t3_join_laboratorios(solicitudes, laboratorios)
            ),
            "T5": lambda: ts.t5_top_demanda(solicitudes),
        }
        DIRECTORIO_SPARK.mkdir(parents=True, exist_ok=True)
        for transformacion, operacion in operaciones.items():
            destino = DIRECTORIO_SPARK / NOMBRES_SALIDA[transformacion]
            temporal = destino.with_name(f"{destino.name}.tmp")
            if temporal.exists():
                shutil.rmtree(temporal)
            resultado = operacion()
            if os.name == "nt" and not (
                os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
            ):
                _escribir_salida_spark_portable(resultado, temporal)
            else:
                resultado.write.mode("overwrite").option("header", True).csv(
                    str(temporal)
                )
            if destino.exists():
                shutil.rmtree(destino)
            temporal.replace(destino)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--motor", choices=("pandas", "spark", "ambos"), default="ambos")
    parser.add_argument("--master", default="spark://127.0.0.1:7077")
    argumentos = parser.parse_args()
    if argumentos.motor in {"pandas", "ambos"}:
        generar_salidas_pandas()
    if argumentos.motor in {"spark", "ambos"}:
        generar_salidas_spark(argumentos.master)


if __name__ == "__main__":
    main()
