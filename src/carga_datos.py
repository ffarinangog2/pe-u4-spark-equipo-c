"""Carga explícita y determinista del dataset PE-U4.

Las columnas temporales se conservan como texto ISO 8601 durante la lectura.
Cada transformación realiza después la conversión temporal que necesita. Esto
evita que Spark asigne la fecha actual a valores que representan solo una hora.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
RUTA_SOLICITUDES = RAIZ_PROYECTO / "data" / "solicitudes_reserva.csv"
RUTA_LABORATORIOS = RAIZ_PROYECTO / "data" / "laboratorios.csv"


TIPOS_PANDAS_SOLICITUDES = {
    "solicitud_id": "string",
    "solicitante_id": "string",
    "docente_id": "string",
    "laboratorio_id": "string",
    "materia_id": "string",
    "periodo_lectivo_id": "string",
    "fecha_reserva": "string",
    "hora_inicio": "string",
    "hora_fin": "string",
    "numero_participantes": "int64",
    "motivo": "string",
    "estado": "string",
    "creada_en": "string",
    "actualizada_en": "string",
}

TIPOS_PANDAS_LABORATORIOS = {
    "laboratorio_id": "string",
    "piso_id": "string",
    "codigo": "string",
    "nombre": "string",
    "capacidad": "int64",
    "descripcion": "string",
    "estado": "string",
    "activo": "boolean",
    "creado_en": "string",
    "actualizado_en": "string",
}

ESQUEMA_SPARK_SOLICITUDES = StructType(
    [
        StructField("solicitud_id", StringType(), False),
        StructField("solicitante_id", StringType(), False),
        StructField("docente_id", StringType(), False),
        StructField("laboratorio_id", StringType(), False),
        StructField("materia_id", StringType(), False),
        StructField("periodo_lectivo_id", StringType(), False),
        StructField("fecha_reserva", StringType(), False),
        StructField("hora_inicio", StringType(), False),
        StructField("hora_fin", StringType(), False),
        StructField("numero_participantes", IntegerType(), False),
        StructField("motivo", StringType(), False),
        StructField("estado", StringType(), False),
        StructField("creada_en", StringType(), False),
        StructField("actualizada_en", StringType(), False),
    ]
)

ESQUEMA_SPARK_LABORATORIOS = StructType(
    [
        StructField("laboratorio_id", StringType(), False),
        StructField("piso_id", StringType(), False),
        StructField("codigo", StringType(), False),
        StructField("nombre", StringType(), False),
        StructField("capacidad", IntegerType(), False),
        StructField("descripcion", StringType(), False),
        StructField("estado", StringType(), False),
        StructField("activo", BooleanType(), False),
        StructField("creado_en", StringType(), False),
        StructField("actualizado_en", StringType(), False),
    ]
)


def cargar_solicitudes_pandas(
    ruta: str | Path = RUTA_SOLICITUDES,
) -> pd.DataFrame:
    """Carga solicitudes con tipos físicos explícitos y sin inferencia temporal."""
    return pd.read_csv(ruta, dtype=TIPOS_PANDAS_SOLICITUDES)


def cargar_laboratorios_pandas(
    ruta: str | Path = RUTA_LABORATORIOS,
) -> pd.DataFrame:
    """Carga laboratorios con tipos físicos explícitos."""
    return pd.read_csv(ruta, dtype=TIPOS_PANDAS_LABORATORIOS)


def cargar_solicitudes_spark(
    spark: SparkSession, ruta: str | Path = RUTA_SOLICITUDES
) -> DataFrame:
    """Carga solicitudes con esquema Spark explícito, sin inferSchema."""
    return (
        spark.read.option("header", True)
        .schema(ESQUEMA_SPARK_SOLICITUDES)
        .csv(str(ruta))
    )


def cargar_laboratorios_spark(
    spark: SparkSession, ruta: str | Path = RUTA_LABORATORIOS
) -> DataFrame:
    """Carga laboratorios con esquema Spark explícito, sin inferSchema."""
    return (
        spark.read.option("header", True)
        .schema(ESQUEMA_SPARK_LABORATORIOS)
        .csv(str(ruta))
    )
