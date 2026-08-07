"""Generador reproducible del dataset sintético de Laboratorios Informáticos.

Este módulo usa exclusivamente la biblioteca estándar. La generación se realiza
en flujo para no conservar las 500 000 solicitudes en memoria.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import random
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterator


SEMILLA_FIJA = 20260804
TOTAL_SOLICITUDES = 500_000
TOTAL_LABORATORIOS = 40

RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
DIRECTORIO_DATOS = RAIZ_PROYECTO / "data"
RUTA_SOLICITUDES = DIRECTORIO_DATOS / "solicitudes_reserva.csv"
RUTA_LABORATORIOS = DIRECTORIO_DATOS / "laboratorios.csv"

ESPACIO_NOMBRES = uuid.uuid5(uuid.NAMESPACE_DNS, f"pe-u4-equipo-c-{SEMILLA_FIJA}")

COLUMNAS_SOLICITUDES = [
    "solicitud_id",
    "solicitante_id",
    "docente_id",
    "laboratorio_id",
    "materia_id",
    "periodo_lectivo_id",
    "fecha_reserva",
    "hora_inicio",
    "hora_fin",
    "numero_participantes",
    "motivo",
    "estado",
    "creada_en",
    "actualizada_en",
]

COLUMNAS_LABORATORIOS = [
    "laboratorio_id",
    "piso_id",
    "codigo",
    "nombre",
    "capacidad",
    "descripcion",
    "estado",
    "activo",
    "creado_en",
    "actualizado_en",
]

ESTADOS_SOLICITUD = (
    "PENDIENTE",
    "EN_REVISION",
    "APROBADA",
    "RECHAZADA",
    "CANCELADA",
    "EXPIRADA",
)
PESOS_ESTADOS_SOLICITUD = (18, 12, 45, 10, 10, 5)
ESTADOS_LABORATORIO = (
    "DISPONIBLE",
    "OCUPADO",
    "MANTENIMIENTO",
    "INACTIVO",
)
PESOS_ESTADOS_LABORATORIO = (65, 20, 10, 5)

MOTIVOS = (
    "Clase práctica de programación",
    "Práctica de redes y conectividad",
    "Evaluación en plataforma virtual",
    "Taller de bases de datos",
    "Capacitación en herramientas informáticas",
    "Proyecto académico de desarrollo de software",
    "Práctica de arquitectura de computadores",
    "Actividad de investigación aplicada",
)


def uuid_determinista(entidad: str, indice: int) -> str:
    """Obtiene un UUID estable para una entidad y un índice únicos."""
    return str(uuid.uuid5(ESPACIO_NOMBRES, f"{entidad}:{indice}"))


def formato_timestamp(valor: datetime) -> str:
    """Serializa un timestamp UTC en ISO 8601 con precisión de segundos."""
    return valor.astimezone(timezone.utc).isoformat(timespec="seconds")


def construir_laboratorios() -> list[dict[str, object]]:
    """Construye el catálogo pequeño y determinista de laboratorios."""
    rng = random.Random(SEMILLA_FIJA)
    capacidades = (20, 24, 30, 32, 36, 40, 45)
    propositos = (
        "Programación y desarrollo de software",
        "Redes y telecomunicaciones",
        "Bases de datos y analítica",
        "Sistemas operativos y arquitectura",
        "Docencia informática de propósito general",
    )
    fecha_base = datetime(2021, 1, 4, 8, 0, tzinfo=timezone.utc)
    laboratorios: list[dict[str, object]] = []

    for indice in range(TOTAL_LABORATORIOS):
        numero_piso = indice // 10 + 1
        estado = rng.choices(
            ESTADOS_LABORATORIO, weights=PESOS_ESTADOS_LABORATORIO, k=1
        )[0]
        creado_en = fecha_base + timedelta(days=indice * 3)
        actualizado_en = creado_en + timedelta(days=rng.randint(30, 900))
        laboratorios.append(
            {
                "laboratorio_id": uuid_determinista("laboratorio", indice),
                "piso_id": uuid_determinista("piso", numero_piso),
                "codigo": f"LAB-{indice + 1:03d}",
                "nombre": f"Laboratorio Informático {indice + 1:02d}",
                "capacidad": rng.choice(capacidades),
                "descripcion": propositos[indice % len(propositos)],
                "estado": estado,
                "activo": str(estado != "INACTIVO").lower(),
                "creado_en": formato_timestamp(creado_en),
                "actualizado_en": formato_timestamp(actualizado_en),
            }
        )

    return laboratorios


def iterar_solicitudes(
    laboratorios: list[dict[str, object]],
) -> Iterator[dict[str, object]]:
    """Produce exactamente TOTAL_SOLICITUDES filas sin almacenarlas juntas."""
    rng = random.Random(SEMILLA_FIJA)
    fecha_inicial = date(2024, 1, 1)
    inicios_minutos = tuple(range(7 * 60, 19 * 60 + 1, 30))
    duraciones_minutos = (60, 90, 120, 180)

    for indice in range(TOTAL_SOLICITUDES):
        laboratorio = laboratorios[rng.randrange(len(laboratorios))]
        capacidad = int(laboratorio["capacidad"])
        fecha_reserva = fecha_inicial + timedelta(days=rng.randrange(730))
        inicio_minutos = rng.choice(inicios_minutos)
        duracion = rng.choice(duraciones_minutos)
        fin_minutos = inicio_minutos + duracion
        hora_inicio = time(inicio_minutos // 60, inicio_minutos % 60)
        hora_fin = time(fin_minutos // 60, fin_minutos % 60)

        instante_reserva = datetime.combine(
            fecha_reserva, hora_inicio, tzinfo=timezone.utc
        )
        anticipacion = timedelta(
            days=rng.randint(1, 90),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )
        creada_en = instante_reserva - anticipacion
        max_actualizacion_minutos = max(
            0, min(int(anticipacion.total_seconds() // 60), 14 * 24 * 60)
        )
        actualizada_en = creada_en + timedelta(
            minutes=rng.randint(0, max_actualizacion_minutos)
        )

        yield {
            "solicitud_id": uuid_determinista("solicitud", indice),
            "solicitante_id": uuid_determinista(
                "solicitante", rng.randrange(25_000)
            ),
            "docente_id": uuid_determinista("docente", rng.randrange(1_200)),
            "laboratorio_id": laboratorio["laboratorio_id"],
            "materia_id": uuid_determinista("materia", rng.randrange(350)),
            "periodo_lectivo_id": uuid_determinista(
                "periodo_lectivo", (fecha_reserva.year - 2024) * 2
                + (1 if fecha_reserva.month > 6 else 0)
            ),
            "fecha_reserva": fecha_reserva.isoformat(),
            "hora_inicio": hora_inicio.isoformat(),
            "hora_fin": hora_fin.isoformat(),
            "numero_participantes": rng.randint(5, capacidad),
            "motivo": rng.choice(MOTIVOS),
            "estado": rng.choices(
                ESTADOS_SOLICITUD, weights=PESOS_ESTADOS_SOLICITUD, k=1
            )[0],
            "creada_en": formato_timestamp(creada_en),
            "actualizada_en": formato_timestamp(actualizada_en),
        }


def escribir_csv_atomico(
    ruta: Path,
    columnas: list[str],
    filas: Iterator[dict[str, object]],
) -> None:
    """Escribe en un temporal y reemplaza el destino solo al finalizar."""
    ruta_temporal = ruta.with_suffix(ruta.suffix + ".tmp")
    try:
        with ruta_temporal.open("w", encoding="utf-8", newline="") as archivo:
            escritor = csv.DictWriter(archivo, fieldnames=columnas)
            escritor.writeheader()
            escritor.writerows(filas)
        os.replace(ruta_temporal, ruta)
    finally:
        if ruta_temporal.exists():
            ruta_temporal.unlink()


def generar_dataset() -> None:
    """Genera ambos CSV de forma determinista e íntegra."""
    DIRECTORIO_DATOS.mkdir(parents=True, exist_ok=True)
    laboratorios = construir_laboratorios()
    escribir_csv_atomico(
        RUTA_LABORATORIOS, COLUMNAS_LABORATORIOS, iter(laboratorios)
    )
    escribir_csv_atomico(
        RUTA_SOLICITUDES,
        COLUMNAS_SOLICITUDES,
        iterar_solicitudes(laboratorios),
    )


def sha256_archivo(ruta: Path) -> str:
    """Calcula una huella para comparar ejecuciones reproducibles."""
    resumen = hashlib.sha256()
    with ruta.open("rb") as archivo:
        for bloque in iter(lambda: archivo.read(1024 * 1024), b""):
            resumen.update(bloque)
    return resumen.hexdigest()


def verificar_dataset() -> None:
    """Verifica de forma integral los dos CSV y publica sus huellas SHA-256."""
    if not RUTA_LABORATORIOS.is_file() or not RUTA_SOLICITUDES.is_file():
        raise FileNotFoundError("Deben existir ambos archivos CSV antes de verificarlos")

    capacidades_laboratorios: dict[str, int] = {}
    codigos_laboratorios: set[str] = set()
    total_laboratorios = 0
    with RUTA_LABORATORIOS.open(encoding="utf-8", newline="") as archivo:
        lector = csv.DictReader(archivo)
        if lector.fieldnames != COLUMNAS_LABORATORIOS:
            raise ValueError("El esquema de laboratorios.csv no es el esperado")
        for numero_fila, fila in enumerate(lector, start=2):
            total_laboratorios += 1
            vacias = [columna for columna in COLUMNAS_LABORATORIOS if not fila[columna].strip()]
            if vacias:
                raise ValueError(
                    f"Valores obligatorios nulos o vacíos en laboratorios.csv, "
                    f"fila {numero_fila}: {vacias}"
                )
            laboratorio_id = fila["laboratorio_id"]
            if laboratorio_id in capacidades_laboratorios:
                raise ValueError(f"laboratorio_id duplicado: {laboratorio_id}")
            if fila["codigo"] in codigos_laboratorios:
                raise ValueError(f"codigo de laboratorio duplicado: {fila['codigo']}")
            capacidad = int(fila["capacidad"])
            if capacidad <= 0:
                raise ValueError(f"Capacidad no positiva en laboratorio {laboratorio_id}")
            capacidades_laboratorios[laboratorio_id] = capacidad
            codigos_laboratorios.add(fila["codigo"])

    if total_laboratorios != TOTAL_LABORATORIOS:
        raise ValueError(
            f"Se esperaban {TOTAL_LABORATORIOS} laboratorios y se encontraron "
            f"{total_laboratorios}"
        )

    ids_solicitudes: set[str] = set()
    total = 0
    with RUTA_SOLICITUDES.open(encoding="utf-8", newline="") as archivo:
        lector = csv.DictReader(archivo)
        if lector.fieldnames != COLUMNAS_SOLICITUDES:
            raise ValueError("El esquema de solicitudes_reserva.csv no es el esperado")
        for numero_fila, fila in enumerate(lector, start=2):
            total += 1
            vacias = [columna for columna in COLUMNAS_SOLICITUDES if not fila[columna].strip()]
            if vacias:
                raise ValueError(
                    f"Valores obligatorios nulos o vacíos en solicitudes_reserva.csv, "
                    f"fila {numero_fila}: {vacias}"
                )
            solicitud_id = fila["solicitud_id"]
            if solicitud_id in ids_solicitudes:
                raise ValueError(f"solicitud_id duplicado: {solicitud_id}")
            ids_solicitudes.add(solicitud_id)
            laboratorio_id = fila["laboratorio_id"]
            if laboratorio_id not in capacidades_laboratorios:
                raise ValueError(
                    f"laboratorio_id inexistente: {laboratorio_id}"
                )
            hora_inicio = time.fromisoformat(fila["hora_inicio"])
            hora_fin = time.fromisoformat(fila["hora_fin"])
            if hora_fin <= hora_inicio:
                raise ValueError(f"hora_fin no posterior en solicitud {solicitud_id}")
            participantes = int(fila["numero_participantes"])
            if participantes <= 0:
                raise ValueError(f"Participantes no positivos en solicitud {solicitud_id}")
            if participantes > capacidades_laboratorios[laboratorio_id]:
                raise ValueError(
                    f"Participantes superiores a la capacidad en solicitud {solicitud_id}"
                )

    if total != TOTAL_SOLICITUDES:
        raise ValueError(
            f"Se esperaban {TOTAL_SOLICITUDES} solicitudes y se encontraron {total}"
        )

    print(f"Solicitudes verificadas: {total}")
    print(f"Laboratorios verificados: {total_laboratorios}")
    print("Esquemas y columnas: correctos")
    print("Identificadores únicos: correctos")
    print("Integridad referencial: correcta")
    print("Valores obligatorios: sin nulos ni vacíos")
    print("Reglas de horario, participantes y capacidad: correctas")
    print(f"SHA-256 solicitudes: {sha256_archivo(RUTA_SOLICITUDES)}")
    print(f"SHA-256 laboratorios: {sha256_archivo(RUTA_LABORATORIOS)}")


def analizar_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verificar",
        action="store_true",
        help="verifica CSV existentes sin volver a generarlos",
    )
    return parser.parse_args()


def main() -> None:
    argumentos = analizar_argumentos()
    if argumentos.verificar:
        verificar_dataset()
        return

    generar_dataset()
    print(f"Dataset generado con semilla fija {SEMILLA_FIJA}")
    print(f"Solicitudes generadas: {TOTAL_SOLICITUDES}")
    print(f"Laboratorios generados: {TOTAL_LABORATORIOS}")


if __name__ == "__main__":
    main()
