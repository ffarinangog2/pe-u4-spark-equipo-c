"""Generación de figuras oficiales de la práctica PE-U4.

El script consume exclusivamente los CSV oficiales de resultados. No importa
transformaciones, no accede al dataset y no inicia Spark.
"""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
RUTA_RESUMEN = RAIZ_PROYECTO / "resultados" / "tiempos_resumen.csv"
RUTA_METRICAS = RAIZ_PROYECTO / "resultados" / "metricas_derivadas.csv"
DIRECTORIO_FIGURAS = RAIZ_PROYECTO / "resultados" / "figuras"

TRANSFORMACIONES = ("T1", "T2", "T3", "T4", "T5")
NUMEROS_EXECUTORS = (1, 2, 4)
FRACCIONES_PARALELAS_AMDAHL = (0.5, 0.75, 0.90, 0.95)
DPI = 300


def _leer_csv(ruta: Path) -> list[dict[str, str]]:
    if not ruta.is_file():
        raise FileNotFoundError(f"No existe el archivo oficial: {ruta}")
    with ruta.open(encoding="utf-8", newline="") as archivo:
        return list(csv.DictReader(archivo))


def cargar_resultados() -> tuple[
    dict[tuple[str, str, str], float], dict[tuple[str, str], float]
]:
    """Carga medianas y métricas mediante claves únicas y verificables."""
    resumen: dict[tuple[str, str, str], float] = {}
    for fila in _leer_csv(RUTA_RESUMEN):
        clave = (
            fila["motor"],
            fila["transformacion"],
            fila["configuracion"],
        )
        if clave in resumen:
            raise ValueError(f"Mediana oficial duplicada: {clave}")
        resumen[clave] = float(fila["mediana_segundos"])

    metricas: dict[tuple[str, str], float] = {}
    for fila in _leer_csv(RUTA_METRICAS):
        clave = (fila["tipo_metrica"], fila["configuracion_n"])
        # Los speedups pandas/PySpark comparten executors[4], por lo que T1--T5
        # forman parte de la clave únicamente para ese tipo de métrica.
        if fila["tipo_metrica"] == "speedup_pandas_pyspark":
            clave = (fila["tipo_metrica"], fila["transformacion"])
        if clave in metricas:
            raise ValueError(f"Métrica oficial duplicada: {clave}")
        metricas[clave] = float(fila["valor"])

    requeridas_resumen = {
        *(('pandas', t, 'pandas') for t in TRANSFORMACIONES),
        *(('pyspark', t, 'executors[4]') for t in TRANSFORMACIONES),
    }
    faltantes_resumen = requeridas_resumen.difference(resumen)
    if faltantes_resumen:
        raise ValueError(f"Faltan medianas oficiales: {sorted(faltantes_resumen)}")

    requeridas_metricas = {
        *(('speedup_pandas_pyspark', t) for t in TRANSFORMACIONES),
        *(('speedup_experimental_t3', f'executors[{n}]') for n in NUMEROS_EXECUTORS),
        *(('eficiencia_porcentaje_t3', f'executors[{n}]') for n in NUMEROS_EXECUTORS),
        *(('speedup_teorico_amdahl', f'N={n}') for n in NUMEROS_EXECUTORS),
        *(('speedup_gustafson', f'N={n}') for n in NUMEROS_EXECUTORS),
    }
    faltantes_metricas = requeridas_metricas.difference(metricas)
    if faltantes_metricas:
        raise ValueError(f"Faltan métricas oficiales: {sorted(faltantes_metricas)}")
    return resumen, metricas


def _nueva_figura():
    figura = plt.figure(figsize=(9, 5.5), constrained_layout=True)
    eje = figura.gca()
    eje.grid(axis="y", linestyle="--", alpha=0.35)
    return figura, eje


def _guardar(figura, nombre: str, *nombres_adicionales: str) -> None:
    DIRECTORIO_FIGURAS.mkdir(parents=True, exist_ok=True)
    for nombre_salida in (nombre, *nombres_adicionales):
        figura.savefig(
            DIRECTORIO_FIGURAS / nombre_salida,
            dpi=DPI,
            bbox_inches="tight",
            facecolor="white",
        )
    plt.close(figura)


def grafica_tiempos(resumen: dict[tuple[str, str, str], float]) -> None:
    pandas_tiempos = [resumen[("pandas", t, "pandas")] for t in TRANSFORMACIONES]
    spark_tiempos = [resumen[("pyspark", t, "executors[4]")] for t in TRANSFORMACIONES]
    posiciones = list(range(len(TRANSFORMACIONES)))
    ancho = 0.36
    figura, eje = _nueva_figura()
    barras_pandas = eje.bar(
        [x - ancho / 2 for x in posiciones],
        pandas_tiempos,
        ancho,
        label="pandas",
        color="#4472C4",
    )
    barras_spark = eje.bar(
        [x + ancho / 2 for x in posiciones],
        spark_tiempos,
        ancho,
        label="PySpark executors[4]",
        color="#ED7D31",
    )
    eje.bar_label(barras_pandas, fmt="%.3f", padding=3, fontsize=8)
    eje.bar_label(barras_spark, fmt="%.3f", padding=3, fontsize=8)
    eje.set_xticks(posiciones, TRANSFORMACIONES)
    eje.set_xlabel("Transformación")
    eje.set_ylabel("Tiempo mediano (segundos)")
    eje.set_title("PE-U4: tiempo mediano de pandas frente a PySpark executors[4]")
    eje.legend()
    _guardar(figura, "01_tiempos_pandas_vs_pyspark.png", "fig1_barras.png")


def grafica_speedup_pandas_pyspark(
    metricas: dict[tuple[str, str], float]
) -> None:
    valores = [metricas[("speedup_pandas_pyspark", t)] for t in TRANSFORMACIONES]
    colores = ["#70AD47" if valor > 1 else "#4472C4" for valor in valores]
    figura, eje = _nueva_figura()
    barras = eje.bar(TRANSFORMACIONES, valores, color=colores)
    eje.bar_label(barras, fmt="%.3f", padding=3, fontsize=9)
    eje.axhline(1.0, color="#C00000", linestyle="--", linewidth=1.5, label="S = 1")
    eje.set_xlabel("Transformación")
    eje.set_ylabel("Speedup T_pandas / T_PySpark")
    eje.set_title("PE-U4: speedup oficial de PySpark executors[4] respecto de pandas")
    eje.legend(title="S > 1: ventaja PySpark\nS < 1: ventaja pandas")
    _guardar(figura, "02_speedup_pandas_vs_pyspark.png")


def grafica_escalabilidad_t3(metricas: dict[tuple[str, str], float]) -> None:
    experimental = [
        metricas[("speedup_experimental_t3", f"executors[{n}]")]
        for n in NUMEROS_EXECUTORS
    ]
    figura, eje = _nueva_figura()
    eje.plot(
        NUMEROS_EXECUTORS,
        experimental,
        marker="o",
        linewidth=2,
        label="Speedup experimental T3",
        color="#4472C4",
    )
    eje.plot(
        NUMEROS_EXECUTORS,
        NUMEROS_EXECUTORS,
        marker="s",
        linestyle="--",
        label="Speedup ideal S=N",
        color="#70AD47",
    )
    eje.set_xticks(NUMEROS_EXECUTORS, [f"executors[{n}]" for n in NUMEROS_EXECUTORS])
    eje.set_xlabel("N: procesos executor Spark Standalone")
    eje.set_ylabel("Speedup")
    eje.set_title("PE-U4: escalabilidad experimental local de T3")
    eje.legend()
    _guardar(figura, "03_escalabilidad_t3.png")


def grafica_eficiencia_t3(metricas: dict[tuple[str, str], float]) -> None:
    eficiencia = [
        metricas[("eficiencia_porcentaje_t3", f"executors[{n}]")]
        for n in NUMEROS_EXECUTORS
    ]
    figura, eje = _nueva_figura()
    barras = eje.bar(
        [f"executors[{n}]" for n in NUMEROS_EXECUTORS],
        eficiencia,
        color="#5B9BD5",
        width=0.55,
    )
    eje.bar_label(barras, fmt="%.2f %%", padding=3)
    eje.set_ylim(0, 110)
    eje.set_xlabel("N: procesos executor Spark Standalone")
    eje.set_ylabel("Eficiencia paralela (%)")
    eje.set_title("PE-U4: eficiencia paralela experimental de T3")
    _guardar(figura, "04_eficiencia_t3.png", "fig3_eficiencia.png")


def grafica_amdahl_t3(metricas: dict[tuple[str, str], float]) -> None:
    experimental = [
        metricas[("speedup_experimental_t3", f"executors[{n}]")]
        for n in NUMEROS_EXECUTORS
    ]
    amdahl = [
        metricas[("speedup_teorico_amdahl", f"N={n}")]
        for n in NUMEROS_EXECUTORS
    ]
    figura, eje = _nueva_figura()
    eje.plot(
        NUMEROS_EXECUTORS,
        experimental,
        marker="o",
        linewidth=2,
        label="Experimental T3",
        color="#4472C4",
    )
    eje.plot(
        NUMEROS_EXECUTORS,
        amdahl,
        marker="s",
        linestyle="--",
        linewidth=2,
        label="Amdahl teórico (f oficial)",
        color="#C55A11",
    )
    eje.set_xticks(NUMEROS_EXECUTORS)
    eje.set_xlabel("N: procesos executor Spark Standalone")
    eje.set_ylabel("Speedup")
    eje.set_title("PE-U4: T3 experimental frente al modelo de Amdahl")
    eje.legend()
    _guardar(figura, "05_amdahl_t3.png", "fig2_speedup.png")


def grafica_gustafson_t3(metricas: dict[tuple[str, str], float]) -> None:
    gustafson = [
        metricas[("speedup_gustafson", f"N={n}")]
        for n in NUMEROS_EXECUTORS
    ]
    figura, eje = _nueva_figura()
    eje.plot(
        NUMEROS_EXECUTORS,
        gustafson,
        marker="o",
        linewidth=2.2,
        color="#7030A0",
        label="Proyección teórica de Gustafson",
    )
    for n, valor in zip(NUMEROS_EXECUTORS, gustafson):
        eje.annotate(f"{valor:.3f}", (n, valor), xytext=(0, 8), textcoords="offset points", ha="center")
    eje.set_xticks(NUMEROS_EXECUTORS)
    eje.set_xlabel("N: procesos executor Spark Standalone")
    eje.set_ylabel("Speedup escalado")
    eje.set_title("PE-U4: proyección de Gustafson basada en la fracción serial observada")
    eje.legend()
    _guardar(figura, "06_gustafson_t3.png")


def grafica_curvas_teoricas_amdahl() -> None:
    """Genera S(N) para las cuatro fracciones paralelas exigidas por la guía."""
    procesadores = tuple(range(1, 65))
    figura, eje = _nueva_figura()
    for fraccion_paralela in FRACCIONES_PARALELAS_AMDAHL:
        speedups = [
            1 / ((1 - fraccion_paralela) + fraccion_paralela / n)
            for n in procesadores
        ]
        eje.plot(
            procesadores,
            speedups,
            linewidth=2,
            label=f"p = {fraccion_paralela:.2f}",
        )
    eje.set_xlabel("N: número de procesadores")
    eje.set_ylabel("Speedup teórico S(N)")
    eje.set_title("Ley de Amdahl para distintas fracciones paralelizables")
    eje.legend(title="Fracción paralelizable")
    _guardar(figura, "07_curvas_amdahl_fracciones.png")


def generar_figuras() -> None:
    resumen, metricas = cargar_resultados()
    grafica_tiempos(resumen)
    grafica_speedup_pandas_pyspark(metricas)
    grafica_escalabilidad_t3(metricas)
    grafica_eficiencia_t3(metricas)
    grafica_amdahl_t3(metricas)
    grafica_gustafson_t3(metricas)
    grafica_curvas_teoricas_amdahl()


if __name__ == "__main__":
    generar_figuras()
