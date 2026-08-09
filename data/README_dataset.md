# Dataset sintético de solicitudes de reserva de laboratorios

**GA-SUM-05 / PE-U4 — FUVV — Sistema de Control de Laboratorios Informáticos (SCLI)**

## Descripción

Este directorio contiene el conjunto de datos utilizado en la práctica experimental **GA-SUM-05 / PE-U4 — Procesamiento Distribuido con Apache Spark**. El dataset es **sintético, reproducible y de uso exclusivamente académico**. Fue creado para ejecutar y comparar las mismas transformaciones mediante **pandas** y **PySpark 3.5.0**.

Los registros no corresponden a datos reales u operacionales de la Universidad Técnica Estatal de Quevedo (UTEQ) y no contienen información personal real.

| Archivo                   | Registros | Columnas | Tamaño aproximado |
| ------------------------- | --------: | -------: | ----------------: |
| `solicitudes_reserva.csv` |   500 000 |       14 |        168.53 MiB |
| `laboratorios.csv`        |        40 |       10 |          0.01 MiB |

## Generación y verificación

El dataset puede generarse nuevamente mediante:

```bash
python src/generar_dataset.py
```

Para comprobar su integridad:

```bash
python src/generar_dataset.py --verificar
```

Parámetros principales:

- **Generador:** `src/generar_dataset.py`
- **Semilla fija:** `20260804`
- **Fecha documentada:** `2026-08-05`
- **Licencia:** MIT

La verificación comprueba la cantidad de registros, estructura de columnas, identificadores únicos, integridad referencial, valores obligatorios, horarios, número de participantes, capacidad de los laboratorios y huellas criptográficas SHA-256.

## Archivos de gran tamaño no incluidos en GitHub

Debido a las restricciones de tamaño de archivos de GitHub, algunos archivos generados y utilizados durante la práctica no se almacenan directamente mediante el flujo convencional de Git.

El caso principal es:

```text
data/solicitudes_reserva.csv
```

Este archivo contiene **500 000 registros, 14 columnas y 176 712 866 bytes (aprox. 168.53 MiB)**.

Su ausencia en GitHub se debe exclusivamente al tamaño del archivo y **no significa que haya sido omitido durante la ejecución del experimento**.

La reproducibilidad no se ve afectada, porque el dataset puede reconstruirse localmente mediante el script generador, utilizando la semilla fija y verificando posteriormente su huella SHA-256.

### Huellas SHA-256

`solicitudes_reserva.csv`

```text
974402873a0b7e6a6f18b4b90f43e146a4a4b6524561ad1769c3077437035783
```

`laboratorios.csv`

```text
756567789ea2ad9483918b6305a66f937d511f693b66e404bea2d2c39a9adbc5
```

Algunas salidas intermedias generadas por pandas y PySpark también pueden alcanzar tamaños elevados y, por esta razón, no mantenerse versionadas en GitHub. Estas salidas son productos derivados y pueden regenerarse ejecutando nuevamente las transformaciones.

El repositorio prioriza la conservación del **código fuente, scripts, tiempos experimentales, métricas, figuras, notebook, evidencias y documentación necesarios para reproducir el experimento**.

## Relación entre los datasets

Las tablas se relacionan mediante el atributo `laboratorio_id`, estableciendo una relación **1:N**, donde cada solicitud referencia un laboratorio y un laboratorio puede estar asociado con múltiples solicitudes.

## Transformaciones evaluadas

Durante el experimento se ejecutan cinco transformaciones equivalentes en pandas y PySpark:

- **T1:** filtrado compuesto y selección de columnas.
- **T2:** agrupación por laboratorio y cálculo de agregaciones.
- **T3:** `INNER JOIN` entre solicitudes y laboratorios.
- **T4:** cálculo de una columna derivada de prioridad.
- **T5:** ordenamiento y selección del **Top-20 de laboratorios de mayor demanda**.

## Esquema resumido

### `solicitudes_reserva.csv`

Contiene los siguientes atributos:

```text
solicitud_id
solicitante_id
docente_id
laboratorio_id
materia_id
periodo_lectivo_id
fecha_reserva
hora_inicio
hora_fin
numero_participantes
motivo
estado
creada_en
actualizada_en
```

### `laboratorios.csv`

Contiene los siguientes atributos:

```text
laboratorio_id
piso_id
codigo
nombre
capacidad
descripcion
estado
activo
creado_en
actualizado_en
```

## Implementaciones y salidas

Las implementaciones utilizadas para realizar las transformaciones se encuentran en:

```text
src/transformaciones_pandas.py
src/transformaciones_spark.py
```

Las salidas generadas durante el procesamiento se organizan en:

```text
data/pandas/
data/spark/
```

Los resultados experimentales oficiales se almacenan principalmente en:

```text
resultados/tiempos_crudos.csv
resultados/tiempos_resumen.csv
resultados/metricas_derivadas.csv
```

## Reproducibilidad

La reproducibilidad del experimento se sustenta en:

- el script generador del dataset;
- la semilla fija `20260804`;
- el esquema de datos documentado;
- las huellas SHA-256;
- las implementaciones equivalentes en pandas y PySpark;
- los archivos de resultados experimentales.

De esta manera, cualquier revisor puede regenerar localmente los archivos, comprobar su integridad y volver a ejecutar las transformaciones utilizadas durante la práctica.

## Privacidad

Todos los identificadores, fechas, motivos, estados y demás valores incluidos en los datasets fueron generados artificialmente.

No se utilizaron nombres reales, correos electrónicos, números de identificación ni registros institucionales reales pertenecientes a estudiantes, docentes o personal de la UTEQ.

## Licencia

El dataset sintético y los scripts asociados se distribuyen bajo la **Licencia MIT**.

## Nota sobre el repositorio

La ausencia de determinados archivos de gran tamaño en GitHub **no representa una ausencia de evidencia experimental**. Los archivos pesados pueden reconstruirse utilizando los scripts incluidos y comprobarse mediante sus respectivas huellas SHA-256.

Esta estrategia permite mantener el repositorio reproducible y verificable, evitando al mismo tiempo problemas derivados de las restricciones de tamaño establecidas por GitHub.
