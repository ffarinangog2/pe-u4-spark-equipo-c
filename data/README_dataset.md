# Dataset sintético de solicitudes de reserva de laboratorios

## Dominio, naturaleza y propósito

**Dominio:** FUVV – Laboratorios Informáticos.

Este dataset es **sintético y reproducible**. Se utiliza porque no existen 500 000 registros operacionales públicos adecuados para este experimento, porque permite controlar el volumen y conservar el esquema conceptual requerido, y porque evita utilizar información personal real de estudiantes, docentes u otros usuarios. Los registros **no son datos reales ni operacionales de la Universidad Técnica Estatal de Quevedo (UTEQ)**; se crearon exclusivamente para fines académicos y para comparar cargas y transformaciones equivalentes en pandas y PySpark.

## Reproducción y verificación

- Fuente: **Generación sintética reproducible mediante script propio**.
- Generador: `src/generar_dataset.py`.
- Semilla fija: `20260804`.
- Fecha real de última regeneración: **2026-08-05**.
- Comando exacto de generación: `python src/generar_dataset.py`.
- Comando exacto de verificación: `python src/generar_dataset.py --verificar`.
- URL permanente del script: **https://github.com/ffarinangog2/pe-u4-spark-equipo-c**.
- Licencia del dataset: **MIT**, conforme al archivo `LICENSE` de la raíz, que cubre el software y los archivos de datos sintéticos publicados con este proyecto.

Dos regeneraciones consecutivas realizadas con la misma semilla produjeron las mismas huellas SHA-256 indicadas a continuación.

## Archivos reales

| Archivo | Naturaleza | Registros | Columnas | Tamaño exacto (bytes) | Tamaño (MiB) | SHA-256 |
|---|---|---:|---:|---:|---:|---|
| `solicitudes_reserva.csv` | Tabla principal sintética de solicitudes | 500 000 | 14 | 176 712 866 | 168.53 | `974402873a0b7e6a6f18b4b90f43e146a4a4b6524561ad1769c3077437035783` |
| `laboratorios.csv` | Tabla dimensional sintética de laboratorios | 40 | 10 | 8 754 | 0.01 | `756567789ea2ad9483918b6305a66f937d511f693b66e404bea2d2c39a9adbc5` |

Los conteos excluyen la fila de encabezado. Los tamaños y hashes se calcularon directamente sobre los archivos regenerados.

## Esquema de `solicitudes_reserva.csv`

| Columna | Tipo de dato al cargar | Descripción |
|---|---|---|
| `solicitud_id` | string (UUID) | Identificador único de la solicitud. |
| `solicitante_id` | string (UUID) | Identificador sintético del solicitante. |
| `docente_id` | string (UUID) | Identificador sintético del docente. |
| `laboratorio_id` | string (UUID) | Clave foránea hacia `laboratorios.csv`. |
| `materia_id` | string (UUID) | Identificador sintético de la materia. |
| `periodo_lectivo_id` | string (UUID) | Identificador sintético del periodo lectivo. |
| `fecha_reserva` | date | Fecha solicitada. |
| `hora_inicio` | time | Hora de inicio. |
| `hora_fin` | time | Hora de fin, posterior a `hora_inicio`. |
| `numero_participantes` | integer | Participantes, mayor que cero y no superior a la capacidad. |
| `motivo` | string | Motivo académico o institucional sintético. |
| `estado` | string | Estado de la solicitud. |
| `creada_en` | timestamp UTC | Creación de la solicitud. |
| `actualizada_en` | timestamp UTC | Última actualización de la solicitud. |

## Esquema de `laboratorios.csv`

| Columna | Tipo de dato al cargar | Descripción |
|---|---|---|
| `laboratorio_id` | string (UUID) | Identificador único del laboratorio. |
| `piso_id` | string (UUID) | Identificador sintético del piso. |
| `codigo` | string | Código único del laboratorio. |
| `nombre` | string | Nombre sintético del laboratorio. |
| `capacidad` | integer | Capacidad máxima positiva. |
| `descripcion` | string | Propósito del laboratorio. |
| `estado` | string | Estado del laboratorio. |
| `activo` | boolean | Indicador de habilitación. |
| `creado_en` | timestamp UTC | Creación del registro. |
| `actualizado_en` | timestamp UTC | Última actualización del registro. |

## Relación

La relación es **uno a muchos (1:N)** mediante `laboratorio_id`: cada solicitud referencia exactamente un laboratorio existente y un laboratorio puede aparecer en muchas solicitudes.

```text
laboratorios (1) ───────── (N) solicitudes_reserva
laboratorio_id                 laboratorio_id
```
