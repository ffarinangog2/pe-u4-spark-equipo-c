# PE-U4 — Procesamiento Distribuido con Apache Spark: comprobación experimental de la Ley de Amdahl

**GA-SUM-05 / PE-U4 · Aplicaciones Distribuidas (ISR-701) · Unidad 4 — Cómputo Paralelo y Distribuido**

**Universidad Técnica Estatal de Quevedo (UTEQ)**  
Facultad de Ciencias de la Computación  
Carrera de Ingeniería de Software  
Séptimo nivel — Paralelo A  
Período académico 2026–2027 PPA  
Docente: Gleiston C. Guerrero-Ulloa, M.Sc.  
Equipo C

---

## Descripción

Este repositorio contiene la práctica experimental de la Unidad 4 de la asignatura **Aplicaciones Distribuidas**, orientada a comprobar experimentalmente la **Ley de Amdahl** mediante la implementación y medición de un pipeline de cinco transformaciones de datos (**T1–T5**).

El procesamiento se ejecuta de dos maneras:

- **Secuencial:** utilizando pandas.
- **Distribuida:** utilizando PySpark 3.5.0 sobre un clúster Spark Standalone real.

El experimento utiliza un conjunto de datos sintético y reproducible compuesto por **500 000 solicitudes de reserva de laboratorios** y **40 laboratorios** pertenecientes al dominio del PFC FUVV.

Cada transformación se mide utilizando `time.perf_counter()`, realizando una ejecución de calentamiento descartada y posteriormente cinco repeticiones oficiales. Como estadístico central se utiliza la **mediana**.

Los resultados permiten calcular:

- tiempos de ejecución;
- speedup experimental;
- eficiencia paralela;
- fracción no escalable observada;
- límite máximo de speedup según la Ley de Amdahl;
- comparación con el modelo de Gustafson-Barsis.

El análisis completo se encuentra documentado en el informe LaTeX disponible en `docs/`.

---

## PFC de referencia

**Código:** FUVV  
**Sistema:** Sistema de Control de Laboratorios Informáticos (SCLI)

El dominio fue seleccionado porque un sistema de laboratorios genera continuamente solicitudes de reserva, registros de asistencia, utilización de equipos y eventos de monitoreo.

El pipeline implementado permite analizar estos datos para apoyar decisiones relacionadas con la planificación de capacidad y priorización de reservas.

---

## Integrantes

| Integrante | PFC de origen | Rol asumido |
|---|---|---|
| **Freddy Farinango** | FUVV | Coordinación del equipo, despliegue del clúster Spark Standalone, protocolo experimental y redacción LaTeX |
| **Jeremy Gaibor** | FUVV | Implementación de las transformaciones T1–T5 en pandas y PySpark, medición de tiempos y redacción LaTeX |
| **Iván Villamarín** | FUVV | Análisis cuantitativo, generación de figuras, verificación de equivalencia y redacción LaTeX |

La redacción, estructuración y compilación del documento LaTeX fue una labor colaborativa de los tres integrantes.

---

## Pipeline de transformaciones

El experimento implementa las mismas cinco transformaciones tanto en pandas como en PySpark.

### T1 — Filtrado compuesto y selección

Filtra solicitudes que cumplen determinadas condiciones de estado, cantidad de participantes y rango de fechas, conservando únicamente las columnas necesarias.

### T2 — Agrupación y agregaciones

Agrupa las solicitudes por `laboratorio_id` y calcula estadísticas de demanda, incluyendo conteo, promedio y máximo de participantes.

### T3 — Join

Realiza un `inner join` entre las solicitudes de reserva y la dimensión de laboratorios mediante `laboratorio_id`.

Esta transformación se utiliza además para estudiar la escalabilidad utilizando:

- 1 executor;
- 2 executors;
- 4 executors.

### T4 — Columna derivada de prioridad

Calcula un puntaje determinista de prioridad considerando:

- número de participantes;
- duración de la reserva;
- porcentaje de ocupación;
- anticipación;
- reserva durante fin de semana;
- estado de aprobación.

### T5 — Ordenamiento y Top-N

Ordena las solicitudes según su nivel de demanda y obtiene las **20 solicitudes de mayor prioridad/demanda**.

---

## Dataset

El conjunto de datos utilizado es **sintético y reproducible**.

Se genera mediante:

```text
src/generar_dataset.py
```

La generación utiliza una semilla fija:

```text
20260804
```

El conjunto generado contiene:

| Archivo | Registros | Columnas |
|---|---:|---:|
| `solicitudes_reserva.csv` | 500 000 | 14 |
| `laboratorios.csv` | 40 | 10 |

El uso de una semilla fija permite regenerar exactamente el mismo conjunto de datos y comprobar su integridad mediante huellas SHA-256.

---

## Tecnologías utilizadas

- Python 3.12
- PySpark 3.5.0
- pandas 2.3.3
- NumPy 1.26.4
- matplotlib 3.11.1
- Jupyter 1.1.1
- Apache Spark Standalone
- LaTeX
- IEEEtran
- biblatex
- Biber

---

## Configuración del experimento Spark

La configuración utilizada durante las mediciones es:

| Parámetro | Valor |
|---|---|
| Gestor de clúster | Spark Standalone |
| Spark Master | `spark://127.0.0.1:7077` |
| Executors utilizados | 1, 2 y 4 |
| Cores por executor | 1 |
| Memoria por executor | 512 MiB |
| Repeticiones | 5 |
| Calentamiento | 1 ejecución descartada |
| Estadístico | Mediana |
| Reloj | `time.perf_counter()` |

No se utiliza `local[N]` para representar los executors del experimento.

---

## Estructura del repositorio

```text
pe-u4-spark-equipo-c/
│
├── data/
│   ├── laboratorios.csv
│   ├── solicitudes_reserva.csv
│   ├── README_dataset.md
│   ├── pandas/
│   └── spark/
│
├── docs/
│   ├── PE_U4_Informe.tex
│   ├── PE_U4_Informe.pdf
│   └── references_U4.bib
│
├── evidencia/
│   └── capturas de Spark UI y Spark Standalone
│
├── notebooks/
│   ├── PE_U4_pipeline_spark.ipynb
│   └── exportación HTML del notebook
│
├── resultados/
│   ├── tiempos_crudos.csv
│   ├── tiempos_resumen.csv
│   ├── metricas_derivadas.csv
│   └── figuras/
│
├── src/
│   ├── carga_datos.py
│   ├── generar_dataset.py
│   ├── graficas.py
│   ├── medicion.py
│   ├── transformaciones_pandas.py
│   └── transformaciones_spark.py
│
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt
```

---

# Reproducción del proyecto

## 1. Clonar el repositorio

```bash
git clone https://github.com/ffarinangog2/pe-u4-spark-equipo-c.git
cd pe-u4-spark-equipo-c
```

---

## 2. Crear el entorno virtual

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Si PowerShell bloquea temporalmente la activación del entorno:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

---

## 3. Instalar las dependencias

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Las versiones utilizadas por el proyecto se encuentran fijadas en `requirements.txt`.

---

## 4. Generar el dataset

Desde la raíz del repositorio ejecutar:

```bash
python src/generar_dataset.py
```

El script genera automáticamente:

```text
data/solicitudes_reserva.csv
data/laboratorios.csv
```

El dataset se genera utilizando la semilla fija `20260804`.

> Debido al tamaño de `solicitudes_reserva.csv`, el archivo puede no estar disponible directamente después de clonar el repositorio. En ese caso debe regenerarse mediante el comando anterior.

---

## 5. Verificar el dataset

Una vez generado:

```bash
python src/generar_dataset.py --verificar
```

La verificación comprueba, entre otros aspectos:

- cantidad correcta de registros;
- estructura de columnas;
- identificadores únicos;
- integridad referencial;
- ausencia de valores obligatorios vacíos;
- consistencia de horarios;
- número de participantes respecto de la capacidad;
- huellas SHA-256.

---

# Ejecución con Spark Standalone

El protocolo experimental requiere un **clúster Spark Standalone previamente iniciado**.

El master esperado por el proyecto es:

```text
spark://127.0.0.1:7077
```

## 6. Iniciar el Spark Master

Abrir una terminal independiente.

Si Apache Spark se encuentra configurado en el `PATH`, puede iniciarse el Master mediante las herramientas de Spark de la instalación local.

Debe verificarse finalmente que el Master esté disponible en:

```text
spark://127.0.0.1:7077
```

La interfaz web del Master normalmente puede consultarse desde:

```text
http://127.0.0.1:8080
```

---

## 7. Iniciar el Spark Worker

Abrir una segunda terminal y conectar el worker al Master:

```text
spark://127.0.0.1:7077
```

El worker debe disponer de recursos suficientes para que el experimento pueda crear hasta **4 executors**, cada uno con:

```text
1 core
512 MiB de memoria
```

Antes de ejecutar las mediciones debe comprobarse en la interfaz del Spark Master que el worker se encuentra registrado.

---

# Ejecución del experimento

## 8. Ejecutar el protocolo completo

Con el entorno virtual activado y Spark Standalone funcionando:

```bash
python src/medicion.py --ejecutar
```

El protocolo ejecuta las transformaciones en pandas y PySpark y genera los archivos oficiales de resultados.

Entre ellos:

```text
resultados/tiempos_crudos.csv
resultados/tiempos_resumen.csv
resultados/metricas_derivadas.csv
```

El protocolo oficial utiliza cinco repeticiones por serie.

También puede indicarse explícitamente:

```bash
python src/medicion.py --ejecutar --repeticiones 5
```

---

## 9. Abrir Spark UI durante la ejecución

Para facilitar la obtención manual de evidencias de Spark UI:

```bash
python src/medicion.py --ejecutar --abrir-spark-ui
```

Esta opción abre las páginas correspondientes de Spark UI, pero **no crea automáticamente las capturas de pantalla**.

Las evidencias utilizadas en el informe se almacenan en:

```text
evidencia/
```

---

## 10. Generar las gráficas

Después de disponer de los CSV oficiales de resultados:

```bash
python src/graficas.py
```

Las figuras se generan en:

```text
resultados/figuras/
```

con una resolución de **300 DPI**.

---

## 11. Ejecutar el notebook

El notebook principal se encuentra en:

```text
notebooks/PE_U4_pipeline_spark.ipynb
```

Puede iniciarse Jupyter mediante:

```bash
jupyter notebook
```

Posteriormente abrir:

```text
notebooks/PE_U4_pipeline_spark.ipynb
```

El repositorio conserva también una exportación HTML del notebook como evidencia reproducible de la ejecución.

---

# Compilación del informe LaTeX

El documento principal del informe es:

```text
docs/PE_U4_Informe.tex
```

y la bibliografía se encuentra en:

```text
docs/references_U4.bib
```

El documento utiliza:

```text
IEEEtran
biblatex
biber
```

---

## 12. Compilar localmente con LaTeX

Se requiere una distribución LaTeX que incluya `pdflatex` y `biber`, por ejemplo MiKTeX o TeX Live.

Desde la raíz del repositorio:

```bash
cd docs
```

Ejecutar exactamente la siguiente secuencia:

```bash
pdflatex PE_U4_Informe.tex
biber PE_U4_Informe
pdflatex PE_U4_Informe.tex
pdflatex PE_U4_Informe.tex
```

La secuencia completa es necesaria para resolver correctamente:

- referencias bibliográficas;
- numeración;
- referencias cruzadas;
- tablas;
- figuras.

El PDF resultante se genera como:

```text
docs/PE_U4_Informe.pdf
```

---

# Compilación en Overleaf

El informe también puede compilarse utilizando Overleaf.

Para ello deben encontrarse disponibles dentro del proyecto:

```text
docs/PE_U4_Informe.tex
docs/references_U4.bib
resultados/figuras/
evidencia/
```

El archivo principal debe configurarse como:

```text
PE_U4_Informe.tex
```

El compilador utilizado es:

```text
pdfLaTeX
```

La bibliografía utiliza:

```text
Biber
```

---

## Modo `draft` durante el desarrollo en Overleaf

Debido a las limitaciones de tiempo de compilación en Overleaf, durante el desarrollo del informe se utiliza temporalmente el modo `draft`, permitiendo compilar el documento sin cargar las imágenes y reduciendo considerablemente el tiempo de procesamiento. Una vez verificado que el documento compila correctamente, se desactiva este modo para generar la versión final con todas las imágenes incluidas.

Para trabajar temporalmente sin cargar las imágenes se puede cambiar:

```latex
\usepackage{graphicx}
```

por:

```latex
\usepackage[draft]{graphicx}
```

De esta forma LaTeX mantiene el espacio correspondiente a cada imagen, pero evita procesar los archivos gráficos durante las compilaciones de prueba.

### Para la versión final

Antes de generar el PDF definitivo se debe regresar a:

```latex
\usepackage{graphicx}
```

y recompilar normalmente.

> **Importante:** el PDF entregado debe compilarse sin `draft`, de modo que todas las figuras y evidencias sean visibles.

---

# Resultados principales

Con cuatro executors, los resultados experimentales obtenidos fueron:

| Transformación | pandas (s) | PySpark (s) | Speedup |
|---|---:|---:|---:|
| T1 | 0.2119 | 0.3457 | 0.6131 |
| T2 | 0.1171 | 0.6875 | 0.1704 |
| T3 | 0.2889 | 0.8657 | 0.3337 |
| T4 | 2.2659 | 1.8100 | 1.2519 |
| T5 | 1.5252 | 1.3565 | 1.1244 |

PySpark superó a pandas en **T4 y T5**, mientras que en T1, T2 y T3 la sobrecarga de coordinación y `shuffle` fue superior a la ganancia obtenida mediante paralelismo.

Para T3 se obtuvo el siguiente comportamiento al variar el número de executors:

| Executors | Tiempo (s) | Speedup | Eficiencia |
|---:|---:|---:|---:|
| 1 | 1.3615 | 1.0000 | 100.00 % |
| 2 | 0.7085 | 1.9217 | 96.08 % |
| 4 | 0.8657 | 1.5727 | 39.32 % |

A partir del punto de cuatro executors se obtuvo una fracción no escalable observada aproximada de:

```text
f = 0.5145
```

y un límite teórico máximo de speedup de:

```text
Smax = 1.9437
```

---

## Verificación de equivalencia

Antes de comparar el rendimiento se verificó que pandas y PySpark produjeran resultados equivalentes.

| Transformación | pandas | PySpark | Resultado |
|---|---:|---:|---|
| T1 | 33 117 | 33 117 | Idéntico |
| T2 | 40 | 40 | Idéntico |
| T3 | 500 000 | 500 000 | Idéntico |
| T4 | 500 000 | 500 000 | Idéntico |
| T5 | 20 | 20 | Idéntico |

Además de las cardinalidades se comprobaron agregados de control y criterios de ordenación.

---

# Reproducibilidad

El proyecto fue diseñado para que el experimento pueda reconstruirse desde cero.

El flujo completo de reproducción es:

```text
1. Clonar repositorio
        ↓
2. Crear entorno virtual
        ↓
3. Instalar requirements.txt
        ↓
4. Generar dataset
        ↓
5. Verificar dataset
        ↓
6. Iniciar Spark Standalone
        ↓
7. Ejecutar protocolo experimental
        ↓
8. Generar gráficas
        ↓
9. Revisar notebook y evidencias
        ↓
10. Compilar informe LaTeX
```

Los tiempos crudos permiten reconstruir las medianas, speedups y métricas reportadas en el informe.

---

## Archivos principales

| Archivo | Función |
|---|---|
| `src/generar_dataset.py` | Generación y verificación reproducible del dataset |
| `src/carga_datos.py` | Carga de datos para pandas y PySpark |
| `src/transformaciones_pandas.py` | Implementación secuencial de T1–T5 |
| `src/transformaciones_spark.py` | Implementación distribuida de T1–T5 |
| `src/medicion.py` | Protocolo experimental y cálculo de métricas |
| `src/graficas.py` | Generación de figuras a partir de resultados oficiales |
| `notebooks/PE_U4_pipeline_spark.ipynb` | Notebook ejecutado del pipeline |
| `docs/PE_U4_Informe.tex` | Fuente LaTeX del informe |
| `docs/references_U4.bib` | Bibliografía del informe |
| `resultados/tiempos_crudos.csv` | Mediciones individuales |
| `resultados/tiempos_resumen.csv` | Medianas y speedups |
| `resultados/metricas_derivadas.csv` | Métricas derivadas del experimento |

---

## Licencia

Este repositorio se distribuye bajo la licencia **MIT**.

Consultar:

```text
LICENSE
```

para obtener los términos completos.

---

## Repositorio

**Equipo C — PE-U4**

https://github.com/ffarinangog2/pe-u4-spark-equipo-c

Universidad Técnica Estatal de Quevedo  
Carrera de Ingeniería de Software  
Aplicaciones Distribuidas — Unidad 4  
2026–2027 PPA
