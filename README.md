# PE-U4 — Procesamiento Distribuido con Apache Spark: comprobación experimental de la Ley de Amdahl

**GA-SUM-05 / PE-U4 · Aplicaciones Distribuidas (ISR-701) · Unidad 4 — Cómputo Paralelo y Distribuido**
Universidad Técnica Estatal de Quevedo · Facultad de Ciencias de la Computación · Ingeniería de Software
Período académico 2026–2027 PPA · Docente: Gleiston C. Guerrero-Ulloa, M.Sc.

## Descripción

Este repositorio contiene la práctica experimental de la Unidad 4: la implementación de un pipeline de cinco transformaciones (T1–T5) sobre un conjunto de datos de 500 000 solicitudes de reserva de laboratorios, ejecutado de forma secuencial con pandas y de forma distribuida con PySpark 3.5.0 sobre un clúster Spark Standalone, con medición rigurosa de tiempos (cinco repeticiones por transformación, mediana, calentamiento descartado) para calcular el speedup experimental, la fracción no escalable observada y el límite teórico predicho por la Ley de Amdahl. El análisis completo se documenta en el informe LaTeX de `docs/`.

## PFC de referencia

**FUVV — Laboratorios Informáticos**: sistema de reserva y monitoreo distribuido de laboratorios. La justificación técnica del dominio se encuentra en la portada del informe (`docs/PE_U4_Informe.pdf`).

## Integrantes

| Integrante          | PFC de origen | Rol asumido                                                |
| ------------------- | ------------- | ---------------------------------------------------------- |
| [NOMBRE COMPLETO A] | [CÓDIGO PFC]  | Protocolo de medición y análisis experimental              |
| [NOMBRE COMPLETO B] | [CÓDIGO PFC]  | Implementación secuencial (pandas) y graficación           |
| [NOMBRE COMPLETO C] | [CÓDIGO PFC]  | Implementación distribuida (PySpark) y generación de datos |
