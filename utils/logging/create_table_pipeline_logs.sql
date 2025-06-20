-- ================================================================
-- TABLA: Pipeline Logs - Sistema de Trazabilidad
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-20
-- Versión: 1.0.0
-- Descripción: Tabla para logging de ejecuciones del pipeline FACO
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.pipeline_logs` (
  timestamp TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de la ejecución"),
  stage_name STRING NOT NULL
    OPTIONS(description="Nombre del stage ejecutado"),
  fecha_proceso DATE NOT NULL
    OPTIONS(description="Fecha procesada"),
  status STRING NOT NULL
    OPTIONS(description="Status de ejecución: SUCCESS, ERROR, WARNING, INICIADO, COMPLETADO"),
  records_processed INT64
    OPTIONS(description="Número de registros procesados"),
  duration_seconds FLOAT64
    OPTIONS(description="Duración en segundos"),
  message STRING
    OPTIONS(description="Mensaje descriptivo"),
  execution_parameters JSON
    OPTIONS(description="Parámetros de ejecución en formato JSON")
)
PARTITION BY DATE(timestamp)
CLUSTER BY stage_name, status
OPTIONS(
  description="Logs de ejecución del pipeline FACO",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("tipo", "logs")]
);
