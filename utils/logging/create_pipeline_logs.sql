-- ================================================================
-- TABLA DE LOGGING: Pipeline de Cobranzas FACO
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Descripción: Tabla centralizada para logging de todos los procesos
--              del pipeline de cobranzas
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.pipeline_logs` (
  
  -- 🔑 IDENTIFICADORES
  log_id STRING NOT NULL DEFAULT GENERATE_UUID()
    OPTIONS(description="Identificador único del log"),
  proceso STRING NOT NULL
    OPTIONS(description="Nombre del proceso (ej: faco_pipeline)"),
  etapa STRING NOT NULL
    OPTIONS(description="Etapa específica del proceso (ej: stage_asignacion)"),
  
  -- 🕒 TEMPORALES
  fecha_inicio TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de inicio del proceso"),
  fecha_fin TIMESTAMP
    OPTIONS(description="Timestamp de finalización del proceso"),
  duracion_segundos INT64 GENERATED ALWAYS AS (
    IF(fecha_fin IS NOT NULL, 
       TIMESTAMP_DIFF(fecha_fin, fecha_inicio, SECOND), 
       NULL)
  ) STORED
    OPTIONS(description="Duración calculada en segundos"),
  
  -- 📊 MÉTRICAS
  registros_procesados INT64
    OPTIONS(description="Total de registros procesados"),
  registros_nuevos INT64
    OPTIONS(description="Registros insertados"),
  registros_actualizados INT64
    OPTIONS(description="Registros actualizados"),
  registros_eliminados INT64
    OPTIONS(description="Registros eliminados"),
  
  -- 🎛️ CONFIGURACIÓN
  parametros JSON
    OPTIONS(description="Parámetros de entrada del proceso"),
  version_codigo STRING
    OPTIONS(description="Versión del código ejecutado"),
  
  -- 📊 ESTADO Y RESULTADOS
  estado STRING NOT NULL
    OPTIONS(description="Estado del proceso: INICIADO, COMPLETADO, ERROR"),
  codigo_error STRING
    OPTIONS(description="Código de error si aplica"),
  mensaje_error STRING
    OPTIONS(description="Mensaje de error detallado si aplica"),
  observaciones STRING
    OPTIONS(description="Observaciones adicionales del proceso"),
  
  -- 🔍 METADATOS
  usuario_ejecucion STRING DEFAULT SESSION_USER()
    OPTIONS(description="Usuario que ejecutó el proceso"),
  entorno STRING DEFAULT 'PRODUCTION'
    OPTIONS(description="Entorno de ejecución"),
  fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    OPTIONS(description="Timestamp de creación del registro")
)

-- 🔍 CONFIGURACIÓN DE PARTICIONADO
PARTITION BY DATE(fecha_inicio)
CLUSTER BY proceso, etapa, estado

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla centralizada de logging para el pipeline de cobranzas FACO. Registra todas las ejecuciones, métricas y errores de los procesos.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("tipo", "logging")]
);
