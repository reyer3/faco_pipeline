-- ================================================================
-- TABLA: Stage de Deudas - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.0.0
-- Descripción: Tabla staging para datos de deudas diarias con lógica
--              de día de apertura vs días subsiguientes
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas` (
  
  -- 🔑 LLAVES PRIMARIAS
  cod_cuenta STRING NOT NULL
    OPTIONS(description="Código de cuenta del cliente"),
  nro_documento STRING NOT NULL
    OPTIONS(description="Número de documento de la deuda"),
  archivo STRING NOT NULL
    OPTIONS(description="Archivo TRAN_DEUDA de origen"),
  fecha_deuda DATE NOT NULL
    OPTIONS(description="Fecha extraída del nombre del archivo"),
  
  -- 💰 DATOS DE DEUDA
  monto_exigible FLOAT64
    OPTIONS(description="Monto exigible de la deuda"),
  estado_deuda STRING NOT NULL DEFAULT 'ACTIVA'
    OPTIONS(description="Estado actual de la deuda"),
  
  -- 📅 DIMENSIONES TEMPORALES
  fecha_deuda_construida DATE
    OPTIONS(description="Fecha construida desde nombre archivo TRAN_DEUDA_DDMM"),
  fecha_asignacion DATE
    OPTIONS(description="Fecha de asignación de cartera (desde calendario)"),
  fecha_cierre DATE
    OPTIONS(description="Fecha de cierre de gestión"),
  dias_gestion INT64
    OPTIONS(description="Días disponibles para gestión"),
  
  -- 🎯 LÓGICA DE NEGOCIO ESPECÍFICA
  es_dia_apertura BOOLEAN NOT NULL
    OPTIONS(description="TRUE si es día de apertura de cartera"),
  es_gestionable BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si el cliente es gestionable"),
  es_medible BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si cuenta para métricas de competencia"),
  tipo_activacion STRING NOT NULL
    OPTIONS(description="APERTURA, SUBSIGUIENTE, REACTIVACION"),
  
  -- 🔗 REFERENCIAS A ASIGNACIÓN
  cod_luna STRING
    OPTIONS(description="Código Luna del cliente (si está asignado)"),
  tiene_asignacion BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si el cliente tiene asignación"),
  segmento_gestion STRING
    OPTIONS(description="Segmento de gestión (desde asignación)"),
  tipo_cartera STRING
    OPTIONS(description="Tipo de cartera (desde asignación)"),
  
  -- 📊 MÉTRICAS CALCULADAS
  monto_gestionable FLOAT64
    OPTIONS(description="Monto que cuenta para gestión (si es_gestionable=TRUE)"),
  monto_medible FLOAT64
    OPTIONS(description="Monto que cuenta para competencia (si es_medible=TRUE)"),
  
  -- 🔢 FLAGS DE ANÁLISIS
  secuencia_activacion INT64
    OPTIONS(description="Orden de activación para el mismo cliente"),
  
  -- 🕒 METADATOS
  creado_el TIMESTAMP
    OPTIONS(description="Timestamp original del registro en batch"),
  fecha_actualizacion TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de última actualización"),
  fecha_proceso DATE NOT NULL
    OPTIONS(description="Fecha del proceso que generó el registro"),
  fecha_carga TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de carga inicial del registro")
)

-- 🔍 CONFIGURACIÓN DE PARTICIONADO Y CLUSTERING
PARTITION BY DATE(fecha_deuda)
CLUSTER BY cod_cuenta, tipo_activacion, es_medible

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para datos de deudas diarias. Maneja lógica de día de apertura vs días subsiguientes, incluyendo filtros de gestionabilidad y medibilidad para competencia.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (cod_cuenta, nro_documento, archivo, fecha_deuda)
-- FOREIGN KEY: cod_cuenta -> asignacion.cod_cuenta (opcional)
-- CHECK: monto_exigible >= 0
-- CHECK: tipo_activacion IN ('APERTURA', 'SUBSIGUIENTE', 'REACTIVACION')
-- CHECK: estado_deuda IN ('ACTIVA', 'INACTIVA', 'CERRADA')

-- ================================================================
-- COMENTARIOS DE NEGOCIO
-- ================================================================

-- Esta tabla maneja la lógica compleja de deudas diarias:
--
-- DÍA DE APERTURA:
-- - Se filtran solo clientes que pasan a "gestionables y medibles"
-- - es_dia_apertura = TRUE
-- - es_gestionable = TRUE solo para clientes asignados
-- - es_medible = TRUE solo para clientes gestionables
-- - tipo_activacion = 'APERTURA'
--
-- DÍAS SUBSIGUIENTES:
-- - Pueden sumarse/activarse deudas de otros clientes
-- - es_dia_apertura = FALSE
-- - es_gestionable = depende de si tiene asignación
-- - es_medible = FALSE (no cuentan para competencia)
-- - tipo_activacion = 'SUBSIGUIENTE' o 'REACTIVACION'
--
-- REGLAS DE MERGE:
-- - Se actualizan montos y estados de deudas existentes
-- - Se insertan nuevas activaciones manteniendo historial
-- - Se preserva la lógica de medibilidad según día de activación
