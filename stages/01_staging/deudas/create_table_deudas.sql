-- ================================================================
-- TABLA: Stage de Deudas - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-20
-- Versión: 1.3.0 - CORREGIDA particionado
-- Descripción: Tabla staging para datos de deudas diarias con lógica
--              de medibilidad basada en coincidencia con FECHA_TRANDEUDA
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
  estado_deuda STRING
    OPTIONS(description="Estado actual de la deuda - DEFAULT: ACTIVA"),
  
  -- 📅 DIMENSIONES TEMPORALES
  fecha_deuda_construida DATE
    OPTIONS(description="Fecha construida desde nombre archivo TRAN_DEUDA_DDMM"),
  fecha_asignacion DATE
    OPTIONS(description="Fecha de asignación de cartera (desde calendario)"),
  fecha_cierre DATE
    OPTIONS(description="Fecha de cierre de gestión"),
  fecha_trandeuda DATE
    OPTIONS(description="FECHA_TRANDEUDA desde calendario - clave para medibilidad"),
  dias_gestion INT64
    OPTIONS(description="Días disponibles para gestión"),
  
  -- 🎯 LÓGICA DE NEGOCIO ESPECÍFICA
  es_dia_apertura BOOLEAN
    OPTIONS(description="TRUE si es día de apertura de cartera"),
  es_gestionable BOOLEAN
    OPTIONS(description="TRUE si el cliente es gestionable (tiene asignación) - DEFAULT: FALSE"),
  es_medible BOOLEAN
    OPTIONS(description="TRUE si fecha_deuda coincide con FECHA_TRANDEUDA del calendario - DEFAULT: FALSE"),
  tipo_activacion STRING
    OPTIONS(description="APERTURA, SUBSIGUIENTE, REACTIVACION"),
  
  -- 🔗 REFERENCIAS A ASIGNACIÓN
  cod_luna STRING
    OPTIONS(description="Código Luna del cliente (si está asignado)"),
  tiene_asignacion BOOLEAN
    OPTIONS(description="TRUE si el cliente tiene asignación - DEFAULT: FALSE"),
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
CLUSTER BY cod_cuenta, es_medible, fecha_trandeuda

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para datos de deudas diarias. La medibilidad se determina por coincidencia entre fecha extraída del archivo TRAN_DEUDA y FECHA_TRANDEUDA del calendario.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (cod_cuenta, nro_documento, archivo, fecha_deuda)
-- FOREIGN KEY: cod_cuenta -> asignacion.cod_cuenta (opcional)
-- BUSINESS RULE: es_medible = TRUE solo si fecha_deuda = fecha_trandeuda
-- CHECK: monto_exigible >= 0
-- CHECK: tipo_activacion IN ('APERTURA', 'SUBSIGUIENTE', 'REACTIVACION')
-- CHECK: estado_deuda IN ('ACTIVA', 'INACTIVA', 'CERRADA')

-- ================================================================
-- VALORES DEFAULT MANEJADOS EN STORED PROCEDURES
-- ================================================================

-- NOTA: Los valores DEFAULT se manejan en los stored procedures:
-- - estado_deuda: 'ACTIVA' si es NULL
-- - es_gestionable: FALSE si es NULL  
-- - es_medible: FALSE si es NULL
-- - tiene_asignacion: FALSE si es NULL
-- - es_dia_apertura: Se calcula en base a lógica de negocio

-- ================================================================
-- COMENTARIOS DE NEGOCIO CORREGIDOS
-- ================================================================

-- Esta tabla maneja la lógica específica de medibilidad de deudas:
--
-- REGLA PRINCIPAL DE MEDIBILIDAD:
-- Un cliente es MEDIBLE solo cuando:
-- 1. Tiene asignación (es_gestionable = TRUE)
-- 2. La fecha extraída del archivo TRAN_DEUDA coincide con FECHA_TRANDEUDA del calendario
-- 3. Esta coincidencia determina que el cliente está "con deuda pendiente en dicha relación"
--
-- LÓGICA DE NEGOCIO:
-- - es_gestionable: TRUE solo si tiene asignación
-- - es_medible: TRUE solo si (es_gestionable AND fecha_deuda = fecha_trandeuda)
-- - monto_medible: monto_exigible solo si es_medible, sino 0
--
-- DÍAS DE APERTURA vs SUBSIGUIENTES:
-- - Día apertura: Cuando fecha_proceso = FECHA_ASIGNACION en calendario
-- - Días subsiguientes: Pueden activarse nuevas deudas
-- - Medibilidad: No depende del tipo de día, sino de la coincidencia con FECHA_TRANDEUDA
--
-- REGLAS DE MERGE:
-- - Se actualizan montos y estados de deudas existentes
-- - Se insertan nuevas activaciones manteniendo historial
-- - Se preserva la lógica de medibilidad según coincidencia con calendario
