-- ================================================================
-- TABLA: Stage de Gestiones - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.0.0
-- Descripción: Tabla staging para gestiones unificadas BOT + HUMANO
--              con homologación de respuestas y operadores
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones` (
  
  -- 🔑 LLAVES PRIMARIAS
  cod_luna INT64 NOT NULL
    OPTIONS(description="Código Luna del cliente gestionado"),
  fecha_gestion DATE NOT NULL
    OPTIONS(description="Fecha de la gestión realizada"),
  canal_origen STRING NOT NULL
    OPTIONS(description="Canal de origen: BOT, HUMANO"),
  secuencia_gestion INT64 NOT NULL
    OPTIONS(description="Secuencia de gestión del día para el cliente"),
  
  -- 👥 DIMENSIONES DE OPERADOR
  nombre_agente_original STRING
    OPTIONS(description="Nombre original del agente antes de homologación"),
  operador_final STRING NOT NULL
    OPTIONS(description="Operador final después de homologación"),
  
  -- 📞 DIMENSIONES DE GESTIÓN
  management_original STRING
    OPTIONS(description="Management original antes de homologación"),
  sub_management_original STRING
    OPTIONS(description="Sub-management original"),
  compromiso_original STRING
    OPTIONS(description="Compromiso original (solo BOT)"),
  
  -- 🎯 RESPUESTAS HOMOLOGADAS
  grupo_respuesta STRING NOT NULL
    OPTIONS(description="Grupo de respuesta homologado"),
  nivel_1 STRING NOT NULL
    OPTIONS(description="Nivel 1 de respuesta homologado"),
  nivel_2 STRING NOT NULL
    OPTIONS(description="Nivel 2 de respuesta homologado"),
  
  -- 💰 COMPROMISOS Y MONTOS
  es_compromiso BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si es un compromiso de pago/PDP"),
  monto_compromiso FLOAT64
    OPTIONS(description="Monto comprometido por el cliente"),
  fecha_compromiso DATE
    OPTIONS(description="Fecha comprometida para el pago"),
  
  -- 📊 FLAGS DE ANÁLISIS
  es_contacto_efectivo BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si fue contacto efectivo"),
  es_primera_gestion_dia BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si es la primera gestión del día para el cliente"),
  
  -- 🔗 REFERENCIAS A OTROS STAGES
  tiene_asignacion BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si el cliente tiene asignación"),
  tiene_deuda BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si el cliente tiene deuda"),
  es_gestion_medible BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si la gestión cuenta para métricas"),
  
  -- 📅 DIMENSIONES TEMPORALES CALCULADAS
  dia_semana STRING
    OPTIONS(description="Día de la semana de la gestión"),
  semana_mes INT64
    OPTIONS(description="Semana del mes (1-5)"),
  es_fin_semana BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si es sábado o domingo"),
  
  -- 🕒 METADATOS
  fecha_actualizacion TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de última actualización"),
  fecha_proceso DATE NOT NULL
    OPTIONS(description="Fecha del proceso que generó el registro"),
  fecha_carga TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de carga inicial del registro")
)

-- 🔍 CONFIGURACIÓN DE PARTICIONADO Y CLUSTERING
PARTITION BY DATE(fecha_gestion)
CLUSTER BY cod_luna, canal_origen, es_contacto_efectivo

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para gestiones unificadas BOT + HUMANO. Incluye homologación de respuestas, operadores y flags de análisis para reportería de efectividad.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (cod_luna, fecha_gestion, canal_origen, secuencia_gestion)
-- FOREIGN KEY: cod_luna -> asignacion.cod_luna (opcional)
-- CHECK: canal_origen IN ('BOT', 'HUMANO')
-- CHECK: monto_compromiso >= 0
-- CHECK: secuencia_gestion >= 1

-- ================================================================
-- COMENTARIOS DE NEGOCIO
-- ================================================================

-- Esta tabla unifica gestiones de múltiples canales:
--
-- CANALES SOPORTADOS:
-- - BOT: Gestiones automáticas del voicebot
-- - HUMANO: Gestiones manuales de agentes
--
-- PROCESO DE HOMOLOGACIÓN:
-- 1. Se extraen gestiones de ambas fuentes
-- 2. Se aplican tablas de homologación por canal
-- 3. Se unifican respuestas en grupos estándar
-- 4. Se calculan flags de efectividad
--
-- LÓGICA DE MEDIBILIDAD:
-- Una gestión es medible si:
-- - El cliente tiene asignación Y/O deuda
-- - La gestión es de tipo efectivo o compromiso
-- - Cumple criterios de calidad definidos
--
-- SECUENCIA DE GESTIÓN:
-- Se numera secuencialmente las gestiones del mismo cliente/día
-- La primera gestión tiene flag especial para análisis
--
-- CAMPOS CALCULADOS:
-- - es_contacto_efectivo: Basado en patterns del management
-- - es_compromiso: Basado en homologación de PDP/compromisos
-- - operador_final: Después de homologación de usuarios
-- - grupo_respuesta: Después de homologación de respuestas
