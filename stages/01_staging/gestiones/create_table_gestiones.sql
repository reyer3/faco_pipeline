-- ================================================================
-- TABLA: Stage de Gestiones - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.1.0
-- Descripción: Tabla staging para gestiones unificadas BOT + HUMANO
--              con contexto de cartera, archivo y cíclicas de vencimiento
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones` (
  
  -- 🔑 LLAVES PRIMARIAS
  cod_luna INT64 NOT NULL
    OPTIONS(description="Código único del cliente en sistema Luna"),
  fecha_gestion DATE NOT NULL
    OPTIONS(description="Fecha de la gestión"),
  canal_origen STRING NOT NULL
    OPTIONS(description="Canal de origen: BOT, HUMANO"),
  secuencia_gestion INT64 NOT NULL
    OPTIONS(description="Secuencia de gestión dentro del día por canal"),
  
  -- 👥 DIMENSIONES DE OPERADOR
  nombre_agente_original STRING
    OPTIONS(description="Nombre del agente original del sistema"),
  operador_final STRING
    OPTIONS(description="Operador final homologado"),
  
  -- 📞 DIMENSIONES DE GESTIÓN ORIGINALES
  management_original STRING
    OPTIONS(description="Management original del sistema"),
  sub_management_original STRING
    OPTIONS(description="Sub-management original del sistema"),
  compromiso_original STRING
    OPTIONS(description="Compromiso original del sistema"),
  
  -- 🎯 RESPUESTAS HOMOLOGADAS
  grupo_respuesta STRING NOT NULL
    OPTIONS(description="Grupo de respuesta homologado"),
  nivel_1 STRING NOT NULL
    OPTIONS(description="Nivel 1 de respuesta homologado"),
  nivel_2 STRING NOT NULL
    OPTIONS(description="Nivel 2 de respuesta homologado"),
  
  -- 💰 COMPROMISOS Y MONTOS
  es_compromiso BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si la gestión generó compromiso"),
  monto_compromiso FLOAT64
    OPTIONS(description="Monto del compromiso de pago"),
  fecha_compromiso DATE
    OPTIONS(description="Fecha comprometida para el pago"),
  
  -- 🔥 NUEVO: CONTEXTO DE CARTERA Y ARCHIVO
  archivo_cartera STRING NOT NULL
    OPTIONS(description="Nombre del archivo de cartera de donde viene el cliente"),
  tipo_cartera STRING NOT NULL
    OPTIONS(description="Tipo de cartera: TEMPRANA, CUOTA_FRACCIONAMIENTO, ALTAS_NUEVAS, OTRAS"),
  
  -- 🔥 NUEVO: INFORMACIÓN DE VENCIMIENTOS Y CÍCLICAS
  fecha_vencimiento_cliente DATE
    OPTIONS(description="Fecha de vencimiento del cliente desde asignación/deudas"),
  categoria_vencimiento STRING NOT NULL
    OPTIONS(description="Categorización del vencimiento: VENCIDO, POR_VENCER_30D, etc."),
  ciclica_vencimiento STRING NOT NULL
    OPTIONS(description="Cíclica derivada del día de vencimiento: CICLICA_01, CICLICA_15, etc."),
  
  -- 🔥 NUEVO: SEGMENTO Y ZONA DESDE CARTERA
  segmento_gestion STRING NOT NULL
    OPTIONS(description="Segmento de gestión desde asignación"),
  zona_geografica STRING NOT NULL
    OPTIONS(description="Zona geográfica desde asignación"),
  
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
    OPTIONS(description="TRUE si la gestión es medible (tiene asignación O deuda)"),
  tipo_medibilidad STRING NOT NULL
    OPTIONS(description="Tipo de medibilidad: ASIGNACION_Y_DEUDA, SOLO_ASIGNACION, SOLO_DEUDA, NO_MEDIBLE"),
  
  -- 📅 DIMENSIONES TEMPORALES CALCULADAS
  dia_semana STRING
    OPTIONS(description="Día de la semana de la gestión"),
  semana_mes INT64
    OPTIONS(description="Semana del mes"),
  es_fin_semana BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si la gestión fue en fin de semana"),
  
  -- 🕒 METADATOS
  timestamp_gestion TIMESTAMP
    OPTIONS(description="Timestamp original de la gestión"),
  fecha_actualizacion TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de última actualización"),
  fecha_proceso DATE NOT NULL
    OPTIONS(description="Fecha del proceso que generó el registro"),
  fecha_carga TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de carga inicial del registro")
)

-- 🔍 CONFIGURACIÓN DE PARTICIONADO Y CLUSTERING
PARTITION BY DATE(fecha_gestion)
CLUSTER BY cod_luna, canal_origen, archivo_cartera, ciclica_vencimiento

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para gestiones unificadas BOT + HUMANO. Incluye contexto completo de cartera, archivo y cíclicas de vencimiento para análisis de efectividad por segmento.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (cod_luna, fecha_gestion, canal_origen, secuencia_gestion)
-- FOREIGN KEY: cod_luna -> asignacion.cod_luna (opcional)
-- CHECK: canal_origen IN ('BOT', 'HUMANO')
-- CHECK: tipo_cartera IN ('TEMPRANA', 'CUOTA_FRACCIONAMIENTO', 'ALTAS_NUEVAS', 'OTRAS')
-- CHECK: tipo_medibilidad IN ('ASIGNACION_Y_DEUDA', 'SOLO_ASIGNACION', 'SOLO_DEUDA', 'NO_MEDIBLE')
-- CHECK: monto_compromiso >= 0

-- ================================================================
-- COMENTARIOS DE NEGOCIO IMPORTANTES
-- ================================================================

-- Esta tabla unifica gestiones de ambos canales (BOT + HUMANO) y los enriquece con:
--
-- 1. CONTEXTO DE CARTERA:
--    - archivo_cartera: Nombre del archivo de donde viene el cliente
--    - tipo_cartera: Tipificación automática de la cartera
--    - segmento_gestion: Segmento asignado al cliente
--
-- 2. INFORMACIÓN DE CÍCLICAS:
--    - fecha_vencimiento_cliente: Vencimiento desde asignación/deudas
--    - categoria_vencimiento: Categorización del estado de vencimiento
--    - ciclica_vencimiento: Cíclica derivada del día de vencimiento (ej: CICLICA_15)
--    
--    La cíclica es CRÍTICA porque nos dice el ciclo de facturación del cliente,
--    lo cual determina patrones de pago y estrategias de gestión específicas.
--
-- 3. MEDIBILIDAD DE GESTIONES:
--    - Una gestión es medible si el cliente tiene asignación O deuda
--    - tipo_medibilidad indica específicamente qué tipo de relación tiene
--    - Permite análisis de efectividad por tipo de cliente
--
-- 4. HOMOLOGACIÓN DE RESPUESTAS:
--    - Unifica las respuestas de BOT y HUMANO bajo una taxonomía común
--    - Permite análisis comparativo entre canales
--    - Facilita reportería consolidada
--
-- 5. ANÁLISIS TEMPORAL:
--    - Considera patrones por día de semana, fin de semana
--    - Permite análisis de efectividad por horarios y días
--
-- REGLAS DE MERGE:
-- - Se actualizan campos calculados y de contexto de cartera
-- - Se preserva el histórico de gestiones
-- - Se mantiene la secuencia de gestiones por día y canal
