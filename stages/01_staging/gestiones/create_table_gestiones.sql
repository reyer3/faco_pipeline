-- ================================================================
-- TABLA: Stage de Gestiones - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-20
-- Versión: 1.5.0 - CORREGIDA particionado
-- Descripción: Tabla staging para gestiones unificadas BOT + HUMANO
--              con marcadores de mejor gestión por canal separado
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones` (
  
  -- 🔑 LLAVES PRIMARIAS (COMPUESTA PARA MÚLTIPLES CARTERAS)
  cod_luna INT64 NOT NULL
    OPTIONS(description="Código único del cliente en sistema Luna"),
  archivo_cartera STRING NOT NULL
    OPTIONS(description="Nombre del archivo de cartera de donde viene el cliente"),
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
  es_compromiso BOOLEAN
    OPTIONS(description="TRUE si la gestión generó compromiso - DEFAULT: FALSE"),
  monto_compromiso FLOAT64
    OPTIONS(description="Monto del compromiso de pago"),
  fecha_compromiso DATE
    OPTIONS(description="Fecha comprometida para el pago"),
  
  -- 🔥 CONTEXTO DE CARTERA Y ARCHIVO
  tipo_cartera STRING NOT NULL
    OPTIONS(description="Tipo de cartera: TEMPRANA, CUOTA_FRACCIONAMIENTO, ALTAS_NUEVAS, OTRAS"),
  
  -- 🔥 INFORMACIÓN DE VENCIMIENTOS Y CÍCLICAS
  fecha_vencimiento_cliente DATE
    OPTIONS(description="Fecha de vencimiento del cliente desde asignación/deudas"),
  categoria_vencimiento STRING NOT NULL
    OPTIONS(description="Categorización del vencimiento: VENCIDO, POR_VENCER_30D, etc."),
  ciclica_vencimiento STRING NOT NULL
    OPTIONS(description="Cíclica derivada del día de vencimiento: CICLICA_01, CICLICA_15, etc."),
  
  -- 🔥 SEGMENTO Y ZONA DESDE CARTERA
  segmento_gestion STRING NOT NULL
    OPTIONS(description="Segmento de gestión desde asignación"),
  zona_geografica STRING NOT NULL
    OPTIONS(description="Zona geográfica desde asignación"),
  
  -- 📊 FLAGS DE ANÁLISIS
  es_contacto_efectivo BOOLEAN
    OPTIONS(description="TRUE si fue contacto efectivo - DEFAULT: FALSE"),
  es_primera_gestion_dia BOOLEAN
    OPTIONS(description="TRUE si es la primera gestión del día para el cliente - DEFAULT: FALSE"),
  
  -- 🔗 REFERENCIAS A OTROS STAGES
  tiene_asignacion BOOLEAN
    OPTIONS(description="TRUE si el cliente tiene asignación - DEFAULT: FALSE"),
  tiene_deuda BOOLEAN
    OPTIONS(description="TRUE si el cliente tiene deuda - DEFAULT: FALSE"),
  es_gestion_medible BOOLEAN
    OPTIONS(description="TRUE si la gestión es medible (tiene asignación O deuda) - DEFAULT: FALSE"),
  tipo_medibilidad STRING NOT NULL
    OPTIONS(description="Tipo de medibilidad: ASIGNACION_Y_DEUDA, SOLO_ASIGNACION, SOLO_DEUDA, NO_MEDIBLE"),
  
  -- 🏆 SISTEMA DE PESO POR CANAL (NUEVO ENFOQUE)
  weight_original INT64 NOT NULL
    OPTIONS(description="Weight original de la tabla fuente (BOT o HUMANO)"),
  es_mejor_gestion_bot_dia BOOLEAN
    OPTIONS(description="TRUE si es la gestión BOT de mayor weight del día para cliente+cartera - DEFAULT: FALSE"),
  es_mejor_gestion_humano_dia BOOLEAN
    OPTIONS(description="TRUE si es la gestión HUMANO de mayor weight del día para cliente+cartera - DEFAULT: FALSE"),
  
  -- 📈 MÉTRICAS MENSUALES POR CANAL
  gestiones_mes_canal INT64
    OPTIONS(description="Total gestiones del canal en el mes para cliente+cartera - DEFAULT: 0"),
  weight_acumulado_mes_canal FLOAT64
    OPTIONS(description="Suma de weights de mejores gestiones del canal en el mes - DEFAULT: 0.0"),
  compromisos_mes_canal INT64
    OPTIONS(description="Total compromisos del canal en el mes para cliente+cartera - DEFAULT: 0"),
  contactos_efectivos_mes_canal INT64
    OPTIONS(description="Total contactos efectivos del canal en el mes para cliente+cartera - DEFAULT: 0"),
  monto_compromisos_mes_canal FLOAT64
    OPTIONS(description="Suma de montos comprometidos del canal en el mes - DEFAULT: 0.0"),
  
  -- 🎯 INDICADORES DE CALIDAD POR CANAL
  tasa_efectividad_canal_mes FLOAT64
    OPTIONS(description="% efectividad del canal en el mes para el cliente"),
  ranking_cliente_canal_mes INT64
    OPTIONS(description="Ranking del cliente en el canal por weight acumulado"),
  dias_gestionado_canal_mes INT64
    OPTIONS(description="Días que el canal gestionó al cliente en el mes - DEFAULT: 0"),
  
  -- 📅 DIMENSIONES TEMPORALES CALCULADAS
  dia_semana STRING
    OPTIONS(description="Día de la semana de la gestión"),
  semana_mes INT64
    OPTIONS(description="Semana del mes"),
  es_fin_semana BOOLEAN
    OPTIONS(description="TRUE si la gestión fue en fin de semana - DEFAULT: FALSE"),
  
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
PARTITION BY fecha_gestion
CLUSTER BY cod_luna, archivo_cartera, canal_origen, weight_original

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para gestiones unificadas con marcadores de mejor gestión por canal separado. El weight original se mantiene para posterior competencia en capa analítica.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (cod_luna, archivo_cartera, fecha_gestion, canal_origen, secuencia_gestion)
-- FOREIGN KEY: cod_luna -> asignacion.cod_luna (opcional)
-- CHECK: canal_origen IN ('BOT', 'HUMANO')
-- CHECK: es_mejor_gestion_bot_dia = TRUE solo si canal_origen = 'BOT'
-- CHECK: es_mejor_gestion_humano_dia = TRUE solo si canal_origen = 'HUMANO'
-- INDEX: (cod_luna, archivo_cartera, fecha_gestion, canal_origen) para cálculos de mejor gestión
-- INDEX: (cod_luna, archivo_cartera, canal_origen, EXTRACT(YEAR_MONTH FROM fecha_gestion)) para métricas mensuales

-- ================================================================
-- VALORES DEFAULT MANEJADOS EN STORED PROCEDURES
-- ================================================================

-- NOTA: Los valores DEFAULT se manejan en los stored procedures:
-- - es_compromiso: FALSE si es NULL
-- - es_contacto_efectivo: FALSE si es NULL  
-- - es_primera_gestion_dia: FALSE si es NULL
-- - tiene_asignacion: FALSE si es NULL
-- - tiene_deuda: FALSE si es NULL
-- - es_gestion_medible: FALSE si es NULL
-- - es_mejor_gestion_bot_dia: FALSE si es NULL
-- - es_mejor_gestion_humano_dia: FALSE si es NULL
-- - gestiones_mes_canal: 0 si es NULL
-- - weight_acumulado_mes_canal: 0.0 si es NULL
-- - compromisos_mes_canal: 0 si es NULL
-- - contactos_efectivos_mes_canal: 0 si es NULL
-- - monto_compromisos_mes_canal: 0.0 si es NULL
-- - dias_gestionado_canal_mes: 0 si es NULL
-- - es_fin_semana: FALSE si es NULL

-- ================================================================
-- LÓGICA DE NEGOCIO - SISTEMA DE PESO POR CANAL
-- ================================================================

-- PESO ORIGINAL:
-- - Se mantiene el weight original de cada fuente (BOT: -960 a 31, HUMANO: -2 a 122)
-- - Valores más altos = mejor gestión
-- - Valores negativos = gestiones del discador (típicamente las peores)
--
-- MEJOR GESTIÓN POR CANAL:
-- - Para cada (cod_luna, archivo_cartera, fecha_gestion, canal_origen)
-- - Se marca es_mejor_gestion_bot_dia = TRUE para la gestión BOT de mayor weight
-- - Se marca es_mejor_gestion_humano_dia = TRUE para la gestión HUMANO de mayor weight
-- - En caso de empate: prevalece la de mayor monto_compromiso
-- - En caso de empate: prevalece la primera por timestamp_gestion
--
-- CÁLCULOS MENSUALES POR CANAL:
-- - weight_acumulado_mes_canal: suma de weight_original donde es_mejor_gestion_X_dia = TRUE
-- - gestiones_mes_canal: count(*) del canal para el mes
-- - compromisos_mes_canal: count donde es_compromiso = TRUE
-- - contactos_efectivos_mes_canal: count donde es_contacto_efectivo = TRUE
-- - tasa_efectividad_canal_mes: (contactos + compromisos) / total_gestiones_canal
--
-- RANKING POR CANAL:
-- - ranking_cliente_canal_mes: ROW_NUMBER() OVER (PARTITION BY canal ORDER BY weight_acumulado_mes_canal DESC)
--
-- SEPARACIÓN CANAL vs COMPETENCIA:
-- - Este stage mantiene BOT y HUMANO separados
-- - La competencia entre canales se resuelve en 02_analytics/gestiones_agregadas
-- - Allí se normalizarán escalas y se determinará mejor gestión global
--
-- CONSIDERACIONES LLAVE COMPUESTA:
-- - Un cliente puede estar en múltiples carteras (archivo_cartera) en el mismo mes
-- - Cada combinación (cod_luna, archivo_cartera, canal_origen) se trata independientemente
-- - Los cálculos mensuales son por combinación cliente+cartera+canal
-- - Esto permite analizar efectividad por canal y por tipo de cartera por separado
