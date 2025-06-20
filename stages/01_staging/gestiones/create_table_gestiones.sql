-- ================================================================
-- TABLA: Stage de Gestiones - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.2.0
-- Descripción: Tabla staging para gestiones unificadas BOT + HUMANO
--              con cálculo de peso por gestión y ranking mensual por cliente
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
  es_compromiso BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si la gestión generó compromiso"),
  monto_compromiso FLOAT64
    OPTIONS(description="Monto del compromiso de pago"),
  fecha_compromiso DATE
    OPTIONS(description="Fecha comprometida para el pago"),
  
  -- 🔥 NUEVO: CONTEXTO DE CARTERA Y ARCHIVO
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
  
  -- 🏆 NUEVO: SISTEMA DE PESO Y RANKING POR CLIENTE
  peso_gestion FLOAT64 NOT NULL DEFAULT 0.0
    OPTIONS(description="Peso calculado de la gestión: Compromiso=3, Contacto Efectivo=2, Gestión=1"),
  es_mejor_gestion_dia BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si es la gestión de mayor peso del día para el cliente+cartera"),
  peso_acumulado_mes FLOAT64 NOT NULL DEFAULT 0.0
    OPTIONS(description="Suma del peso de las mejores gestiones del mes para cliente+cartera"),
  ranking_cliente_mes INT64 DEFAULT NULL
    OPTIONS(description="Posición del cliente en ranking mensual por peso acumulado"),
  
  -- 📈 NUEVO: MÉTRICAS DE PERFORMANCE CLIENTE
  gestiones_mes_cliente INT64 NOT NULL DEFAULT 0
    OPTIONS(description="Total de gestiones en el mes para cliente+cartera"),
  compromisos_mes_cliente INT64 NOT NULL DEFAULT 0
    OPTIONS(description="Total compromisos en el mes para cliente+cartera"),
  contactos_efectivos_mes_cliente INT64 NOT NULL DEFAULT 0
    OPTIONS(description="Total contactos efectivos en el mes para cliente+cartera"),
  monto_compromisos_mes_cliente FLOAT64 NOT NULL DEFAULT 0.0
    OPTIONS(description="Suma de montos comprometidos en el mes para cliente+cartera"),
  
  -- 🎯 NUEVO: INDICADORES DE CALIDAD
  tasa_efectividad_cliente_mes FLOAT64 DEFAULT NULL
    OPTIONS(description="% de efectividad del cliente en el mes (contactos+compromisos/total)"),
  es_cliente_top_mes BOOLEAN NOT NULL DEFAULT FALSE
    OPTIONS(description="TRUE si está en el top 10% de clientes del mes por peso"),
  dias_gestionado_mes INT64 NOT NULL DEFAULT 0
    OPTIONS(description="Cantidad de días que fue gestionado en el mes"),
  
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
CLUSTER BY cod_luna, archivo_cartera, es_mejor_gestion_dia, peso_gestion

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para gestiones unificadas con sistema de peso y ranking por cliente. Calcula la mejor gestión del día y mantiene acumulados mensuales considerando que un cliente puede estar en múltiples carteras.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (cod_luna, archivo_cartera, fecha_gestion, canal_origen, secuencia_gestion)
-- FOREIGN KEY: cod_luna -> asignacion.cod_luna (opcional)
-- CHECK: canal_origen IN ('BOT', 'HUMANO')
-- CHECK: peso_gestion >= 0
-- CHECK: peso_acumulado_mes >= 0
-- CHECK: tasa_efectividad_cliente_mes BETWEEN 0 AND 1
-- INDEX: (cod_luna, archivo_cartera, fecha_gestion) para cálculos de mejor gestión día
-- INDEX: (cod_luna, archivo_cartera, EXTRACT(YEAR_MONTH FROM fecha_gestion)) para cálculos mensuales

-- ================================================================
-- LÓGICA DE NEGOCIO - SISTEMA DE PESO
-- ================================================================

-- CÁLCULO DE PESO POR GESTIÓN:
-- - Compromiso (es_compromiso = TRUE): peso = 3.0
-- - Contacto Efectivo (es_contacto_efectivo = TRUE): peso = 2.0  
-- - Gestión Simple: peso = 1.0
-- - Si es compromiso Y contacto efectivo: peso = 3.0 (prevalece compromiso)
--
-- MEJOR GESTIÓN DEL DÍA:
-- - Para cada (cod_luna, archivo_cartera, fecha_gestion)
-- - Se marca es_mejor_gestion_dia = TRUE la de mayor peso
-- - En caso de empate: prevalece la de mayor monto_compromiso
-- - En caso de empate: prevalece la primera por timestamp_gestion
--
-- CÁLCULOS MENSUALES:
-- - peso_acumulado_mes: suma de peso_gestion donde es_mejor_gestion_dia = TRUE
-- - gestiones_mes_cliente: count(*) para el mes 
-- - compromisos_mes_cliente: count donde es_compromiso = TRUE
-- - contactos_efectivos_mes_cliente: count donde es_contacto_efectivo = TRUE
-- - tasa_efectividad_cliente_mes: (contactos + compromisos) / total_gestiones
--
-- RANKING MENSUAL:
-- - ranking_cliente_mes: ROW_NUMBER() OVER (ORDER BY peso_acumulado_mes DESC)
-- - es_cliente_top_mes: TRUE si ranking <= PERCENTILE_90
--
-- CONSIDERACIONES LLAVE COMPUESTA:
-- - Un cliente puede estar en múltiples carteras (archivo_cartera) en el mismo mes
-- - Cada combinación (cod_luna, archivo_cartera) se trata independientemente
-- - Los cálculos mensuales son por combinación cliente+cartera
-- - Esto permite analizar efectividad del cliente por tipo de cartera
