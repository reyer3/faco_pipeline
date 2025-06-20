-- ================================================================
-- DEBUG PASO A PASO: SP Gestiones - Encontrar el problema real
-- ================================================================
-- Ejecutar consulta por consulta para identificar dónde falla
-- ================================================================

-- 🔍 PASO 1: Verificar si las tablas fuente existen y tienen datos
SELECT 
  'VERIFICACION_TABLAS_FUENTE' as test,
  '1_BOT_GENERAL' as subtipo,
  COUNT(*) as total_registros,
  MIN(DATE(date)) as fecha_min,
  MAX(DATE(date)) as fecha_max
FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`

UNION ALL

SELECT 
  'VERIFICACION_TABLAS_FUENTE' as test,
  '2_HUMANO_GENERAL' as subtipo,
  COUNT(*) as total_registros,
  MIN(DATE(date)) as fecha_min,
  MAX(DATE(date)) as fecha_max
FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`

UNION ALL

SELECT 
  'VERIFICACION_TABLAS_FUENTE' as test,
  '3_BOT_FECHA_ESPECIFICA' as subtipo,
  COUNT(*) as total_registros,
  MIN(DATE(date)) as fecha_min,
  MAX(DATE(date)) as fecha_max
FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) = '2025-05-14'

UNION ALL

SELECT 
  'VERIFICACION_TABLAS_FUENTE' as test,
  '4_HUMANO_FECHA_ESPECIFICA' as subtipo,
  COUNT(*) as total_registros,
  MIN(DATE(date)) as fecha_min,
  MAX(DATE(date)) as fecha_max
FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) = '2025-05-14';

-- ================================================================
-- 🔍 PASO 2: Ver fechas disponibles recientes
-- ================================================================

SELECT 
  'FECHAS_RECIENTES' as test,
  'BOT' as canal,
  DATE(date) as fecha,
  COUNT(*) as registros,
  COUNT(CASE WHEN SAFE_CAST(document AS INT64) IS NOT NULL THEN 1 END) as con_document_valido
FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) >= '2025-05-01'
GROUP BY DATE(date)

UNION ALL

SELECT 
  'FECHAS_RECIENTES' as test,
  'HUMANO' as canal,
  DATE(date) as fecha,
  COUNT(*) as registros,
  COUNT(CASE WHEN SAFE_CAST(document AS INT64) IS NOT NULL THEN 1 END) as con_document_valido
FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) >= '2025-05-01'
GROUP BY DATE(date)

ORDER BY fecha DESC, canal
LIMIT 20;

-- ================================================================
-- 🔍 PASO 3: Probar la unión más simple posible
-- ================================================================

WITH gestiones_simples AS (
  -- BOT
  SELECT 
    SAFE_CAST(document AS INT64) AS cod_luna,
    DATE(date) AS fecha_gestion,
    'BOT' AS canal_origen,
    date as timestamp_original
  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = '2025-05-14'
    AND SAFE_CAST(document AS INT64) IS NOT NULL
  
  UNION ALL
  
  -- HUMANO
  SELECT 
    SAFE_CAST(document AS INT64) AS cod_luna,
    DATE(date) AS fecha_gestion,
    'HUMANO' AS canal_origen,
    date as timestamp_original
  FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = '2025-05-14'
    AND SAFE_CAST(document AS INT64) IS NOT NULL
)

SELECT 
  'UNION_SIMPLE' as test,
  canal_origen,
  COUNT(*) as total_registros,
  COUNT(DISTINCT cod_luna) as clientes_unicos,
  MIN(timestamp_original) as primera_gestion,
  MAX(timestamp_original) as ultima_gestion
FROM gestiones_simples
GROUP BY canal_origen;

-- ================================================================
-- 🔍 PASO 4: Verificar si el problema está en el calendario
-- ================================================================

SELECT 
  'CALENDARIO_CHECK' as test,
  FECHA_ASIGNACION,
  ARCHIVO,
  FECHA_TRANDEUDA,
  FECHA_CIERRE
FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
WHERE FECHA_ASIGNACION = '2025-05-14'
   OR FECHA_TRANDEUDA = '2025-05-14'
   OR '2025-05-14' BETWEEN FECHA_ASIGNACION AND FECHA_CIERRE;

-- ================================================================
-- 🔍 PASO 5: Test del JOIN con calendario
-- ================================================================

WITH gestiones_simples AS (
  SELECT 
    SAFE_CAST(document AS INT64) AS cod_luna,
    DATE(date) AS fecha_gestion,
    'BOT' AS canal_origen
  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = '2025-05-14'
    AND SAFE_CAST(document AS INT64) IS NOT NULL
  LIMIT 10  -- Solo 10 para test
)

SELECT 
  'JOIN_CALENDARIO_TEST' as test,
  g.canal_origen,
  g.cod_luna,
  g.fecha_gestion,
  cal.ARCHIVO,
  cal.FECHA_ASIGNACION
FROM gestiones_simples g
LEFT JOIN `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` cal
  ON g.fecha_gestion = cal.FECHA_ASIGNACION;

-- ================================================================
-- 🔍 PASO 6: Verificar si las tablas stages anteriores tienen datos
-- ================================================================

SELECT 
  'STAGES_PREVIOS' as test,
  'ASIGNACION' as stage,
  COUNT(*) as registros,
  MIN(fecha_asignacion) as fecha_min,
  MAX(fecha_asignacion) as fecha_max
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
WHERE fecha_asignacion = '2025-05-14'

UNION ALL

SELECT 
  'STAGES_PREVIOS' as test,
  'DEUDAS' as stage,
  COUNT(*) as registros,
  MIN(fecha_deuda) as fecha_min,
  MAX(fecha_deuda) as fecha_max
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_deuda = '2025-05-14';

-- ================================================================
-- 🔍 PASO 7: Ver el estado actual de la tabla gestiones
-- ================================================================

SELECT 
  'TABLA_GESTIONES_ACTUAL' as test,
  fecha_proceso,
  canal_origen,
  COUNT(*) as registros
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
WHERE fecha_proceso >= '2025-05-10'
GROUP BY fecha_proceso, canal_origen
ORDER BY fecha_proceso DESC, canal_origen;
