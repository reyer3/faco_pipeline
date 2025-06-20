-- ================================================================
-- DIAGNÓSTICO: SP Gestiones - Troubleshooting
-- ================================================================
-- Para identificar por qué no está cargando datos
-- Ejecutar consultas una por una para encontrar el problema
-- ================================================================

-- 🔍 PASO 1: Verificar datos en tablas fuente
-- ================================================================

-- Verificar si existen datos BOT para la fecha
SELECT 
  'DATOS_BOT' as fuente,
  COUNT(*) as total_registros,
  COUNT(CASE WHEN SAFE_CAST(document AS INT64) IS NOT NULL THEN 1 END) as con_document_valido,
  MIN(DATE(date)) as fecha_minima,
  MAX(DATE(date)) as fecha_maxima,
  COUNT(DISTINCT DATE(date)) as dias_unicos
FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) = '2025-05-14'

UNION ALL

-- Verificar si existen datos HUMANO para la fecha
SELECT 
  'DATOS_HUMANO' as fuente,
  COUNT(*) as total_registros,
  COUNT(CASE WHEN SAFE_CAST(document AS INT64) IS NOT NULL THEN 1 END) as con_document_valido,
  MIN(DATE(date)) as fecha_minima,
  MAX(DATE(date)) as fecha_maxima,
  COUNT(DISTINCT DATE(date)) as dias_unicos
FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) = '2025-05-14';

-- ================================================================
-- 🔍 PASO 2: Verificar fechas disponibles en las fuentes
-- ================================================================

-- Ver qué fechas están disponibles cerca de 2025-05-14
SELECT 
  'FECHAS_DISPONIBLES_BOT' as tipo,
  DATE(date) as fecha,
  COUNT(*) as registros
FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) BETWEEN '2025-05-10' AND '2025-05-20'
GROUP BY DATE(date)

UNION ALL

SELECT 
  'FECHAS_DISPONIBLES_HUMANO' as tipo,
  DATE(date) as fecha,
  COUNT(*) as registros
FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) BETWEEN '2025-05-10' AND '2025-05-20'
GROUP BY DATE(date)

ORDER BY tipo, fecha;

-- ================================================================
-- 🔍 PASO 3: Verificar calendario (posible causa)
-- ================================================================

-- Ver si hay datos en el calendario para esa fecha
SELECT 
  'CALENDARIO' as fuente,
  FECHA_ASIGNACION,
  ARCHIVO,
  FECHA_TRANDEUDA,
  FECHA_CIERRE
FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
WHERE FECHA_ASIGNACION = '2025-05-14'
   OR FECHA_TRANDEUDA = '2025-05-14';

-- ================================================================
-- 🔍 PASO 4: Test de la lógica de unión (simplificada)
-- ================================================================

-- Test rápido para ver si los JOINs funcionan
SELECT 
  'TEST_UNION' as tipo,
  COUNT(*) as total_registros,
  COUNT(CASE WHEN cal.ARCHIVO IS NOT NULL THEN 1 END) as con_archivo_cartera,
  COUNT(DISTINCT COALESCE(cal.ARCHIVO, 'SIN_CARTERA')) as carteras_distintas

FROM (
  -- BOT simplificado
  SELECT 
    SAFE_CAST(document AS INT64) as cod_luna,
    DATE(date) as fecha_gestion,
    'BOT' as canal
  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = '2025-05-14'
    AND SAFE_CAST(document AS INT64) IS NOT NULL
  
  UNION ALL
  
  -- HUMANO simplificado  
  SELECT 
    SAFE_CAST(document AS INT64) as cod_luna,
    DATE(date) as fecha_gestion,
    'HUMANO' as canal
  FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = '2025-05-14'
    AND SAFE_CAST(document AS INT64) IS NOT NULL
    
) AS gestiones
LEFT JOIN `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` AS cal
  ON gestiones.fecha_gestion = cal.FECHA_ASIGNACION;

-- ================================================================
-- 🔍 PASO 5: Verificar problema específico del canal filter
-- ================================================================

-- El problema puede estar en el canal_filter
-- En el SP: (p_canal_filter IS NULL OR p_canal_filter = 'BOT')
-- Si pasas '', no es NULL, entonces solo filtra BOT

SELECT 
  'CANAL_FILTER_TEST' as test,
  -- Simular la lógica del SP
  CASE 
    WHEN '' IS NULL OR '' = 'BOT' THEN 'FILTRA_SOLO_BOT'
    WHEN '' IS NULL OR '' = 'HUMANO' THEN 'FILTRA_SOLO_HUMANO'  
    WHEN '' IS NULL THEN 'FILTRA_AMBOS'
    ELSE 'NO_FILTRA_NADA'
  END as resultado_filtro;

-- ================================================================
-- 🔍 PASO 6: Ver logs del pipeline
-- ================================================================

-- Verificar si hay logs de ejecución
SELECT 
  timestamp,
  stage_name,
  fecha_proceso,
  status,
  records_processed,
  message,
  execution_parameters
FROM `BI_USA.pipeline_logs`
WHERE stage_name = 'stage_gestiones'
  AND fecha_proceso = '2025-05-14'
ORDER BY timestamp DESC
LIMIT 5;
