-- ================================================================
-- TESTS DE CALIDAD DE DATOS: Stage de Gestiones
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Descripción: Conjunto de tests para validar la calidad de datos
--              en el stage de gestiones unificadas
-- ================================================================

-- ================================================================
-- TEST 1: Validación de llaves primarias únicas
-- ================================================================
WITH test_unique_keys AS (
  SELECT 
    cod_luna,
    fecha_gestion,
    canal_origen,
    secuencia_gestion,
    COUNT(*) as duplicados
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY cod_luna, fecha_gestion, canal_origen, secuencia_gestion
  HAVING COUNT(*) > 1
)
SELECT 
  'TEST_UNIQUE_KEYS_GESTIONES' as test_name,
  COUNT(*) as violaciones,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_unique_keys;

-- ================================================================
-- TEST 2: Validación de canales válidos
-- ================================================================
WITH test_canales_validos AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE canal_origen NOT IN ('BOT', 'HUMANO')
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_CANALES_VALIDOS' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_canales_validos;

-- ================================================================
-- TEST 3: Validación de secuencia de gestiones
-- ================================================================
WITH test_secuencia_gestiones AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE secuencia_gestion <= 0
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_SECUENCIA_GESTIONES' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_secuencia_gestiones;

-- ================================================================
-- TEST 4: Consistencia de homologación BOT
-- ================================================================
WITH test_homologacion_bot AS (
  SELECT COUNT(*) as registros_sin_homologar
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE canal_origen = 'BOT'
    AND grupo_respuesta = management_original
    AND management_original != 'SIN_MANAGEMENT'
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_HOMOLOGACION_BOT' as test_name,
  registros_sin_homologar as violaciones,
  CASE 
    WHEN registros_sin_homologar = 0 THEN 'PASS'
    WHEN registros_sin_homologar < 10 THEN 'WARN'
    ELSE 'FAIL'
  END as resultado
FROM test_homologacion_bot;

-- ================================================================
-- TEST 5: Consistencia de homologación HUMANO
-- ================================================================
WITH test_homologacion_humano AS (
  SELECT COUNT(*) as registros_sin_homologar
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE canal_origen = 'HUMANO'
    AND grupo_respuesta = management_original
    AND management_original != 'SIN_MANAGEMENT'
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_HOMOLOGACION_HUMANO' as test_name,
  registros_sin_homologar as violaciones,
  CASE 
    WHEN registros_sin_homologar = 0 THEN 'PASS'
    WHEN registros_sin_homologar < 10 THEN 'WARN'
    ELSE 'FAIL'
  END as resultado
FROM test_homologacion_humano;

-- ================================================================
-- TEST 6: Validación de montos de compromiso
-- ================================================================
WITH test_montos_compromiso AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE (
    -- Monto negativo
    monto_compromiso < 0
    OR
    -- Compromiso sin monto
    (es_compromiso = TRUE AND monto_compromiso = 0)
    OR
    -- Monto sin compromiso (para canal HUMANO)
    (es_compromiso = FALSE AND monto_compromiso > 0 AND canal_origen = 'HUMANO')
  )
  AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_MONTOS_COMPROMISO' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_montos_compromiso;

-- ================================================================
-- TEST 7: Consistencia flags primera gestión día
-- ================================================================
WITH test_primera_gestion AS (
  SELECT 
    cod_luna,
    fecha_gestion,
    COUNT(CASE WHEN es_primera_gestion_dia THEN 1 END) as primeras_gestiones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY cod_luna, fecha_gestion
  HAVING COUNT(CASE WHEN es_primera_gestion_dia THEN 1 END) != 1
)
SELECT 
  'TEST_PRIMERA_GESTION_DIA' as test_name,
  COUNT(*) as violaciones,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_primera_gestion;

-- ================================================================
-- TEST 8: Validación operadores BOT
-- ================================================================
WITH test_operadores_bot AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE canal_origen = 'BOT'
    AND operador_final != 'SISTEMA_BOT'
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_OPERADORES_BOT' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_operadores_bot;

-- ================================================================
-- TEST 9: Consistencia con asignación y deudas
-- ================================================================
WITH test_medibilidad AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Si es medible, debe tener asignación O deuda
      (es_gestion_medible = TRUE AND tiene_asignacion = FALSE AND tiene_deuda = FALSE)
      OR
      -- Si no tiene ni asignación ni deuda, no debe ser medible
      (tiene_asignacion = FALSE AND tiene_deuda = FALSE AND es_gestion_medible = TRUE)
    )
)
SELECT 
  'TEST_MEDIBILIDAD_GESTIONES' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_medibilidad;

-- ================================================================
-- TEST 10: Validación de días de semana
-- ================================================================
WITH test_dias_semana AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE dia_semana NOT IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_DIAS_SEMANA' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_dias_semana;

-- ================================================================
-- RESUMEN DE MÉTRICAS DE NEGOCIO
-- ================================================================
SELECT 
  'METRICAS_NEGOCIO_GESTIONES' as tipo,
  fecha_proceso,
  COUNT(*) as total_gestiones,
  COUNT(DISTINCT cod_luna) as clientes_gestionados,
  COUNT(DISTINCT CASE WHEN canal_origen = 'BOT' THEN cod_luna END) as clientes_bot,
  COUNT(DISTINCT CASE WHEN canal_origen = 'HUMANO' THEN cod_luna END) as clientes_humano,
  COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) as contactos_efectivos,
  COUNT(CASE WHEN es_compromiso THEN 1 END) as compromisos,
  ROUND(SUM(monto_compromiso), 2) as monto_compromisos,
  COUNT(CASE WHEN es_gestion_medible THEN 1 END) as gestiones_medibles,
  COUNT(CASE WHEN es_primera_gestion_dia THEN 1 END) as primeras_gestiones,
  ROUND(COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) / COUNT(*) * 100, 2) as pct_efectividad,
  ROUND(COUNT(CASE WHEN es_compromiso THEN 1 END) / COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) * 100, 2) as pct_compromiso_sobre_efectivos
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
WHERE fecha_proceso = CURRENT_DATE()
GROUP BY fecha_proceso;

-- ================================================================
-- ANÁLISIS DE EFECTIVIDAD POR CANAL
-- ================================================================
SELECT 
  'EFECTIVIDAD_POR_CANAL' as analisis,
  canal_origen,
  COUNT(*) as total_gestiones,
  COUNT(DISTINCT cod_luna) as clientes_unicos,
  COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) as contactos_efectivos,
  COUNT(CASE WHEN es_compromiso THEN 1 END) as compromisos,
  ROUND(AVG(CASE WHEN es_compromiso THEN monto_compromiso END), 2) as monto_promedio_compromiso,
  ROUND(COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) / COUNT(*) * 100, 2) as pct_efectividad,
  ROUND(COUNT(CASE WHEN es_compromiso THEN 1 END) / COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) * 100, 2) as pct_conversion_compromiso
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
WHERE fecha_proceso = CURRENT_DATE()
GROUP BY canal_origen
ORDER BY canal_origen;

-- ================================================================
-- ANÁLISIS DE HOMOLOGACIÓN
-- ================================================================
WITH analisis_homologacion AS (
  SELECT 
    canal_origen,
    COUNT(*) as total_gestiones,
    COUNT(CASE WHEN grupo_respuesta != management_original THEN 1 END) as respuestas_homologadas,
    COUNT(CASE WHEN nivel_1 != 'SIN_N1' THEN 1 END) as con_nivel_1,
    COUNT(CASE WHEN nivel_2 != 'SIN_N2' THEN 1 END) as con_nivel_2,
    COUNT(DISTINCT management_original) as management_originales_distintos,
    COUNT(DISTINCT grupo_respuesta) as grupos_respuesta_distintos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY canal_origen
)
SELECT 
  'ANALISIS_HOMOLOGACION' as reporte,
  canal_origen,
  total_gestiones,
  ROUND(respuestas_homologadas / total_gestiones * 100, 2) as pct_homologacion,
  ROUND(con_nivel_1 / total_gestiones * 100, 2) as pct_con_nivel_1,
  ROUND(con_nivel_2 / total_gestiones * 100, 2) as pct_con_nivel_2,
  management_originales_distintos,
  grupos_respuesta_distintos,
  CASE 
    WHEN respuestas_homologadas / total_gestiones >= 0.8 THEN '✅ Buena homologación'
    WHEN respuestas_homologadas / total_gestiones >= 0.5 THEN '⚠️ Homologación parcial'
    ELSE '❌ Revisar homologación'
  END as evaluacion
FROM analisis_homologacion
ORDER BY canal_origen;

-- ================================================================
-- RESUMEN CONSOLIDADO DE TODOS LOS TESTS
-- ================================================================
WITH todos_los_tests AS (
  -- Este sería el UNION de todos los tests anteriores
  -- Por simplicidad, mostramos estructura
  SELECT 'TEST_PLACEHOLDER' as test_name, 0 as violaciones, 'PASS' as resultado
)
SELECT 
  'RESUMEN_TESTS_GESTIONES' as resumen,
  COUNT(*) as total_tests,
  SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) as tests_passed,
  SUM(CASE WHEN resultado = 'FAIL' THEN 1 ELSE 0 END) as tests_failed,
  SUM(CASE WHEN resultado = 'WARN' THEN 1 ELSE 0 END) as tests_warnings,
  ROUND(SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as success_rate
FROM todos_los_tests
WHERE test_name != 'TEST_PLACEHOLDER';
