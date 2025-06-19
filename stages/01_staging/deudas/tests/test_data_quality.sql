-- ================================================================
-- TESTS DE CALIDAD DE DATOS: Stage de Deudas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Descripción: Conjunto de tests para validar la calidad de datos
--              en el stage de deudas, incluyendo lógica de negocio
-- ================================================================

-- ================================================================
-- TEST 1: Validación de llaves primarias únicas
-- ================================================================
WITH test_unique_keys AS (
  SELECT 
    cod_cuenta,
    nro_documento,
    archivo,
    fecha_deuda,
    COUNT(*) as duplicados
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY cod_cuenta, nro_documento, archivo, fecha_deuda
  HAVING COUNT(*) > 1
)
SELECT 
  'TEST_UNIQUE_KEYS_DEUDAS' as test_name,
  COUNT(*) as violaciones,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_unique_keys;

-- ================================================================
-- TEST 2: Validación de construcción de fechas desde archivo
-- ================================================================
WITH test_fecha_construccion AS (
  SELECT COUNT(*) as registros_fecha_nula
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_deuda_construida IS NULL
    AND fecha_proceso = CURRENT_DATE()
    AND REGEXP_CONTAINS(archivo, r'TRAN_DEUDA_\d{4}')
)
SELECT 
  'TEST_FECHA_CONSTRUCCION' as test_name,
  registros_fecha_nula as violaciones,
  CASE WHEN registros_fecha_nula = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_fecha_construccion;

-- ================================================================
-- TEST 3: Consistencia de lógica día de apertura
-- ================================================================
WITH test_dia_apertura AS (
  -- Verificar que todos los registros del mismo día tengan el mismo valor de es_dia_apertura
  SELECT 
    fecha_proceso,
    COUNT(DISTINCT es_dia_apertura) as valores_distintos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY fecha_proceso
  HAVING COUNT(DISTINCT es_dia_apertura) > 1
)
SELECT 
  'TEST_CONSISTENCIA_DIA_APERTURA' as test_name,
  COUNT(*) as violaciones,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_dia_apertura;

-- ================================================================
-- TEST 4: Validación de cálculos de montos medibles
-- ================================================================
WITH test_montos_medibles AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Monto medible debe ser 0 si no es medible
      (es_medible = FALSE AND monto_medible > 0)
      OR
      -- Monto medible debe ser igual a monto_exigible si es medible
      (es_medible = TRUE AND monto_medible != monto_exigible)
      OR
      -- Monto gestionable debe ser 0 si no es gestionable
      (es_gestionable = FALSE AND monto_gestionable > 0)
      OR
      -- Monto gestionable debe ser igual a monto_exigible si es gestionable
      (es_gestionable = TRUE AND monto_gestionable != monto_exigible)
    )
)
SELECT 
  'TEST_CALCULO_MONTOS' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_montos_medibles;

-- ================================================================
-- TEST 5: Validación de lógica es_medible
-- ================================================================
WITH test_logica_medible AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Si es medible, debe ser gestionable Y día de apertura
      (es_medible = TRUE AND (es_gestionable = FALSE OR es_dia_apertura = FALSE))
      OR
      -- Si no es día de apertura, no debe ser medible (independiente de si es gestionable)
      (es_dia_apertura = FALSE AND es_medible = TRUE)
    )
)
SELECT 
  'TEST_LOGICA_MEDIBLE' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_logica_medible;

-- ================================================================
-- TEST 6: Validación de tipos de activación
-- ================================================================
WITH test_tipos_activacion AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE tipo_activacion NOT IN ('APERTURA', 'SUBSIGUIENTE', 'REACTIVACION')
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_TIPOS_ACTIVACION' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_tipos_activacion;

-- ================================================================
-- TEST 7: Validación de rangos de montos
-- ================================================================
WITH test_rangos_montos AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE (monto_exigible < 0 OR monto_gestionable < 0 OR monto_medible < 0)
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_RANGOS_MONTOS' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_rangos_montos;

-- ================================================================
-- TEST 8: Validación de consistencia tiene_asignacion vs es_gestionable
-- ================================================================
WITH test_consistencia_asignacion AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Si tiene asignación, debe ser gestionable
      (tiene_asignacion = TRUE AND es_gestionable = FALSE)
      OR
      -- Si es gestionable, debe tener asignación
      (es_gestionable = TRUE AND tiene_asignacion = FALSE)
    )
)
SELECT 
  'TEST_CONSISTENCIA_ASIGNACION' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_consistencia_asignacion;

-- ================================================================
-- RESUMEN DE MÉTRICAS DE NEGOCIO
-- ================================================================
SELECT 
  'METRICAS_NEGOCIO_DEUDAS' as tipo,
  fecha_proceso,
  es_dia_apertura,
  COUNT(*) as total_deudas,
  ROUND(SUM(monto_exigible), 2) as monto_total,
  COUNT(CASE WHEN es_gestionable THEN 1 END) as deudas_gestionables,
  ROUND(SUM(monto_gestionable), 2) as monto_gestionable,
  COUNT(CASE WHEN es_medible THEN 1 END) as deudas_medibles,
  ROUND(SUM(monto_medible), 2) as monto_medible,
  COUNT(DISTINCT cod_cuenta) as clientes_unicos,
  COUNT(DISTINCT archivo) as archivos_procesados,
  COUNT(DISTINCT tipo_activacion) as tipos_activacion_distintos
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = CURRENT_DATE()
GROUP BY fecha_proceso, es_dia_apertura;

-- ================================================================
-- TEST COMPARATIVO: Deudas por tipo de día
-- ================================================================
WITH comparativo_tipos_dia AS (
  SELECT 
    tipo_activacion,
    es_dia_apertura,
    COUNT(*) as cantidad_deudas,
    ROUND(SUM(monto_exigible), 2) as monto_total,
    ROUND(AVG(monto_exigible), 2) as monto_promedio,
    COUNT(CASE WHEN es_gestionable THEN 1 END) as gestionables,
    COUNT(CASE WHEN es_medible THEN 1 END) as medibles
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY tipo_activacion, es_dia_apertura
)
SELECT 
  'COMPARATIVO_TIPOS_DIA' as test_name,
  *,
  CASE 
    WHEN tipo_activacion = 'APERTURA' AND medibles = 0 THEN 'WARN: Sin deudas medibles en apertura'
    WHEN tipo_activacion = 'SUBSIGUIENTE' AND medibles > 0 THEN 'WARN: Deudas medibles en subsiguiente'
    ELSE 'OK'
  END as validacion
FROM comparativo_tipos_dia
ORDER BY tipo_activacion, es_dia_apertura;

-- ================================================================
-- RESUMEN CONSOLIDADO DE TODOS LOS TESTS
-- ================================================================
WITH todos_los_tests AS (
  -- Este sería el UNION de todos los tests anteriores
  -- Por simplicidad, mostramos estructura
  SELECT 'TEST_PLACEHOLDER' as test_name, 0 as violaciones, 'PASS' as resultado
)
SELECT 
  'RESUMEN_TESTS_DEUDAS' as resumen,
  COUNT(*) as total_tests,
  SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) as tests_passed,
  SUM(CASE WHEN resultado = 'FAIL' THEN 1 ELSE 0 END) as tests_failed,
  ROUND(SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as success_rate
FROM todos_los_tests
WHERE test_name != 'TEST_PLACEHOLDER';
