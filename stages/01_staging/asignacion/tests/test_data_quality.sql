-- ================================================================
-- TESTS DE CALIDAD DE DATOS: Stage de Asignación
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Descripción: Conjunto de tests para validar la calidad de datos
--              en el stage de asignación
-- ================================================================

-- ================================================================
-- TEST 1: Validación de llaves primarias únicas
-- ================================================================
WITH test_unique_keys AS (
  SELECT 
    cod_luna,
    cod_cuenta,
    archivo,
    COUNT(*) as duplicados
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY cod_luna, cod_cuenta, archivo
  HAVING COUNT(*) > 1
)
SELECT 
  'TEST_UNIQUE_KEYS' as test_name,
  COUNT(*) as violaciones,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_unique_keys;

-- ================================================================
-- TEST 2: Validación de valores obligatorios
-- ================================================================
WITH test_not_null AS (
  SELECT
    'cod_luna' as campo,
    COUNT(*) as nulos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE cod_luna IS NULL AND fecha_proceso = CURRENT_DATE()
  
  UNION ALL
  
  SELECT
    'servicio' as campo,
    COUNT(*) as nulos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE servicio IS NULL AND fecha_proceso = CURRENT_DATE()
  
  UNION ALL
  
  SELECT
    'segmento_gestion' as campo,
    COUNT(*) as nulos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE segmento_gestion IS NULL AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_NOT_NULL' as test_name,
  campo,
  nulos as violaciones,
  CASE WHEN nulos = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_not_null
WHERE nulos > 0;

-- ================================================================
-- TEST 3: Validación de rangos de objetivo de recupero
-- ================================================================
WITH test_objetivo_rango AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE (objetivo_recupero < 0 OR objetivo_recupero > 1)
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_OBJETIVO_RANGO' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_objetivo_rango;

-- ================================================================
-- TEST 4: Validación de categorías de vencimiento
-- ================================================================
WITH test_categoria_vencimiento AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE categoria_vencimiento NOT IN (
    'SIN_VENCIMIENTO', 'VENCIDO', 'POR_VENCER_30D', 
    'POR_VENCER_60D', 'POR_VENCER_90D', 'VIGENTE_MAS_90D'
  )
  AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_CATEGORIA_VENCIMIENTO' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_categoria_vencimiento;

-- ================================================================
-- TEST 5: Validación de completitud de join con calendario
-- ================================================================
WITH test_join_calendario AS (
  SELECT COUNT(*) as registros_sin_calendario
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE fecha_asignacion IS NULL
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_JOIN_CALENDARIO' as test_name,
  registros_sin_calendario as violaciones,
  CASE WHEN registros_sin_calendario = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_join_calendario;

-- ================================================================
-- RESUMEN DE TESTS
-- ================================================================
WITH all_tests AS (
  -- Aquí se unirían todos los tests anteriores
  SELECT 'PLACEHOLDER' as test_name, 0 as violaciones, 'PASS' as resultado
)
SELECT 
  COUNT(*) as total_tests,
  SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) as tests_passed,
  SUM(CASE WHEN resultado = 'FAIL' THEN 1 ELSE 0 END) as tests_failed,
  ROUND(SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as success_rate
FROM all_tests
WHERE test_name != 'PLACEHOLDER';
