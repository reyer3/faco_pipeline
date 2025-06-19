-- ================================================================
-- TESTS DE CALIDAD DE DATOS: Stage de Deudas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.1.0
-- Descripción: Conjunto de tests para validar la calidad de datos
--              en el stage de deudas, incluyendo lógica de FECHA_TRANDEUDA
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
-- TEST 3: Validación específica lógica FECHA_TRANDEUDA
-- ================================================================
WITH test_medibilidad_trandeuda AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE es_medible = TRUE 
    AND (fecha_trandeuda IS NULL OR fecha_deuda != fecha_trandeuda)
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_MEDIBILIDAD_TRANDEUDA' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_medibilidad_trandeuda;

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
-- TEST 5: Validación de lógica es_medible CORREGIDA
-- ================================================================
WITH test_logica_medible AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Si es medible, debe ser gestionable Y tener coincidencia FECHA_TRANDEUDA
      (es_medible = TRUE AND (es_gestionable = FALSE OR fecha_deuda != fecha_trandeuda))
      OR
      -- Si no es gestionable, no debe ser medible
      (es_gestionable = FALSE AND es_medible = TRUE)
      OR
      -- Si no coincide FECHA_TRANDEUDA, no debe ser medible
      (fecha_trandeuda IS NULL AND es_medible = TRUE)
    )
)
SELECT 
  'TEST_LOGICA_MEDIBLE_TRANDEUDA' as test_name,
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
-- TEST 9: Cobertura de FECHA_TRANDEUDA en calendario
-- ================================================================
WITH test_cobertura_trandeuda AS (
  SELECT 
    COUNT(*) as total_deudas,
    COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) as con_calendario,
    ROUND(COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) / COUNT(*) * 100, 2) as pct_cobertura
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_COBERTURA_TRANDEUDA' as test_name,
  CASE WHEN pct_cobertura < 50 THEN 1 ELSE 0 END as violaciones,
  CASE WHEN pct_cobertura >= 50 THEN 'PASS' ELSE 'WARN' END as resultado,
  CONCAT('Cobertura: ', CAST(pct_cobertura AS STRING), '%') as detalle
FROM test_cobertura_trandeuda;

-- ================================================================
-- RESUMEN DE MÉTRICAS DE NEGOCIO CON FECHA_TRANDEUDA
-- ================================================================
SELECT 
  'METRICAS_NEGOCIO_TRANDEUDA' as tipo,
  fecha_proceso,
  COUNT(*) as total_deudas,
  ROUND(SUM(monto_exigible), 2) as monto_total,
  COUNT(CASE WHEN es_gestionable THEN 1 END) as deudas_gestionables,
  ROUND(SUM(monto_gestionable), 2) as monto_gestionable,
  COUNT(CASE WHEN es_medible THEN 1 END) as deudas_medibles_por_trandeuda,
  ROUND(SUM(monto_medible), 2) as monto_medible_por_trandeuda,
  COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) as deudas_con_calendario,
  COUNT(DISTINCT fecha_deuda) as fechas_distintas_archivos,
  COUNT(DISTINCT fecha_trandeuda) as fechas_distintas_calendario,
  ROUND(COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) / COUNT(*) * 100, 2) as pct_cobertura_calendario
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = CURRENT_DATE()
GROUP BY fecha_proceso;

-- ================================================================
-- ANÁLISIS DE COINCIDENCIAS POR FECHA
-- ================================================================
SELECT 
  'ANALISIS_COINCIDENCIAS_FECHA' as tipo,
  fecha_deuda_construida,
  fecha_trandeuda,
  COUNT(*) as cantidad_deudas,
  ROUND(SUM(monto_exigible), 2) as monto_total,
  COUNT(CASE WHEN es_gestionable THEN 1 END) as gestionables,
  COUNT(CASE WHEN es_medible THEN 1 END) as medibles,
  CASE 
    WHEN fecha_trandeuda IS NULL THEN 'SIN_CALENDARIO'
    WHEN fecha_deuda_construida = fecha_trandeuda THEN 'COINCIDE'
    ELSE 'NO_COINCIDE'
  END as estado_coincidencia
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = CURRENT_DATE()
GROUP BY fecha_deuda_construida, fecha_trandeuda
ORDER BY fecha_deuda_construida, fecha_trandeuda;

-- ================================================================
-- COMPARATIVO: Antes vs Después de corrección FECHA_TRANDEUDA
-- ================================================================
WITH comparativo_logicas AS (
  SELECT 
    -- Lógica anterior (incorrecta) - solo para comparación
    COUNT(CASE WHEN es_gestionable AND es_dia_apertura THEN 1 END) as medibles_logica_anterior,
    ROUND(SUM(CASE WHEN es_gestionable AND es_dia_apertura THEN monto_exigible ELSE 0 END), 2) as monto_logica_anterior,
    
    -- Lógica actual (correcta) - por FECHA_TRANDEUDA
    COUNT(CASE WHEN es_medible THEN 1 END) as medibles_logica_actual,
    ROUND(SUM(monto_medible), 2) as monto_logica_actual
    
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
)
SELECT 
  'COMPARATIVO_LOGICAS_MEDIBILIDAD' as test_name,
  medibles_logica_anterior,
  medibles_logica_actual,
  medibles_logica_anterior - medibles_logica_actual as diferencia_cantidad,
  monto_logica_anterior - monto_logica_actual as diferencia_monto,
  CASE 
    WHEN medibles_logica_actual <= medibles_logica_anterior THEN 'OK'
    ELSE 'WARN: Más medibles con nueva lógica'
  END as validacion
FROM comparativo_logicas;

-- ================================================================
-- RESUMEN CONSOLIDADO DE TODOS LOS TESTS
-- ================================================================
WITH todos_los_tests AS (
  -- Este sería el UNION de todos los tests anteriores
  -- Por simplicidad, mostramos estructura
  SELECT 'TEST_PLACEHOLDER' as test_name, 0 as violaciones, 'PASS' as resultado
)
SELECT 
  'RESUMEN_TESTS_DEUDAS_TRANDEUDA' as resumen,
  COUNT(*) as total_tests,
  SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) as tests_passed,
  SUM(CASE WHEN resultado = 'FAIL' THEN 1 ELSE 0 END) as tests_failed,
  SUM(CASE WHEN resultado = 'WARN' THEN 1 ELSE 0 END) as tests_warnings,
  ROUND(SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as success_rate
FROM todos_los_tests
WHERE test_name != 'TEST_PLACEHOLDER';
