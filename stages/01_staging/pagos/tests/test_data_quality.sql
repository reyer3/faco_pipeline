-- ================================================================
-- TESTS DE CALIDAD DE DATOS: Stage de Pagos
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Descripción: Conjunto de tests para validar la calidad de datos
--              en el stage de pagos con atribución de gestiones
-- ================================================================

-- ================================================================
-- TEST 1: Validación de llaves primarias únicas
-- ================================================================
WITH test_unique_keys AS (
  SELECT 
    nro_documento,
    fecha_pago,
    COUNT(*) as duplicados
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY nro_documento, fecha_pago
  HAVING COUNT(*) > 1
)
SELECT 
  'TEST_UNIQUE_KEYS_PAGOS' as test_name,
  COUNT(*) as violaciones,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_unique_keys;

-- ================================================================
-- TEST 2: Validación de rangos de score de efectividad
-- ================================================================
WITH test_score_efectividad AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE (efectividad_atribucion < 0 OR efectividad_atribucion > 1)
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_SCORE_EFECTIVIDAD_RANGO' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_score_efectividad;

-- ================================================================
-- TEST 3: Validación de tipos de pago válidos
-- ================================================================
WITH test_tipos_pago AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE tipo_pago NOT IN ('PUNTUAL', 'TARDIO_PDP', 'POST_GESTION', 'ESPONTANEO')
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_TIPOS_PAGO_VALIDOS' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_tipos_pago;

-- ================================================================
-- TEST 4: Validación de categorías de efectividad
-- ================================================================
WITH test_categorias_efectividad AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE categoria_efectividad NOT IN ('ALTA', 'MEDIA', 'BAJA', 'SIN_ATRIBUCION')
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_CATEGORIAS_EFECTIVIDAD' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_categorias_efectividad;

-- ================================================================
-- TEST 5: Consistencia de flags PDP
-- ================================================================
WITH test_consistencia_pdp AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Si es pago con PDP, debe tener fecha compromiso
      (es_pago_con_pdp = TRUE AND fecha_compromiso IS NULL)
      OR
      -- Si PDP estaba vigente, debe ser pago con PDP
      (pdp_estaba_vigente = TRUE AND es_pago_con_pdp = FALSE)
      OR
      -- Si pago es puntual, debe tener PDP y fecha compromiso
      (pago_es_puntual = TRUE AND (es_pago_con_pdp = FALSE OR fecha_compromiso IS NULL))
    )
)
SELECT 
  'TEST_CONSISTENCIA_PDP' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_consistencia_pdp;

-- ================================================================
-- TEST 6: Consistencia de atribución de gestión
-- ================================================================
WITH test_consistencia_gestion AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Si tiene gestión previa, debe tener fecha gestión atribuida
      (tiene_gestion_previa = TRUE AND fecha_gestion_atribuida IS NULL)
      OR
      -- Si no tiene gestión previa, no debe tener datos de gestión
      (tiene_gestion_previa = FALSE AND fecha_gestion_atribuida IS NOT NULL)
      OR
      -- Canal y operador deben ser consistentes
      (tiene_gestion_previa = TRUE AND canal_atribuido = 'SIN_GESTION_PREVIA')
      OR
      (tiene_gestion_previa = FALSE AND canal_atribuido != 'SIN_GESTION_PREVIA')
    )
)
SELECT 
  'TEST_CONSISTENCIA_GESTION' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_consistencia_gestion;

-- ================================================================
-- TEST 7: Validación de cálculo de días entre gestión y pago
-- ================================================================
WITH test_calculo_dias AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- Si tiene gestión, debe tener días calculados
      (fecha_gestion_atribuida IS NOT NULL AND dias_entre_gestion_y_pago IS NULL)
      OR
      -- Si no tiene gestión, no debe tener días
      (fecha_gestion_atribuida IS NULL AND dias_entre_gestion_y_pago IS NOT NULL)
      OR
      -- Los días no pueden ser negativos
      (dias_entre_gestion_y_pago < 0)
    )
)
SELECT 
  'TEST_CALCULO_DIAS_GESTION_PAGO' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_calculo_dias;

-- ================================================================
-- TEST 8: Validación de montos positivos
-- ================================================================
WITH test_montos_positivos AS (
  SELECT COUNT(*) as violaciones
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE (monto_pagado <= 0 OR (monto_compromiso IS NOT NULL AND monto_compromiso <= 0))
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_MONTOS_POSITIVOS' as test_name,
  violaciones,
  CASE WHEN violaciones = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_montos_positivos;

-- ================================================================
-- TEST 9: Cobertura de contexto de cartera
-- ================================================================
WITH test_cobertura_cartera AS (
  SELECT 
    COUNT(*) as total_pagos,
    COUNT(CASE WHEN cartera IS NOT NULL AND vencimiento IS NOT NULL THEN 1 END) as con_contexto_cartera,
    ROUND(COUNT(CASE WHEN cartera IS NOT NULL AND vencimiento IS NOT NULL THEN 1 END) / COUNT(*) * 100, 2) as pct_cobertura
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_COBERTURA_CONTEXTO_CARTERA' as test_name,
  CASE WHEN pct_cobertura < 90 THEN 1 ELSE 0 END as violaciones,
  CASE WHEN pct_cobertura >= 90 THEN 'PASS' ELSE 'WARN' END as resultado,
  CONCAT('Cobertura contexto: ', CAST(pct_cobertura AS STRING), '%') as detalle
FROM test_cobertura_cartera;

-- ================================================================
-- TEST 10: Validación de coherencia score vs categoría efectividad
-- ================================================================
WITH test_coherencia_score_categoria AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_proceso = CURRENT_DATE()
    AND (
      -- ALTA debe ser score >= 0.8
      (categoria_efectividad = 'ALTA' AND efectividad_atribucion < 0.8)
      OR
      -- MEDIA debe ser score 0.4-0.8
      (categoria_efectividad = 'MEDIA' AND (efectividad_atribucion < 0.4 OR efectividad_atribucion >= 0.8))
      OR
      -- BAJA debe ser score 0.0-0.4
      (categoria_efectividad = 'BAJA' AND (efectividad_atribucion <= 0.0 OR efectividad_atribucion >= 0.4))
      OR
      -- SIN_ATRIBUCION debe ser score = 0.0
      (categoria_efectividad = 'SIN_ATRIBUCION' AND efectividad_atribucion != 0.0)
    )
)
SELECT 
  'TEST_COHERENCIA_SCORE_CATEGORIA' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_coherencia_score_categoria;

-- ================================================================
-- MÉTRICAS DE NEGOCIO: Resumen de atribución y efectividad
-- ================================================================
SELECT 
  'METRICAS_NEGOCIO_PAGOS' as tipo,
  COUNT(*) as total_pagos,
  ROUND(SUM(monto_pagado), 2) as monto_total_pagado,
  
  -- Distribución por tipo de pago
  COUNT(CASE WHEN tipo_pago = 'PUNTUAL' THEN 1 END) as pagos_puntuales,
  COUNT(CASE WHEN tipo_pago = 'TARDIO_PDP' THEN 1 END) as pagos_tardios_pdp,
  COUNT(CASE WHEN tipo_pago = 'POST_GESTION' THEN 1 END) as pagos_post_gestion,
  COUNT(CASE WHEN tipo_pago = 'ESPONTANEO' THEN 1 END) as pagos_espontaneos,
  
  -- Distribución por efectividad
  COUNT(CASE WHEN categoria_efectividad = 'ALTA' THEN 1 END) as efectividad_alta,
  COUNT(CASE WHEN categoria_efectividad = 'MEDIA' THEN 1 END) as efectividad_media,
  COUNT(CASE WHEN categoria_efectividad = 'BAJA' THEN 1 END) as efectividad_baja,
  COUNT(CASE WHEN categoria_efectividad = 'SIN_ATRIBUCION' THEN 1 END) as sin_atribucion,
  
  -- Métricas de atribución
  ROUND(AVG(efectividad_atribucion), 3) as score_efectividad_promedio,
  ROUND(COUNT(CASE WHEN tiene_gestion_previa THEN 1 END) / COUNT(*) * 100, 2) as pct_con_gestion_previa,
  ROUND(COUNT(CASE WHEN es_pago_con_pdp THEN 1 END) / COUNT(*) * 100, 2) as pct_con_pdp,
  
  -- Métricas de tiempo
  ROUND(AVG(dias_entre_gestion_y_pago), 1) as dias_promedio_gestion_pago,
  
  -- Cobertura de contexto
  ROUND(COUNT(CASE WHEN vencimiento IS NOT NULL THEN 1 END) / COUNT(*) * 100, 2) as pct_con_vencimiento
  
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
WHERE fecha_proceso = CURRENT_DATE();

-- ================================================================
-- ANÁLISIS DE EFECTIVIDAD POR CARTERA
-- ================================================================
SELECT 
  'EFECTIVIDAD_POR_CARTERA' as analisis,
  cartera,
  categoria_vencimiento,
  COUNT(*) as total_pagos,
  ROUND(SUM(monto_pagado), 2) as monto_total,
  ROUND(AVG(efectividad_atribucion), 3) as score_promedio,
  COUNT(CASE WHEN categoria_efectividad = 'ALTA' THEN 1 END) as alta_efectividad,
  ROUND(COUNT(CASE WHEN tiene_gestion_previa THEN 1 END) / COUNT(*) * 100, 2) as pct_con_gestion
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
WHERE fecha_proceso = CURRENT_DATE()
  AND cartera IS NOT NULL
GROUP BY cartera, categoria_vencimiento
ORDER BY cartera, categoria_vencimiento;

-- ================================================================
-- ANÁLISIS TEMPORAL: Distribución de días gestión -> pago
-- ================================================================
WITH distribucion_dias AS (
  SELECT 
    CASE 
      WHEN dias_entre_gestion_y_pago IS NULL THEN 'SIN_GESTION'
      WHEN dias_entre_gestion_y_pago = 0 THEN 'MISMO_DIA'
      WHEN dias_entre_gestion_y_pago <= 3 THEN '1_A_3_DIAS'
      WHEN dias_entre_gestion_y_pago <= 7 THEN '4_A_7_DIAS'
      WHEN dias_entre_gestion_y_pago <= 15 THEN '8_A_15_DIAS'
      WHEN dias_entre_gestion_y_pago <= 30 THEN '16_A_30_DIAS'
      ELSE 'MAS_30_DIAS'
    END as rango_dias,
    COUNT(*) as cantidad_pagos,
    ROUND(SUM(monto_pagado), 2) as monto_total,
    ROUND(AVG(efectividad_atribucion), 3) as score_promedio
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY rango_dias
)
SELECT 
  'DISTRIBUCION_TEMPORAL_GESTION_PAGO' as analisis,
  rango_dias,
  cantidad_pagos,
  monto_total,
  score_promedio,
  ROUND(cantidad_pagos / SUM(cantidad_pagos) OVER() * 100, 2) as porcentaje_del_total
FROM distribucion_dias
ORDER BY 
  CASE rango_dias
    WHEN 'SIN_GESTION' THEN 1
    WHEN 'MISMO_DIA' THEN 2
    WHEN '1_A_3_DIAS' THEN 3
    WHEN '4_A_7_DIAS' THEN 4
    WHEN '8_A_15_DIAS' THEN 5
    WHEN '16_A_30_DIAS' THEN 6
    WHEN 'MAS_30_DIAS' THEN 7
  END;

-- ================================================================
-- RESUMEN CONSOLIDADO DE TODOS LOS TESTS
-- ================================================================
WITH todos_los_tests AS (
  -- Este sería el UNION de todos los tests anteriores
  -- Por simplicidad, mostramos estructura
  SELECT 'TEST_PLACEHOLDER' as test_name, 0 as violaciones, 'PASS' as resultado
)
SELECT 
  'RESUMEN_TESTS_PAGOS' as resumen,
  COUNT(*) as total_tests,
  SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) as tests_passed,
  SUM(CASE WHEN resultado = 'FAIL' THEN 1 ELSE 0 END) as tests_failed,
  SUM(CASE WHEN resultado = 'WARN' THEN 1 ELSE 0 END) as tests_warnings,
  ROUND(SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as success_rate
FROM todos_los_tests
WHERE test_name != 'TEST_PLACEHOLDER';
