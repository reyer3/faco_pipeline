-- ================================================================
-- EJEMPLOS DE USO: Stage de Deudas con Lógica FECHA_TRANDEUDA
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.1.0
-- Descripción: Ejemplos prácticos que muestran la lógica corregida
--              de medibilidad basada en FECHA_TRANDEUDA
-- ================================================================

-- ================================================================
-- EJEMPLO 1: EJECUCIÓN AUTOMÁTICA DIARIA (RECOMENDADO)
-- ================================================================
-- Procesamiento automático que detecta coincidencias FECHA_TRANDEUDA

-- 1. Ejecutar asignación primero (prerequisito)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`();

-- 2. Ejecutar deudas con detección automática
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`();

-- ================================================================
-- EJEMPLO 2: PROCESAMIENTO DE FECHA ESPECÍFICA
-- ================================================================
-- Procesar fecha específica con análisis de coincidencias

CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(
  '2025-06-19'     -- fecha_proceso específica
);

-- ================================================================
-- EJEMPLO 3: ANÁLISIS DE COINCIDENCIAS FECHA_TRANDEUDA
-- ================================================================
-- Consulta para entender qué archivos coinciden con calendario

SELECT 
  '📊 ANÁLISIS_COINCIDENCIAS' as reporte,
  fecha_deuda_construida as fecha_archivo,
  fecha_trandeuda as fecha_calendario,
  COUNT(*) as total_deudas,
  ROUND(SUM(monto_exigible), 2) as monto_total,
  COUNT(CASE WHEN es_gestionable THEN 1 END) as gestionables,
  COUNT(CASE WHEN es_medible THEN 1 END) as medibles,
  ROUND(SUM(monto_medible), 2) as monto_medible,
  CASE 
    WHEN fecha_trandeuda IS NULL THEN '❌ SIN_CALENDARIO'
    WHEN fecha_deuda_construida = fecha_trandeuda THEN '✅ COINCIDE'
    ELSE '⚠️ NO_COINCIDE'
  END as estado_coincidencia
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = CURRENT_DATE()
GROUP BY fecha_deuda_construida, fecha_trandeuda
ORDER BY fecha_deuda_construida;

-- ================================================================
-- EJEMPLO 4: COMPARACIÓN LÓGICA ANTERIOR vs NUEVA
-- ================================================================
-- Comparar resultados entre lógica antigua y nueva

WITH comparacion AS (
  SELECT 
    cod_cuenta,
    monto_exigible,
    es_gestionable,
    es_dia_apertura,
    es_medible as medible_actual,
    monto_medible as monto_medible_actual,
    fecha_deuda_construida,
    fecha_trandeuda,
    
    -- Simular lógica anterior (día apertura)
    CASE WHEN es_gestionable AND es_dia_apertura THEN TRUE ELSE FALSE END as medible_anterior,
    CASE WHEN es_gestionable AND es_dia_apertura THEN monto_exigible ELSE 0 END as monto_medible_anterior,
    
    -- Estado de coincidencia
    CASE 
      WHEN fecha_trandeuda IS NULL THEN 'SIN_CALENDARIO'
      WHEN fecha_deuda_construida = fecha_trandeuda THEN 'COINCIDE_TRANDEUDA'
      ELSE 'NO_COINCIDE'
    END as estado_calendario
    
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
)
SELECT 
  '🔄 COMPARACIÓN_LÓGICAS' as reporte,
  estado_calendario,
  COUNT(*) as total_registros,
  SUM(CASE WHEN medible_anterior THEN 1 ELSE 0 END) as medibles_logica_anterior,
  SUM(CASE WHEN medible_actual THEN 1 ELSE 0 END) as medibles_logica_actual,
  ROUND(SUM(monto_medible_anterior), 2) as monto_anterior,
  ROUND(SUM(monto_medible_actual), 2) as monto_actual,
  ROUND(SUM(monto_medible_anterior) - SUM(monto_medible_actual), 2) as diferencia_monto
FROM comparacion
GROUP BY estado_calendario
ORDER BY estado_calendario;

-- ================================================================
-- EJEMPLO 5: DETECCIÓN DE PROBLEMAS COMUNES
-- ================================================================

-- 5.1 Archivos TRAN_DEUDA sin correspondencia en calendario
SELECT 
  '⚠️ ARCHIVOS_SIN_CALENDARIO' as alerta,
  fecha_deuda_construida,
  COUNT(*) as deudas_afectadas,
  ROUND(SUM(monto_exigible), 2) as monto_perdido,
  STRING_AGG(DISTINCT archivo, ', ') as archivos
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_trandeuda IS NULL 
  AND fecha_proceso = CURRENT_DATE()
GROUP BY fecha_deuda_construida
ORDER BY deudas_afectadas DESC;

-- 5.2 Fechas en calendario sin archivos TRAN_DEUDA
SELECT 
  '❓ CALENDARIO_SIN_ARCHIVOS' as alerta,
  cal.FECHA_TRANDEUDA,
  cal.ARCHIVO as cartera,
  'Posible archivo esperado: TRAN_DEUDA_' || FORMAT_DATE('%d%m', cal.FECHA_TRANDEUDA) as archivo_esperado
FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` cal
LEFT JOIN (
  SELECT DISTINCT fecha_trandeuda
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = CURRENT_DATE()
) deudas ON cal.FECHA_TRANDEUDA = deudas.fecha_trandeuda
WHERE cal.FECHA_ASIGNACION = CURRENT_DATE()
  AND deudas.fecha_trandeuda IS NULL;

-- ================================================================
-- EJEMPLO 6: MONITOREO DE COBERTURA POR PERÍODO
-- ================================================================
-- Análisis de cobertura de FECHA_TRANDEUDA en los últimos 7 días

SELECT 
  '📈 COBERTURA_SEMANAL' as reporte,
  fecha_proceso,
  COUNT(*) as total_deudas,
  COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) as con_calendario,
  ROUND(COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) / COUNT(*) * 100, 2) as pct_cobertura,
  COUNT(CASE WHEN es_medible THEN 1 END) as medibles,
  ROUND(COUNT(CASE WHEN es_medible THEN 1 END) / COUNT(*) * 100, 2) as pct_medible,
  ROUND(SUM(monto_medible), 2) as monto_medible_total
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY fecha_proceso
ORDER BY fecha_proceso DESC;

-- ================================================================
-- EJEMPLO 7: PIPELINE COMPLETO CON VALIDACIONES
-- ================================================================
-- Script completo que incluye ejecución y validaciones

-- Ejecutar pipeline en secuencia
DECLARE fecha_proceso DATE DEFAULT CURRENT_DATE();

-- 1. Asignación
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(fecha_proceso);

-- 2. Deudas
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(fecha_proceso);

-- 3. Validar resultados
SELECT 
  '✅ VALIDACIÓN_PIPELINE' as resultado,
  'Asignación' as stage,
  COUNT(*) as registros_procesados,
  COUNT(DISTINCT cod_luna) as clientes_unicos
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
WHERE fecha_proceso = fecha_proceso

UNION ALL

SELECT 
  '✅ VALIDACIÓN_PIPELINE' as resultado,
  'Deudas' as stage,
  COUNT(*) as registros_procesados,
  COUNT(DISTINCT cod_cuenta) as clientes_unicos
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = fecha_proceso;

-- ================================================================
-- EJEMPLO 8: TROUBLESHOOTING - CASOS ESPECÍFICOS
-- ================================================================

-- 8.1 Cliente gestionable pero no medible
SELECT 
  '🔍 GESTIONABLE_NO_MEDIBLE' as caso,
  cod_cuenta,
  monto_exigible,
  fecha_deuda_construida,
  fecha_trandeuda,
  es_gestionable,
  es_medible,
  'Gestionable pero sin coincidencia FECHA_TRANDEUDA' as motivo
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE es_gestionable = TRUE 
  AND es_medible = FALSE
  AND fecha_proceso = CURRENT_DATE()
LIMIT 10;

-- 8.2 Archivos con formato incorrecto
SELECT 
  '❌ FORMATO_ARCHIVO_INCORRECTO' as problema,
  archivo,
  COUNT(*) as registros_afectados,
  'No cumple patrón TRAN_DEUDA_DDMM' as descripcion
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE NOT REGEXP_CONTAINS(archivo, r'TRAN_DEUDA_\d{4}')
  AND fecha_proceso = CURRENT_DATE()
GROUP BY archivo;

-- ================================================================
-- EJEMPLO 9: MÉTRICAS DE NEGOCIO DETALLADAS
-- ================================================================
-- Dashboard completo de métricas por FECHA_TRANDEUDA

SELECT 
  '📊 DASHBOARD_MEDIBILIDAD' as dashboard,
  CURRENT_DATE() as fecha_reporte,
  
  -- Totales generales
  COUNT(*) as total_deudas,
  ROUND(SUM(monto_exigible), 2) as monto_total_exigible,
  
  -- Métricas de gestionabilidad
  COUNT(CASE WHEN es_gestionable THEN 1 END) as deudas_gestionables,
  ROUND(SUM(monto_gestionable), 2) as monto_gestionable,
  ROUND(COUNT(CASE WHEN es_gestionable THEN 1 END) / COUNT(*) * 100, 2) as pct_gestionable,
  
  -- Métricas de medibilidad (nueva lógica)
  COUNT(CASE WHEN es_medible THEN 1 END) as deudas_medibles_trandeuda,
  ROUND(SUM(monto_medible), 2) as monto_medible_trandeuda,
  ROUND(COUNT(CASE WHEN es_medible THEN 1 END) / COUNT(*) * 100, 2) as pct_medible,
  
  -- Cobertura de calendario
  COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) as deudas_con_calendario,
  ROUND(COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) / COUNT(*) * 100, 2) as pct_cobertura_calendario,
  
  -- Análisis de fechas
  COUNT(DISTINCT fecha_deuda_construida) as fechas_archivos_distintas,
  COUNT(DISTINCT fecha_trandeuda) as fechas_calendario_distintas
  
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = CURRENT_DATE();

-- ================================================================
-- EJEMPLO 10: SCRIPT DE MANTENIMIENTO Y CORRECCIÓN
-- ================================================================
-- Para casos donde se necesite corregir configuración de calendario

-- Identificar fechas de archivos que deberían tener FECHA_TRANDEUDA
WITH fechas_faltantes AS (
  SELECT DISTINCT 
    fecha_deuda_construida,
    FORMAT('TRAN_DEUDA_%s', FORMAT_DATE('%d%m', fecha_deuda_construida)) as archivo_esperado
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_trandeuda IS NULL
    AND fecha_proceso >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)
SELECT 
  '🔧 MANTENIMIENTO_CALENDARIO' as accion,
  fecha_deuda_construida as fecha_faltante,
  archivo_esperado,
  'Considerar agregar FECHA_TRANDEUDA al calendario' as recomendacion
FROM fechas_faltantes
ORDER BY fecha_deuda_construida DESC;

-- ================================================================
-- RESUMEN EJECUTIVO
-- ================================================================
SELECT 
  '📋 RESUMEN_EJECUTIVO_TRANDEUDA' as reporte,
  
  -- Cambio principal implementado
  'Medibilidad basada en coincidencia FECHA_TRANDEUDA' as cambio_principal,
  
  -- Impacto en métricas
  CONCAT(
    'Antes: ', 
    CAST(SUM(CASE WHEN es_gestionable AND es_dia_apertura THEN 1 ELSE 0 END) AS STRING),
    ' | Ahora: ',
    CAST(SUM(CASE WHEN es_medible THEN 1 ELSE 0 END) AS STRING)
  ) as comparacion_cantidad_medibles,
  
  -- Precisión mejorada
  ROUND(COUNT(CASE WHEN es_medible THEN 1 END) / COUNT(CASE WHEN es_gestionable THEN 1 END) * 100, 2) as precision_medibilidad,
  
  -- Recomendación
  CASE 
    WHEN COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) / COUNT(*) >= 0.8 
    THEN '✅ Buena cobertura calendario'
    ELSE '⚠️ Revisar configuración FECHA_TRANDEUDA en calendario'
  END as recomendacion
  
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = CURRENT_DATE();
