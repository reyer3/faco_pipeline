-- ================================================================
-- EJEMPLOS DE USO: Stage de Asignación con Detección Automática
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.1.0
-- Descripción: Ejemplos prácticos de ejecución del stored procedure
--              con diferentes configuraciones y escenarios
-- ================================================================

-- ================================================================
-- EJEMPLO 1: EJECUCIÓN AUTOMÁTICA DIARIA (RECOMENDADO)
-- ================================================================
-- Procesamiento automático de todos los archivos del día actual
-- El sistema detecta automáticamente qué archivos corresponden a la fecha

CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`();

-- Equivalente explícito:
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  CURRENT_DATE(),  -- fecha_proceso: hoy
  NULL,            -- archivo_filter: AUTO_DETECT
  'INCREMENTAL'    -- modo_ejecucion: solo nuevos/modificados
);

-- ================================================================
-- EJEMPLO 2: PROCESAMIENTO DE FECHA ESPECÍFICA
-- ================================================================
-- Procesar automáticamente todos los archivos de una fecha específica

CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  '2025-06-18'     -- fecha_proceso: ayer
);

-- ================================================================
-- EJEMPLO 3: FILTRO MANUAL POR TIPO DE CARTERA
-- ================================================================
-- Procesar solo archivos que contengan 'TEMPRANA' en el nombre

CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  CURRENT_DATE(),
  'TEMPRANA',      -- archivo_filter: filtro manual
  'INCREMENTAL'
);

-- Otros filtros específicos:
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  CURRENT_DATE(),
  'CF_ANN'         -- Solo cuotas fraccionamiento
);

CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  CURRENT_DATE(),
  'AN'             -- Solo altas nuevas
);

-- ================================================================
-- EJEMPLO 4: REPROCESAMIENTO COMPLETO (FULL REFRESH)
-- ================================================================
-- Reprocesar completamente todo el histórico disponible

CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  CURRENT_DATE(),
  NULL,            -- AUTO_DETECT
  'FULL'           -- Reprocesa todo
);

-- ================================================================
-- EJEMPLO 5: PROCESAMIENTO DE MÚLTIPLES DÍAS
-- ================================================================
-- Script para reprocesar varios días consecutivos

DECLARE fecha_inicio DATE DEFAULT '2025-06-15';
DECLARE fecha_fin DATE DEFAULT '2025-06-19';
DECLARE fecha_actual DATE;

SET fecha_actual = fecha_inicio;

-- Loop para procesar cada día
WHILE fecha_actual <= fecha_fin DO
  
  -- Procesar la fecha actual
  CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
    fecha_actual,
    NULL,              -- AUTO_DETECT por fecha
    'INCREMENTAL'
  );
  
  -- Avanzar al siguiente día
  SET fecha_actual = DATE_ADD(fecha_actual, INTERVAL 1 DAY);
  
END WHILE;

-- ================================================================
-- EJEMPLO 6: VERIFICACIÓN DE ARCHIVOS DISPONIBLES
-- ================================================================
-- Consulta para ver qué archivos están disponibles por fecha

SELECT 
  FECHA_ASIGNACION,
  COUNT(*) as total_archivos,
  STRING_AGG(ARCHIVO, ', ') as archivos_disponibles
FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
WHERE FECHA_ASIGNACION >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY FECHA_ASIGNACION
ORDER BY FECHA_ASIGNACION DESC;

-- ================================================================
-- EJEMPLO 7: MONITOREO POST-EJECUCIÓN
-- ================================================================
-- Consultar los resultados del último procesamiento

SELECT 
  proceso,
  etapa,
  fecha_inicio,
  fecha_fin,
  duracion_segundos,
  registros_procesados,
  registros_nuevos,
  registros_actualizados,
  estado,
  JSON_EXTRACT_SCALAR(parametros, '$.archivos_detectados') as archivos_detectados,
  observaciones
FROM `BI_USA.pipeline_logs`
WHERE proceso = 'faco_pipeline' 
  AND etapa = 'stage_asignacion'
ORDER BY fecha_inicio DESC
LIMIT 5;

-- ================================================================
-- EJEMPLO 8: VALIDACIÓN DE CALIDAD POST-PROCESO
-- ================================================================
-- Ejecutar tests de calidad después del procesamiento

-- Test de unicidad de llaves
SELECT 
  'DUPLICADOS_DETECTADOS' as test,
  COUNT(*) as cantidad
FROM (
  SELECT cod_luna, cod_cuenta, archivo, COUNT(*) as duplicados
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE fecha_proceso = CURRENT_DATE()
  GROUP BY cod_luna, cod_cuenta, archivo
  HAVING COUNT(*) > 1
);

-- Resumen de registros procesados hoy
SELECT 
  COUNT(*) as total_registros,
  COUNT(DISTINCT cod_luna) as clientes_unicos,
  COUNT(DISTINCT archivo) as archivos_procesados,
  COUNT(DISTINCT tipo_cartera) as tipos_cartera,
  AVG(objetivo_recupero) as objetivo_promedio
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
WHERE fecha_proceso = CURRENT_DATE();

-- ================================================================
-- EJEMPLO 9: TROUBLESHOOTING - ARCHIVOS SIN PROCESAR
-- ================================================================
-- Identificar archivos en calendario que no fueron procesados

WITH archivos_calendario AS (
  SELECT ARCHIVO
  FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
  WHERE FECHA_ASIGNACION = CURRENT_DATE()
),
archivos_procesados AS (
  SELECT DISTINCT archivo
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE fecha_proceso = CURRENT_DATE()
)
SELECT 
  cal.ARCHIVO as archivo_calendario,
  CASE WHEN proc.archivo IS NOT NULL THEN 'PROCESADO' ELSE 'PENDIENTE' END as estado
FROM archivos_calendario cal
LEFT JOIN archivos_procesados proc ON cal.ARCHIVO = proc.archivo;

-- ================================================================
-- EJEMPLO 10: SCRIPT DE MANTENIMIENTO SEMANAL
-- ================================================================
-- Rutina de mantenimiento para ejecutar semanalmente

-- 1. Limpiar logs antiguos (más de 90 días)
CALL `BI_USA.sp_limpiar_logs_antiguos`(90);

-- 2. Reprocesar la semana completa en modo INCREMENTAL
DECLARE fecha_semana_inicio DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
DECLARE i INT64 DEFAULT 0;

WHILE i < 7 DO
  CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
    DATE_ADD(fecha_semana_inicio, INTERVAL i DAY),
    NULL,
    'INCREMENTAL'
  );
  SET i = i + 1;
END WHILE;

-- 3. Generar reporte de calidad semanal
SELECT 
  'REPORTE_SEMANAL' as tipo,
  DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) as fecha_inicio,
  CURRENT_DATE() as fecha_fin,
  COUNT(*) as total_registros,
  COUNT(DISTINCT cod_luna) as clientes_unicos,
  COUNT(DISTINCT archivo) as archivos_procesados,
  ROUND(AVG(objetivo_recupero), 3) as objetivo_promedio
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
WHERE fecha_proceso >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
