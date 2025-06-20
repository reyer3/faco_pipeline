-- ================================================================
-- STORED PROCEDURES WRAPPER - FACO Pipeline
-- ================================================================
-- Fecha: 2025-06-20
-- Descripción: Procedimientos wrapper para simplificar ejecución
--              con manejo de parámetros default
-- ================================================================

-- ================================================================
-- WRAPPER: Asignación con parámetros default
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion_simple`(
  IN fecha_proceso DATE
)
BEGIN
  -- Llamar al SP principal con valores default
  CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
    fecha_proceso,
    NULL,  -- archivo_filter = NULL (detección automática)
    'INCREMENTAL'  -- modo por defecto
  );
END;

-- ================================================================
-- WRAPPER: Deudas con parámetros default  
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas_simple`(
  IN fecha_proceso DATE
)
BEGIN
  -- Llamar al SP principal con valores default
  CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(
    fecha_proceso,
    NULL,  -- archivo_filter = NULL (detección automática)
    'INCREMENTAL'  -- modo por defecto
  );
END;

-- ================================================================
-- WRAPPER: Gestiones con parámetros default
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones_simple`(
  IN fecha_proceso DATE
)
BEGIN
  -- Llamar al SP principal con valores default
  CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`(
    fecha_proceso,
    NULL,  -- canal_filter = NULL (ambos canales)
    'INCREMENTAL'  -- modo por defecto
  );
END;

-- ================================================================
-- WRAPPER: Pagos con parámetros default
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos_simple`(
  IN fecha_proceso DATE
)
BEGIN
  -- Llamar al SP principal con valores default
  CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos`(
    fecha_proceso,
    NULL,  -- archivo_filter = NULL (detección automática)
    'INCREMENTAL'  -- modo por defecto
  );
END;

-- ================================================================
-- PROCEDIMIENTO MAESTRO: Ejecutar pipeline completo
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pipeline_completo`(
  IN fecha_proceso DATE
)
BEGIN
  
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_error_message STRING;
  
  -- Log de inicio
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    duration_seconds, message, execution_parameters
  ) VALUES (
    v_inicio_proceso, 'PIPELINE_INICIO', fecha_proceso, 'INICIADO', 0, 0.0,
    'Iniciando pipeline completo FACO', JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING))
  );
  
  BEGIN
    -- 1. ASIGNACIÓN
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion_simple`(fecha_proceso);
    
    -- 2. DEUDAS (requiere asignación)
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas_simple`(fecha_proceso);
    
    -- 3. GESTIONES (requiere asignación y deudas)
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones_simple`(fecha_proceso);
    
    -- 4. PAGOS (requiere todos los anteriores)
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos_simple`(fecha_proceso);
    
    -- Log de éxito
    INSERT INTO `BI_USA.pipeline_logs` (
      timestamp, stage_name, fecha_proceso, status, records_processed, 
      duration_seconds, message, execution_parameters
    ) VALUES (
      CURRENT_TIMESTAMP(), 'PIPELINE_COMPLETO', fecha_proceso, 'SUCCESS', 0,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), v_inicio_proceso, SECOND),
      'Pipeline completo ejecutado exitosamente',
      JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'modo', 'AUTO')
    );
    
  EXCEPTION WHEN ERROR THEN
    SET v_error_message = @@error.message;
    
    -- Log de error
    INSERT INTO `BI_USA.pipeline_logs` (
      timestamp, stage_name, fecha_proceso, status, records_processed, 
      duration_seconds, message, execution_parameters
    ) VALUES (
      CURRENT_TIMESTAMP(), 'PIPELINE_ERROR', fecha_proceso, 'ERROR', 0,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), v_inicio_proceso, SECOND),
      CONCAT('Error en pipeline: ', v_error_message),
      JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'error', v_error_message)
    );
    
    -- Re-lanzar error
    RAISE USING MESSAGE = v_error_message;
  END;
  
END;
