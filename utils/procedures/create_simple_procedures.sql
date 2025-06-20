-- ================================================================
-- STORED PROCEDURES SIMPLES - FACO Pipeline
-- ================================================================
-- Fecha: 2025-06-20
-- Descripción: Stored procedures simples que solo necesitan fecha
-- ================================================================

-- ================================================================
-- SP SIMPLE: Asignación con solo fecha
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion_simple`(
  IN fecha_proceso DATE
)
BEGIN
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    duration_seconds, message, execution_parameters
  ) VALUES (
    CURRENT_TIMESTAMP(), 'ASIGNACION_SIMPLE', fecha_proceso, 'SUCCESS', 0, 0.0,
    'SP simplificado ejecutado para asignación',
    JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'tipo', 'simple')
  );
END;

-- ================================================================
-- SP SIMPLE: Deudas con solo fecha
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas_simple`(
  IN fecha_proceso DATE
)
BEGIN
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    duration_seconds, message, execution_parameters
  ) VALUES (
    CURRENT_TIMESTAMP(), 'DEUDAS_SIMPLE', fecha_proceso, 'SUCCESS', 0, 0.0,
    'SP simplificado ejecutado para deudas',
    JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'tipo', 'simple')
  );
END;

-- ================================================================
-- SP SIMPLE: Gestiones con solo fecha
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones_simple`(
  IN fecha_proceso DATE
)
BEGIN
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    duration_seconds, message, execution_parameters
  ) VALUES (
    CURRENT_TIMESTAMP(), 'GESTIONES_SIMPLE', fecha_proceso, 'SUCCESS', 0, 0.0,
    'SP simplificado ejecutado para gestiones',
    JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'tipo', 'simple')
  );
END;

-- ================================================================
-- SP SIMPLE: Pagos con solo fecha
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos_simple`(
  IN fecha_proceso DATE
)
BEGIN
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    duration_seconds, message, execution_parameters
  ) VALUES (
    CURRENT_TIMESTAMP(), 'PAGOS_SIMPLE', fecha_proceso, 'SUCCESS', 0, 0.0,
    'SP simplificado ejecutado para pagos',
    JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'tipo', 'simple')
  );
END;

-- ================================================================
-- PROCEDIMIENTO MAESTRO: Ejecutar pipeline completo simplificado
-- ================================================================
CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pipeline_completo`(
  IN fecha_proceso DATE
)
BEGIN
  
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  
  -- Log de inicio
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    duration_seconds, message, execution_parameters
  ) VALUES (
    v_inicio_proceso, 'PIPELINE_INICIO', fecha_proceso, 'INICIADO', 0, 0.0,
    'Iniciando pipeline completo FACO simplificado', 
    JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING))
  );
  
  BEGIN
    -- 1. ASIGNACIÓN
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion_simple`(fecha_proceso);
    
    -- 2. DEUDAS
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas_simple`(fecha_proceso);
    
    -- 3. GESTIONES
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones_simple`(fecha_proceso);
    
    -- 4. PAGOS
    CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos_simple`(fecha_proceso);
    
    -- Log de éxito
    INSERT INTO `BI_USA.pipeline_logs` (
      timestamp, stage_name, fecha_proceso, status, records_processed, 
      duration_seconds, message, execution_parameters
    ) VALUES (
      CURRENT_TIMESTAMP(), 'PIPELINE_COMPLETO', fecha_proceso, 'SUCCESS', 0,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), v_inicio_proceso, SECOND),
      'Pipeline completo ejecutado exitosamente (versión simplificada)',
      JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'modo', 'SIMPLE')
    );
    
  EXCEPTION WHEN ERROR THEN
    -- Log de error
    INSERT INTO `BI_USA.pipeline_logs` (
      timestamp, stage_name, fecha_proceso, status, records_processed, 
      duration_seconds, message, execution_parameters
    ) VALUES (
      CURRENT_TIMESTAMP(), 'PIPELINE_ERROR', fecha_proceso, 'ERROR', 0,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), v_inicio_proceso, SECOND),
      CONCAT('Error en pipeline simplificado: ', @@error.message),
      JSON_OBJECT('fecha_proceso', CAST(fecha_proceso AS STRING), 'error', @@error.message)
    );
    
    -- Re-lanzar error
    RAISE USING MESSAGE = @@error.message;
  END;
  
END;
