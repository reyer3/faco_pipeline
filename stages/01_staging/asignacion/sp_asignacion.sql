-- ================================================================
-- STORED PROCEDURE: Stage de Asignación - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.0.0
-- Descripción: Procesamiento y transformación de datos de asignación
--              para reportería de gestión de cobranzas
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  IN p_fecha_proceso DATE DEFAULT CURRENT_DATE(),
  IN p_archivo_filter STRING DEFAULT NULL,
  IN p_modo_ejecucion STRING DEFAULT 'INCREMENTAL' -- 'FULL' o 'INCREMENTAL'
)
BEGIN
  
  -- Variables de control
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_registros_procesados INT64 DEFAULT 0;
  DECLARE v_registros_nuevos INT64 DEFAULT 0;
  DECLARE v_registros_actualizados INT64 DEFAULT 0;
  
  -- ================================================================
  -- LOGGING: Inicio del proceso
  -- ================================================================
  INSERT INTO `BI_USA.pipeline_logs` (
    proceso, 
    etapa, 
    fecha_inicio, 
    parametros, 
    estado
  ) 
  VALUES (
    'faco_pipeline', 
    'stage_asignacion', 
    v_inicio_proceso,
    JSON_OBJECT(
      'fecha_proceso', CAST(p_fecha_proceso AS STRING),
      'archivo_filter', IFNULL(p_archivo_filter, 'ALL'),
      'modo_ejecucion', p_modo_ejecucion
    ),
    'INICIADO'
  );
  
  -- ================================================================
  -- MERGE/UPSERT: Datos de asignación
  -- ================================================================
  
  MERGE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion` AS target
  USING (
    
    SELECT
      -- 🔑 LLAVES PRIMARIAS
      asig.cod_luna,
      asig.cuenta AS cod_cuenta,
      COALESCE(cal.ARCHIVO, asig.archivo, 'SIN_ARCHIVO') AS archivo,
      
      -- 📊 DIMENSIONES CLIENTE
      asig.cliente,
      asig.telefono,
      COALESCE(asig.negocio, 'SIN_SERVICIO') AS servicio,
      COALESCE(asig.tramo_gestion, 'SIN_SEGMENTO') AS segmento_gestion,
      COALESCE(asig.zona, 'SIN_ZONA') AS zona_geografica,
      
      -- 📅 DIMENSIONES TEMPORALES
      COALESCE(asig.min_vto, DATE('1900-01-01')) AS fecha_vencimiento,
      cal.FECHA_ASIGNACION,
      cal.FECHA_CIERRE,
      cal.FECHA_TRANDEUDA,
      cal.DIAS_GESTION,
      
      -- 🎯 CATEGORIZACIÓN DE VENCIMIENTO
      CASE
        WHEN asig.min_vto IS NULL THEN 'SIN_VENCIMIENTO'
        WHEN asig.min_vto <= CURRENT_DATE() THEN 'VENCIDO'
        WHEN asig.min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'POR_VENCER_30D'
        WHEN asig.min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 60 DAY) THEN 'POR_VENCER_60D'
        WHEN asig.min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'POR_VENCER_90D'
        ELSE 'VIGENTE_MAS_90D'
      END AS categoria_vencimiento,
      
      -- 📁 TIPIFICACIÓN DE CARTERA
      CASE
        WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'TEMPRANA') THEN 'TEMPRANA'
        WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'CF_ANN') THEN 'CUOTA_FRACCIONAMIENTO'
        WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'AN') THEN 'ALTAS_NUEVAS'
        ELSE 'OTRAS'
      END AS tipo_cartera,
      
      -- 🎯 OBJETIVO DE RECUPERO
      CASE
        WHEN COALESCE(asig.tramo_gestion, '') = 'AL VCTO' THEN 0.15
        WHEN COALESCE(asig.tramo_gestion, '') = 'ENTRE 4 Y 15D' THEN 0.25
        WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'TEMPRANA') THEN 0.20
        ELSE 0.20
      END AS objetivo_recupero,
      
      -- 💰 FRACCIONAMIENTO
      CASE 
        WHEN COALESCE(asig.fraccionamiento, 'NO') = 'SI' THEN 'FRACCIONADO' 
        ELSE 'NORMAL' 
      END AS tipo_fraccionamiento,
      
      -- 🔢 FLAGS DE ANÁLISIS
      ROW_NUMBER() OVER (
        PARTITION BY asig.cod_luna, COALESCE(cal.ARCHIVO, asig.archivo) 
        ORDER BY asig.creado_el ASC
      ) AS flag_cliente_unico,
      
      -- 📊 CAMPOS DE GESTIÓN (por implementar)
      NULL AS saldo_dia,
      'ACTIVO' AS estado_cartera,
      
      -- 🕒 METADATOS
      CURRENT_TIMESTAMP() AS fecha_actualizacion,
      p_fecha_proceso AS fecha_proceso,
      v_inicio_proceso AS fecha_carga
      
    FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` AS asig
    INNER JOIN `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` AS cal
      ON asig.archivo = CONCAT(cal.ARCHIVO, '.txt')
    
    WHERE 
      (p_archivo_filter IS NULL OR asig.archivo LIKE CONCAT('%', p_archivo_filter, '%'))
      AND (p_modo_ejecucion = 'FULL' OR cal.FECHA_ASIGNACION >= p_fecha_proceso)
      
  ) AS source
  
  ON target.cod_luna = source.cod_luna
     AND target.cod_cuenta = source.cod_cuenta  
     AND target.archivo = source.archivo
  
  -- 🔄 ACTUALIZAR REGISTROS EXISTENTES
  WHEN MATCHED THEN UPDATE SET
    target.estado_cartera = source.estado_cartera,
    target.saldo_dia = source.saldo_dia,
    target.fecha_actualizacion = source.fecha_actualizacion,
    target.fecha_proceso = source.fecha_proceso
  
  -- ➕ INSERTAR NUEVOS REGISTROS
  WHEN NOT MATCHED THEN INSERT (
    cod_luna, cod_cuenta, archivo, cliente, telefono, servicio,
    segmento_gestion, zona_geografica, fecha_vencimiento,
    fecha_asignacion, fecha_cierre, fecha_trandeuda, dias_gestion,
    categoria_vencimiento, tipo_cartera, objetivo_recupero,
    tipo_fraccionamiento, flag_cliente_unico, saldo_dia,
    estado_cartera, fecha_actualizacion, fecha_proceso, fecha_carga
  )
  VALUES (
    source.cod_luna, source.cod_cuenta, source.archivo, source.cliente,
    source.telefono, source.servicio, source.segmento_gestion,
    source.zona_geografica, source.fecha_vencimiento, source.fecha_asignacion,
    source.fecha_cierre, source.fecha_trandeuda, source.dias_gestion,
    source.categoria_vencimiento, source.tipo_cartera, source.objetivo_recupero,
    source.tipo_fraccionamiento, source.flag_cliente_unico, source.saldo_dia,
    source.estado_cartera, source.fecha_actualizacion, source.fecha_proceso,
    source.fecha_carga
  );
  
  -- ================================================================
  -- ESTADÍSTICAS Y LOGGING FINAL
  -- ================================================================
  
  SET v_registros_procesados = @@row_count;
  
  -- Obtener estadísticas detalladas
  SELECT COUNT(*) INTO v_registros_nuevos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
  WHERE fecha_carga = v_inicio_proceso;
  
  SET v_registros_actualizados = v_registros_procesados - v_registros_nuevos;
  
  -- Log final
  INSERT INTO `BI_USA.pipeline_logs` (
    proceso, 
    etapa, 
    fecha_inicio,
    fecha_fin,
    registros_procesados,
    registros_nuevos,
    registros_actualizados,
    estado,
    observaciones
  ) 
  VALUES (
    'faco_pipeline', 
    'stage_asignacion', 
    v_inicio_proceso,
    CURRENT_TIMESTAMP(),
    v_registros_procesados,
    v_registros_nuevos,
    v_registros_actualizados,
    'COMPLETADO',
    CONCAT('Proceso completado exitosamente. Modo: ', p_modo_ejecucion)
  );
  
END;
