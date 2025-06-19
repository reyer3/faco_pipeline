-- ================================================================
-- STORED PROCEDURE: Stage de Deudas - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.0.0
-- Descripción: Procesamiento de deudas diarias con lógica de día de
--              apertura vs días subsiguientes y detección automática
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(
  IN p_fecha_proceso DATE DEFAULT CURRENT_DATE(),
  IN p_archivo_filter STRING DEFAULT NULL,  -- OPCIONAL: Si es NULL, detecta automáticamente
  IN p_modo_ejecucion STRING DEFAULT 'INCREMENTAL' -- 'FULL' o 'INCREMENTAL'
)
BEGIN
  
  -- Variables de control
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_registros_procesados INT64 DEFAULT 0;
  DECLARE v_registros_nuevos INT64 DEFAULT 0;
  DECLARE v_registros_actualizados INT64 DEFAULT 0;
  DECLARE v_archivos_detectados STRING DEFAULT '';
  DECLARE v_es_dia_apertura BOOLEAN DEFAULT FALSE;
  DECLARE v_carteras_abriendo STRING DEFAULT '';
  
  -- ================================================================
  -- DETECCIÓN DE DÍA DE APERTURA
  -- ================================================================
  
  -- Verificar si la fecha de proceso corresponde a apertura de alguna cartera
  SELECT 
    COUNT(*) > 0,
    STRING_AGG(ARCHIVO, ', ')
  INTO 
    v_es_dia_apertura,
    v_carteras_abriendo
  FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
  WHERE FECHA_ASIGNACION = p_fecha_proceso;
  
  -- ================================================================
  -- DETECCIÓN AUTOMÁTICA DE ARCHIVOS TRAN_DEUDA
  -- ================================================================
  
  IF p_archivo_filter IS NULL THEN
    -- Detectar archivos TRAN_DEUDA para la fecha específica
    -- Formato esperado: TRAN_DEUDA_DDMM
    DECLARE fecha_ddmm STRING DEFAULT FORMAT_DATE('%d%m', p_fecha_proceso);
    
    SELECT STRING_AGG(DISTINCT archivo, ', ')
    INTO v_archivos_detectados
    FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
    WHERE REGEXP_CONTAINS(archivo, CONCAT(r'TRAN_DEUDA_', fecha_ddmm))
      AND DATE(creado_el) = p_fecha_proceso;
    
    IF v_archivos_detectados IS NULL OR v_archivos_detectados = '' THEN
      SET v_archivos_detectados = CONCAT('No se encontraron archivos TRAN_DEUDA para ', FORMAT_DATE('%d/%m', p_fecha_proceso));
    END IF;
  ELSE
    SET v_archivos_detectados = CONCAT('Filtro manual: ', p_archivo_filter);
  END IF;
  
  -- ================================================================
  -- LOGGING: Inicio del proceso
  -- ================================================================
  INSERT INTO `BI_USA.pipeline_logs` (
    proceso, 
    etapa, 
    fecha_inicio, 
    parametros, 
    estado,
    observaciones
  ) 
  VALUES (
    'faco_pipeline', 
    'stage_deudas', 
    v_inicio_proceso,
    JSON_OBJECT(
      'fecha_proceso', CAST(p_fecha_proceso AS STRING),
      'archivo_filter', IFNULL(p_archivo_filter, 'AUTO_DETECT'),
      'modo_ejecucion', p_modo_ejecucion,
      'es_dia_apertura', v_es_dia_apertura,
      'carteras_abriendo', v_carteras_abriendo,
      'archivos_detectados', v_archivos_detectados
    ),
    'INICIADO',
    CONCAT('Día apertura: ', CAST(v_es_dia_apertura AS STRING), 
           '. Carteras: ', v_carteras_abriendo,
           '. Archivos: ', v_archivos_detectados)
  );
  
  -- ================================================================
  -- MERGE/UPSERT: Datos de deudas con lógica de negocio
  -- ================================================================
  
  MERGE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas` AS target
  USING (
    
    WITH deuda_con_fecha AS (
      SELECT 
        deuda.cod_cuenta, 
        deuda.nro_documento, 
        deuda.monto_exigible,
        deuda.archivo,
        deuda.creado_el,
        
        -- 📅 EXTRAER FECHA DEL NOMBRE DEL ARCHIVO TRAN_DEUDA_DDMM
        CASE 
          WHEN REGEXP_CONTAINS(deuda.archivo, r'TRAN_DEUDA_(\\d{4})') THEN
            SAFE.PARSE_DATE('%Y-%m-%d', 
              CONCAT(
                CAST(EXTRACT(YEAR FROM deuda.creado_el) AS STRING), '-',
                SUBSTR(REGEXP_EXTRACT(deuda.archivo, r'TRAN_DEUDA_(\\d{4})'), 3, 2), '-',
                SUBSTR(REGEXP_EXTRACT(deuda.archivo, r'TRAN_DEUDA_(\\d{4})'), 1, 2)
              )
            )
          ELSE DATE(deuda.creado_el)
        END AS fecha_deuda_construida
        
      FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda` AS deuda
      WHERE 
        -- Filtros de detección automática o manual
        (
          (p_archivo_filter IS NOT NULL AND deuda.archivo LIKE CONCAT('%', p_archivo_filter, '%'))
          OR
          (p_archivo_filter IS NULL AND DATE(deuda.creado_el) = p_fecha_proceso)
        )
        AND (p_modo_ejecucion = 'FULL' OR DATE(deuda.creado_el) >= p_fecha_proceso)
    ),
    
    deuda_enriquecida AS (
      SELECT
        -- 🔑 LLAVES PRIMARIAS
        deu.cod_cuenta,
        deu.nro_documento,
        deu.archivo,
        deu.fecha_deuda_construida AS fecha_deuda,
        
        -- 💰 DATOS DE DEUDA
        deu.monto_exigible,
        'ACTIVA' AS estado_deuda,
        
        -- 📅 DIMENSIONES TEMPORALES (desde calendario)
        deu.fecha_deuda_construida,
        cal.FECHA_ASIGNACION AS fecha_asignacion,
        cal.FECHA_CIERRE AS fecha_cierre,
        cal.DIAS_GESTION AS dias_gestion,
        
        -- 🎯 LÓGICA DE NEGOCIO ESPECÍFICA
        v_es_dia_apertura AS es_dia_apertura,
        
        -- Determinar si es gestionable (tiene asignación)
        CASE WHEN asig.cod_cuenta IS NOT NULL THEN TRUE ELSE FALSE END AS es_gestionable,
        
        -- Determinar si es medible (gestionable Y día de apertura)
        CASE 
          WHEN asig.cod_cuenta IS NOT NULL AND v_es_dia_apertura THEN TRUE 
          ELSE FALSE 
        END AS es_medible,
        
        -- Tipo de activación
        CASE
          WHEN v_es_dia_apertura THEN 'APERTURA'
          ELSE 'SUBSIGUIENTE'
        END AS tipo_activacion,
        
        -- 🔗 REFERENCIAS A ASIGNACIÓN
        asig.cod_luna,
        CASE WHEN asig.cod_cuenta IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_asignacion,
        asig.segmento_gestion,
        asig.tipo_cartera,
        
        -- 📊 MÉTRICAS CALCULADAS
        CASE 
          WHEN asig.cod_cuenta IS NOT NULL THEN deu.monto_exigible 
          ELSE 0 
        END AS monto_gestionable,
        
        CASE 
          WHEN asig.cod_cuenta IS NOT NULL AND v_es_dia_apertura THEN deu.monto_exigible 
          ELSE 0 
        END AS monto_medible,
        
        -- 🔢 FLAGS DE ANÁLISIS
        ROW_NUMBER() OVER (
          PARTITION BY deu.cod_cuenta 
          ORDER BY deu.fecha_deuda_construida, deu.creado_el
        ) AS secuencia_activacion,
        
        -- 🕒 METADATOS
        deu.creado_el,
        CURRENT_TIMESTAMP() AS fecha_actualizacion,
        p_fecha_proceso AS fecha_proceso,
        v_inicio_proceso AS fecha_carga
        
      FROM deuda_con_fecha AS deu
      
      -- Join con calendario para obtener información de cartera
      LEFT JOIN `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` AS cal
        ON deu.fecha_deuda_construida = cal.FECHA_ASIGNACION
      
      -- Join con asignación para determinar gestionabilidad
      LEFT JOIN `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion` AS asig
        ON deu.cod_cuenta = asig.cod_cuenta
        AND deu.fecha_deuda_construida = asig.fecha_asignacion
      
      WHERE deu.fecha_deuda_construida IS NOT NULL
    )
    
    SELECT * FROM deuda_enriquecida
    
  ) AS source
  
  ON target.cod_cuenta = source.cod_cuenta
     AND target.nro_documento = source.nro_documento
     AND target.archivo = source.archivo 
     AND target.fecha_deuda = source.fecha_deuda
  
  -- 🔄 ACTUALIZAR REGISTROS EXISTENTES
  WHEN MATCHED THEN UPDATE SET
    target.monto_exigible = source.monto_exigible,
    target.estado_deuda = source.estado_deuda,
    target.monto_gestionable = source.monto_gestionable,
    target.monto_medible = source.monto_medible,
    target.fecha_actualizacion = source.fecha_actualizacion,
    target.fecha_proceso = source.fecha_proceso
  
  -- ➕ INSERTAR NUEVOS REGISTROS
  WHEN NOT MATCHED THEN INSERT (
    cod_cuenta, nro_documento, archivo, fecha_deuda, monto_exigible, estado_deuda,
    fecha_deuda_construida, fecha_asignacion, fecha_cierre, dias_gestion,
    es_dia_apertura, es_gestionable, es_medible, tipo_activacion,
    cod_luna, tiene_asignacion, segmento_gestion, tipo_cartera,
    monto_gestionable, monto_medible, secuencia_activacion,
    creado_el, fecha_actualizacion, fecha_proceso, fecha_carga
  )
  VALUES (
    source.cod_cuenta, source.nro_documento, source.archivo, source.fecha_deuda,
    source.monto_exigible, source.estado_deuda, source.fecha_deuda_construida,
    source.fecha_asignacion, source.fecha_cierre, source.dias_gestion,
    source.es_dia_apertura, source.es_gestionable, source.es_medible, source.tipo_activacion,
    source.cod_luna, source.tiene_asignacion, source.segmento_gestion, source.tipo_cartera,
    source.monto_gestionable, source.monto_medible, source.secuencia_activacion,
    source.creado_el, source.fecha_actualizacion, source.fecha_proceso, source.fecha_carga
  );
  
  -- ================================================================
  -- ESTADÍSTICAS Y LOGGING FINAL
  -- ================================================================
  
  SET v_registros_procesados = @@row_count;
  
  -- Obtener estadísticas detalladas
  SELECT COUNT(*) INTO v_registros_nuevos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_carga = v_inicio_proceso;
  
  SET v_registros_actualizados = v_registros_procesados - v_registros_nuevos;
  
  -- Log final con métricas de negocio
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
    'stage_deudas', 
    v_inicio_proceso,
    CURRENT_TIMESTAMP(),
    v_registros_procesados,
    v_registros_nuevos,
    v_registros_actualizados,
    'COMPLETADO',
    CONCAT('Proceso completado. Día apertura: ', CAST(v_es_dia_apertura AS STRING),
           '. Archivos: ', v_archivos_detectados)
  );
  
  -- ================================================================
  -- RESUMEN DE NEGOCIO (opcional, comentar en producción)
  -- ================================================================
  
  -- Mostrar métricas de negocio del proceso
  SELECT 
    'RESUMEN_DEUDAS' as tipo,
    p_fecha_proceso as fecha_proceso,
    v_es_dia_apertura as es_dia_apertura,
    v_carteras_abriendo as carteras_abriendo,
    COUNT(*) as total_deudas,
    SUM(monto_exigible) as monto_total,
    SUM(CASE WHEN es_gestionable THEN 1 ELSE 0 END) as deudas_gestionables,
    SUM(monto_gestionable) as monto_gestionable_total,
    SUM(CASE WHEN es_medible THEN 1 ELSE 0 END) as deudas_medibles,
    SUM(monto_medible) as monto_medible_total,
    COUNT(DISTINCT cod_cuenta) as clientes_unicos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE fecha_proceso = p_fecha_proceso;
  
END;
