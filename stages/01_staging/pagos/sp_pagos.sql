-- ================================================================
-- STORED PROCEDURE: Stage de Pagos - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.0.0
-- Descripción: Procesamiento de pagos con atribución de gestiones
--              y análisis de efectividad
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos`(
  IN p_fecha_inicio DATE,
  IN p_fecha_fin DATE,
  IN p_modo_ejecucion STRING
)
BEGIN

  -- Variables de control
  DECLARE v_inicio_proceso TIMESTAMP;
  DECLARE v_registros_procesados INT64;
  DECLARE v_registros_nuevos INT64;
  DECLARE v_registros_actualizados INT64;
  DECLARE v_pagos_detectados INT64;
  DECLARE v_periodo_analisis STRING;

  -- Inicialización
  SET v_inicio_proceso = CURRENT_TIMESTAMP();
  SET v_registros_procesados = 0;
  SET v_registros_nuevos = 0;
  SET v_registros_actualizados = 0;
  SET v_pagos_detectados = 0;
  SET v_periodo_analisis = ''; 
  
  -- Asignar valores por defecto si los parámetros son NULL
  SET p_fecha_inicio = IFNULL(p_fecha_inicio, DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));
  SET p_fecha_fin = IFNULL(p_fecha_fin, CURRENT_DATE());
  SET p_modo_ejecucion = IFNULL(p_modo_ejecucion, 'INCREMENTAL');

  
  -- ================================================================
  -- DETECCIÓN DE PAGOS EN EL PERÍODO
  -- ================================================================
  
  SET v_periodo_analisis = CONCAT(
    FORMAT_DATE('%d/%m/%Y', p_fecha_inicio), 
    ' - ', 
    FORMAT_DATE('%d/%m/%Y', p_fecha_fin)
  );
  
  -- Contar pagos en el período
    SET v_pagos_detectados = (
    SELECT COUNT(*)
    FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
    WHERE DATE(fecha_pago) BETWEEN p_fecha_inicio AND p_fecha_fin
  );

  
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
    'stage_pagos', 
    v_inicio_proceso,
    JSON_OBJECT(
      'fecha_inicio', CAST(p_fecha_inicio AS STRING),
      'fecha_fin', CAST(p_fecha_fin AS STRING),
      'modo_ejecucion', p_modo_ejecucion,
      'pagos_detectados', v_pagos_detectados
    ),
    'INICIADO',
    CONCAT('Procesando pagos del período: ', v_periodo_analisis, 
           '. Pagos detectados: ', CAST(v_pagos_detectados AS STRING))
  );
  
  -- ================================================================
  -- MERGE/UPSERT: Datos de pagos con atribución compleja
  -- ================================================================
  
  MERGE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos` AS target
  USING (
    
    -- CTE 1: Pagos con contexto de cartera
    WITH pagos_con_contexto AS (
      SELECT
        p.fecha_pago,
        p.monto_cancelado,
        p.nro_documento,
        
        -- Contexto desde asignación (última válida)
        asig.tipo_cartera AS cartera,
        asig.servicio,
        asig.fecha_vencimiento AS vencimiento,
        asig.categoria_vencimiento,
        asig.archivo AS id_archivo_asignacion,
        asig.cod_luna,
        asig.cod_cuenta,
        
        -- Control de última asignación
        ROW_NUMBER() OVER(
          PARTITION BY p.nro_documento 
          ORDER BY asig.fecha_asignacion DESC
        ) AS rn_ultima_asignacion,
        
        asig.fecha_asignacion
        
      FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos` p
      
      -- Join con deudas para obtener cod_cuenta
      INNER JOIN `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas` deuda 
        ON p.nro_documento = deuda.nro_documento
      
      -- Join con asignación para contexto de cartera
      INNER JOIN `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion` asig 
        ON deuda.cod_cuenta = asig.cod_cuenta
      
      WHERE DATE(p.fecha_pago) BETWEEN p_fecha_inicio AND p_fecha_fin
        AND (p_modo_ejecucion = 'FULL' OR DATE(p.fecha_pago) >= p_fecha_inicio)
    ),
    
    -- CTE 2: Pagos con gestión atribuida
    pagos_con_gestion_atribuida AS (
      SELECT
        p.*,
        
        -- Datos de gestión atribuida
        ges.fecha_gestion,
        ges.fecha_compromiso,
        ges.canal,
        ges.operador_final,
        ges.es_compromiso,
        ges.monto_compromiso,
        
        -- Control de última gestión
        ROW_NUMBER() OVER(
          PARTITION BY p.nro_documento 
          ORDER BY ges.fecha_gestion DESC
        ) AS rn_ultima_gestion
        
      FROM pagos_con_contexto p
      
      -- Join con gestiones (última antes del pago)
      LEFT JOIN `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones` ges 
        ON p.cod_luna = ges.cod_luna 
        AND DATE(ges.fecha_gestion) <= DATE(p.fecha_pago)
      
      WHERE p.rn_ultima_asignacion = 1
    ),
    
    -- CTE 3: Pagos procesados con métricas
    pagos_procesados AS (
      SELECT
        -- Llaves primarias
        nro_documento,
        DATE(fecha_pago) AS fecha_pago,
        
        -- Datos del pago
        monto_cancelado AS monto_pagado,
        
        -- Contexto de cartera
        cod_luna,
        cod_cuenta,
        cartera,
        servicio,
        vencimiento,
        categoria_vencimiento,
        id_archivo_asignacion,
        
        -- Atribución de gestión
        DATE(fecha_gestion) AS fecha_gestion_atribuida,
        COALESCE(canal, 'SIN_GESTION_PREVIA') AS canal_atribuido,
        COALESCE(operador_final, 'SIN_GESTION_PREVIA') AS operador_atribuido,
        
        -- Datos de compromiso
        DATE(fecha_compromiso) AS fecha_compromiso,
        monto_compromiso,
        
        -- Flags de análisis
        COALESCE(es_compromiso, FALSE) AS es_pago_con_pdp,
        
        -- PDP estaba vigente (compromiso + pago dentro de 7 días)
        CASE WHEN COALESCE(es_compromiso, FALSE) 
                  AND fecha_compromiso IS NOT NULL 
                  AND DATE(fecha_pago) BETWEEN DATE(fecha_compromiso) 
                  AND DATE_ADD(DATE(fecha_compromiso), INTERVAL 7 DAY) 
             THEN TRUE 
             ELSE FALSE 
        END AS pdp_estaba_vigente,
        
        -- Pago puntual (mismo día que compromiso)
        CASE WHEN COALESCE(es_compromiso, FALSE) 
                  AND fecha_compromiso IS NOT NULL 
                  AND DATE(fecha_pago) = DATE(fecha_compromiso) 
             THEN TRUE 
             ELSE FALSE 
        END AS pago_es_puntual,
        
        -- Tiene gestión previa
        CASE WHEN fecha_gestion IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_gestion_previa,
        
        -- Días entre gestión y pago
        CASE 
          WHEN fecha_gestion IS NOT NULL THEN 
            DATE_DIFF(DATE(fecha_pago), DATE(fecha_gestion), DAY)
          ELSE NULL 
        END AS dias_entre_gestion_y_pago,
        
        -- Score de efectividad de atribución
        CASE
          WHEN COALESCE(es_compromiso, FALSE) AND DATE(fecha_pago) = DATE(fecha_compromiso) THEN 1.0  -- Pago puntual
          WHEN COALESCE(es_compromiso, FALSE) AND DATE(fecha_pago) <= DATE_ADD(DATE(fecha_compromiso), INTERVAL 3 DAY) THEN 0.8  -- Dentro de 3 días
          WHEN COALESCE(es_compromiso, FALSE) AND DATE(fecha_pago) <= DATE_ADD(DATE(fecha_compromiso), INTERVAL 7 DAY) THEN 0.6  -- Dentro de semana
          WHEN fecha_gestion IS NOT NULL AND DATE_DIFF(DATE(fecha_pago), DATE(fecha_gestion), DAY) <= 7 THEN 0.4  -- Post-gestión semana
          WHEN fecha_gestion IS NOT NULL THEN 0.2  -- Hay gestión previa
          ELSE 0.0  -- Sin gestión atribuible
        END AS efectividad_atribucion,
        
        -- Clasificación de pago
        CASE
          WHEN COALESCE(es_compromiso, FALSE) AND DATE(fecha_pago) = DATE(fecha_compromiso) THEN 'PUNTUAL'
          WHEN COALESCE(es_compromiso, FALSE) AND DATE(fecha_pago) <= DATE_ADD(DATE(fecha_compromiso), INTERVAL 7 DAY) THEN 'TARDIO_PDP'
          WHEN fecha_gestion IS NOT NULL THEN 'POST_GESTION'
          ELSE 'ESPONTANEO'
        END AS tipo_pago,
        
        -- Metadatos
        fecha_asignacion AS fecha_ultima_asignacion,
        DATE(fecha_gestion) AS fecha_ultima_gestion,
        CURRENT_TIMESTAMP() AS fecha_actualizacion,
        CURRENT_DATE() AS fecha_proceso,
        v_inicio_proceso AS fecha_carga
        
      FROM pagos_con_gestion_atribuida
      WHERE rn_ultima_gestion = 1 OR rn_ultima_gestion IS NULL
    )
    
    -- Resultado final con categoría de efectividad
    SELECT
      *,
      -- Categoría de efectividad
      CASE
        WHEN efectividad_atribucion >= 0.8 THEN 'ALTA'
        WHEN efectividad_atribucion >= 0.4 THEN 'MEDIA'
        WHEN efectividad_atribucion > 0.0 THEN 'BAJA'
        ELSE 'SIN_ATRIBUCION'
      END AS categoria_efectividad
      
    FROM pagos_procesados
    
  ) AS source
  
  ON target.nro_documento = source.nro_documento
     AND target.fecha_pago = source.fecha_pago
  
  -- 🔄 ACTUALIZAR REGISTROS EXISTENTES
  WHEN MATCHED THEN UPDATE SET
    target.monto_pagado = source.monto_pagado,
    target.canal_atribuido = source.canal_atribuido,
    target.operador_atribuido = source.operador_atribuido,
    target.fecha_gestion_atribuida = source.fecha_gestion_atribuida,
    target.efectividad_atribucion = source.efectividad_atribucion,
    target.tipo_pago = source.tipo_pago,
    target.categoria_efectividad = source.categoria_efectividad,
    target.fecha_actualizacion = source.fecha_actualizacion,
    target.fecha_proceso = source.fecha_proceso
  
  -- ➕ INSERTAR NUEVOS REGISTROS
  WHEN NOT MATCHED THEN INSERT (
    nro_documento, fecha_pago, monto_pagado, cod_luna, cod_cuenta,
    cartera, servicio, vencimiento, categoria_vencimiento, id_archivo_asignacion,
    fecha_gestion_atribuida, canal_atribuido, operador_atribuido,
    fecha_compromiso, monto_compromiso, es_pago_con_pdp, pdp_estaba_vigente,
    pago_es_puntual, tiene_gestion_previa, dias_entre_gestion_y_pago,
    efectividad_atribucion, tipo_pago, categoria_efectividad,
    fecha_ultima_asignacion, fecha_ultima_gestion, fecha_actualizacion,
    fecha_proceso, fecha_carga
  )
  VALUES (
    source.nro_documento, source.fecha_pago, source.monto_pagado, source.cod_luna, source.cod_cuenta,
    source.cartera, source.servicio, source.vencimiento, source.categoria_vencimiento, source.id_archivo_asignacion,
    source.fecha_gestion_atribuida, source.canal_atribuido, source.operador_atribuido,
    source.fecha_compromiso, source.monto_compromiso, source.es_pago_con_pdp, source.pdp_estaba_vigente,
    source.pago_es_puntual, source.tiene_gestion_previa, source.dias_entre_gestion_y_pago,
    source.efectividad_atribucion, source.tipo_pago, source.categoria_efectividad,
    source.fecha_ultima_asignacion, source.fecha_ultima_gestion, source.fecha_actualizacion,
    source.fecha_proceso, source.fecha_carga
  );
  
  -- ================================================================
  -- ESTADÍSTICAS Y LOGGING FINAL
  -- ================================================================
  
  SET v_registros_procesados = @@row_count;
  
  -- Obtener estadísticas detalladas
  SET v_registros_nuevos = (
    SELECT COUNT(*) 
    FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
    WHERE DATE(fecha_carga) = DATE(v_inicio_proceso)
  );

  
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
    'stage_pagos', 
    v_inicio_proceso,
    CURRENT_TIMESTAMP(),
    v_registros_procesados,
    v_registros_nuevos,
    v_registros_actualizados,
    'COMPLETADO',
    CONCAT('Proceso completado. Período: ', v_periodo_analisis,
           '. Pagos detectados: ', CAST(v_pagos_detectados AS STRING))
  );
  
  -- ================================================================
  -- RESUMEN DE NEGOCIO (opcional, comentar en producción)
  -- ================================================================
  
  -- Mostrar métricas de atribución y efectividad
  SELECT 
    'RESUMEN_PAGOS_ATRIBUCION' as tipo,
    v_periodo_analisis as periodo_analisis,
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
    
    -- Métricas promedio
    ROUND(AVG(efectividad_atribucion), 3) as score_efectividad_promedio,
    ROUND(AVG(dias_entre_gestion_y_pago), 1) as dias_promedio_gestion_pago,
    
    -- Cobertura de atribución
    ROUND(COUNT(CASE WHEN tiene_gestion_previa THEN 1 END) / COUNT(*) * 100, 2) as pct_con_gestion_previa
    
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_pago BETWEEN p_fecha_inicio AND p_fecha_fin;
  
END;
