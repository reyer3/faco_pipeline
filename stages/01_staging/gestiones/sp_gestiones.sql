-- ================================================================
-- STORED PROCEDURE: Stage de Gestiones - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.0.0
-- Descripción: Procesamiento de gestiones unificadas BOT + HUMANO
--              con homologación de respuestas y operadores
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`(
  IN p_fecha_proceso DATE DEFAULT CURRENT_DATE(),
  IN p_canal_filter STRING DEFAULT NULL,  -- OPCIONAL: 'BOT', 'HUMANO' o NULL para ambos
  IN p_modo_ejecucion STRING DEFAULT 'INCREMENTAL' -- 'FULL' o 'INCREMENTAL'
)
BEGIN
  
  -- Variables de control
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_registros_procesados INT64 DEFAULT 0;
  DECLARE v_registros_nuevos INT64 DEFAULT 0;
  DECLARE v_registros_actualizados INT64 DEFAULT 0;
  DECLARE v_canales_detectados STRING DEFAULT '';
  DECLARE v_gestiones_bot INT64 DEFAULT 0;
  DECLARE v_gestiones_humano INT64 DEFAULT 0;
  
  -- ================================================================
  -- DETECCIÓN DE CANALES Y VOLÚMENES
  -- ================================================================
  
  -- Detectar gestiones BOT disponibles para la fecha
  SELECT COUNT(*)
  INTO v_gestiones_bot
  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = p_fecha_proceso
    AND SAFE_CAST(document AS INT64) IS NOT NULL;
  
  -- Detectar gestiones HUMANO disponibles para la fecha
  SELECT COUNT(*)
  INTO v_gestiones_humano
  FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = p_fecha_proceso
    AND SAFE_CAST(document AS INT64) IS NOT NULL;
  
  -- Construir resumen de canales detectados
  SET v_canales_detectados = CONCAT(
    'BOT: ', CAST(v_gestiones_bot AS STRING),
    ', HUMANO: ', CAST(v_gestiones_humano AS STRING)
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
    'stage_gestiones', 
    v_inicio_proceso,
    JSON_OBJECT(
      'fecha_proceso', CAST(p_fecha_proceso AS STRING),
      'canal_filter', IFNULL(p_canal_filter, 'TODOS'),
      'modo_ejecucion', p_modo_ejecucion,
      'gestiones_bot', v_gestiones_bot,
      'gestiones_humano', v_gestiones_humano
    ),
    'INICIADO',
    CONCAT('Canales detectados: ', v_canales_detectados)
  );
  
  -- ================================================================
  -- MERGE/UPSERT: Gestiones unificadas con homologación
  -- ================================================================
  
  MERGE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones` AS target
  USING (
    
    WITH gestiones_bot AS (
      SELECT 
        SAFE_CAST(document AS INT64) AS cod_luna,
        DATE(date) AS fecha_gestion,
        COALESCE(management, 'SIN_MANAGEMENT') AS management_original,
        '' AS sub_management_original,
        COALESCE(compromiso, '') AS compromiso_original,
        'SISTEMA_BOT' AS nombre_agente_original,
        CAST(0 AS FLOAT64) AS monto_compromiso,
        CAST(NULL AS DATE) AS fecha_compromiso,
        'BOT' AS canal_origen,
        date as timestamp_gestion
      FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
      WHERE SAFE_CAST(document AS INT64) IS NOT NULL
        AND DATE(date) = p_fecha_proceso
        AND DATE(date) >= '2025-01-01' -- Filtrar fechas erróneas
        AND (p_canal_filter IS NULL OR p_canal_filter = 'BOT')
        AND (p_modo_ejecucion = 'FULL' OR DATE(date) >= p_fecha_proceso)
    ),
    
    gestiones_humano AS (
      SELECT 
        SAFE_CAST(document AS INT64) AS cod_luna,
        DATE(date) AS fecha_gestion,
        COALESCE(management, 'SIN_MANAGEMENT') AS management_original,
        '' AS sub_management_original,
        '' AS compromiso_original,
        COALESCE(nombre_agente, 'SIN_AGENTE') AS nombre_agente_original,
        CAST(COALESCE(monto_compromiso, 0) AS FLOAT64) AS monto_compromiso,
        CAST(fecha_compromiso AS DATE) AS fecha_compromiso,
        'HUMANO' AS canal_origen,
        date as timestamp_gestion
      FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
      WHERE SAFE_CAST(document AS INT64) IS NOT NULL
        AND DATE(date) = p_fecha_proceso
        AND (p_canal_filter IS NULL OR p_canal_filter = 'HUMANO')
        AND (p_modo_ejecucion = 'FULL' OR DATE(date) >= p_fecha_proceso)
    ),
    
    gestiones_unificadas AS (
      SELECT * FROM gestiones_bot
      UNION ALL
      SELECT * FROM gestiones_humano
    ),
    
    gestiones_enriquecidas AS (
      SELECT
        -- 🔑 LLAVES PRIMARIAS
        g.cod_luna,
        g.fecha_gestion,
        g.canal_origen,
        ROW_NUMBER() OVER (
          PARTITION BY g.cod_luna, g.fecha_gestion, g.canal_origen 
          ORDER BY g.timestamp_gestion
        ) AS secuencia_gestion,
        
        -- 👥 DIMENSIONES DE OPERADOR
        g.nombre_agente_original,
        CASE 
          WHEN g.canal_origen = 'BOT' THEN 'SISTEMA_BOT'
          WHEN g.canal_origen = 'HUMANO' THEN COALESCE(h_user.usuario, g.nombre_agente_original, 'SIN_AGENTE')
          ELSE 'NO_IDENTIFICADO'
        END AS operador_final,
        
        -- 📞 DIMENSIONES DE GESTIÓN ORIGINALES
        g.management_original,
        g.sub_management_original,
        g.compromiso_original,
        
        -- 🎯 RESPUESTAS HOMOLOGADAS
        COALESCE(
          CASE
            WHEN g.canal_origen = 'BOT' THEN h_bot.contactabilidad_homologada
            WHEN g.canal_origen = 'HUMANO' THEN h_call.contactabilidad
          END,
          g.management_original,
          'NO_IDENTIFICADO'
        ) AS grupo_respuesta,
        
        COALESCE(
          CASE
            WHEN g.canal_origen = 'BOT' THEN h_bot.n1_homologado
            WHEN g.canal_origen = 'HUMANO' THEN h_call.n_1
          END,
          'SIN_N1'
        ) AS nivel_1,
        
        COALESCE(
          CASE
            WHEN g.canal_origen = 'BOT' THEN h_bot.n2_homologado
            WHEN g.canal_origen = 'HUMANO' THEN h_call.n_2
          END,
          'SIN_N2'
        ) AS nivel_2,
        
        -- 💰 COMPROMISOS Y MONTOS
        CASE
          WHEN g.canal_origen = 'BOT' THEN COALESCE(h_bot.es_pdp_homologado, 0) = 1
          WHEN g.canal_origen = 'HUMANO' THEN UPPER(COALESCE(h_call.pdp, '')) = 'SI'
          ELSE FALSE
        END AS es_compromiso,
        
        g.monto_compromiso,
        g.fecha_compromiso,
        
        -- 📊 FLAGS DE ANÁLISIS
        CASE 
          WHEN UPPER(g.management_original) LIKE '%CONTACTO_EFECTIVO%' 
               OR UPPER(g.management_original) LIKE '%EFECTIVO%' 
          THEN TRUE
          ELSE FALSE 
        END AS es_contacto_efectivo,
        
        ROW_NUMBER() OVER (
          PARTITION BY g.cod_luna, g.fecha_gestion 
          ORDER BY g.timestamp_gestion
        ) = 1 AS es_primera_gestion_dia,
        
        -- 🔗 REFERENCIAS A OTROS STAGES (calculadas después)
        CASE WHEN asig.cod_luna IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_asignacion,
        CASE WHEN deuda.cod_cuenta IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_deuda,
        
        -- Una gestión es medible si el cliente tiene asignación O deuda
        CASE 
          WHEN asig.cod_luna IS NOT NULL OR deuda.cod_cuenta IS NOT NULL THEN TRUE 
          ELSE FALSE 
        END AS es_gestion_medible,
        
        -- 📅 DIMENSIONES TEMPORALES CALCULADAS
        FORMAT_DATE('%A', g.fecha_gestion) AS dia_semana,
        EXTRACT(WEEK FROM g.fecha_gestion) - EXTRACT(WEEK FROM DATE_TRUNC(g.fecha_gestion, MONTH)) + 1 AS semana_mes,
        CASE 
          WHEN EXTRACT(DAYOFWEEK FROM g.fecha_gestion) IN (1, 7) THEN TRUE 
          ELSE FALSE 
        END AS es_fin_semana,
        
        -- 🕒 METADATOS
        CURRENT_TIMESTAMP() AS fecha_actualizacion,
        p_fecha_proceso AS fecha_proceso,
        v_inicio_proceso AS fecha_carga
        
      FROM gestiones_unificadas g
      
      -- 🔗 Homologación BOT
      LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` AS h_bot 
        ON g.canal_origen = 'BOT' 
        AND COALESCE(g.management_original, '') = h_bot.bot_management 
        AND COALESCE(g.sub_management_original, '') = h_bot.bot_sub_management 
        AND COALESCE(g.compromiso_original, '') = h_bot.bot_compromiso
      
      -- 🔗 Homologación HUMANO  
      LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` AS h_call 
        ON g.canal_origen = 'HUMANO' 
        AND COALESCE(g.management_original, '') = h_call.management
      
      -- 🔗 Homologación USUARIOS
      LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_usuarios` AS h_user 
        ON g.canal_origen = 'HUMANO' 
        AND g.nombre_agente_original = h_user.usuario
      
      -- 🔗 Join con ASIGNACIÓN para medibilidad
      LEFT JOIN `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion` AS asig
        ON g.cod_luna = asig.cod_luna
        AND g.fecha_gestion = asig.fecha_asignacion
      
      -- 🔗 Join con DEUDAS para medibilidad  
      LEFT JOIN (
        SELECT DISTINCT cod_cuenta, fecha_deuda
        FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
        WHERE fecha_proceso = p_fecha_proceso
      ) AS deuda
        ON CAST(g.cod_luna AS STRING) = deuda.cod_cuenta
        AND g.fecha_gestion = deuda.fecha_deuda
      
      WHERE g.cod_luna IS NOT NULL
    )
    
    SELECT * FROM gestiones_enriquecidas
    
  ) AS source
  
  ON target.cod_luna = source.cod_luna
     AND target.fecha_gestion = source.fecha_gestion
     AND target.canal_origen = source.canal_origen
     AND target.secuencia_gestion = source.secuencia_gestion
  
  -- 🔄 ACTUALIZAR REGISTROS EXISTENTES
  WHEN MATCHED THEN UPDATE SET
    target.grupo_respuesta = source.grupo_respuesta,
    target.nivel_1 = source.nivel_1,
    target.nivel_2 = source.nivel_2,
    target.es_compromiso = source.es_compromiso,
    target.monto_compromiso = source.monto_compromiso,
    target.es_gestion_medible = source.es_gestion_medible,
    target.fecha_actualizacion = source.fecha_actualizacion,
    target.fecha_proceso = source.fecha_proceso
  
  -- ➕ INSERTAR NUEVOS REGISTROS
  WHEN NOT MATCHED THEN INSERT (
    cod_luna, fecha_gestion, canal_origen, secuencia_gestion,
    nombre_agente_original, operador_final, management_original,
    sub_management_original, compromiso_original, grupo_respuesta,
    nivel_1, nivel_2, es_compromiso, monto_compromiso, fecha_compromiso,
    es_contacto_efectivo, es_primera_gestion_dia, tiene_asignacion,
    tiene_deuda, es_gestion_medible, dia_semana, semana_mes,
    es_fin_semana, fecha_actualizacion, fecha_proceso, fecha_carga
  )
  VALUES (
    source.cod_luna, source.fecha_gestion, source.canal_origen, source.secuencia_gestion,
    source.nombre_agente_original, source.operador_final, source.management_original,
    source.sub_management_original, source.compromiso_original, source.grupo_respuesta,
    source.nivel_1, source.nivel_2, source.es_compromiso, source.monto_compromiso,
    source.fecha_compromiso, source.es_contacto_efectivo, source.es_primera_gestion_dia,
    source.tiene_asignacion, source.tiene_deuda, source.es_gestion_medible,
    source.dia_semana, source.semana_mes, source.es_fin_semana,
    source.fecha_actualizacion, source.fecha_proceso, source.fecha_carga
  );
  
  -- ================================================================
  -- ESTADÍSTICAS Y LOGGING FINAL
  -- ================================================================
  
  SET v_registros_procesados = @@row_count;
  
  -- Obtener estadísticas detalladas
  SELECT COUNT(*) INTO v_registros_nuevos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
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
    'stage_gestiones', 
    v_inicio_proceso,
    CURRENT_TIMESTAMP(),
    v_registros_procesados,
    v_registros_nuevos,
    v_registros_actualizados,
    'COMPLETADO',
    CONCAT('Proceso completado. Canales: ', v_canales_detectados)
  );
  
  -- ================================================================
  -- RESUMEN DE NEGOCIO
  -- ================================================================
  
  -- Mostrar métricas de gestiones por canal
  SELECT 
    'RESUMEN_GESTIONES' as tipo,
    p_fecha_proceso as fecha_proceso,
    canal_origen,
    COUNT(*) as total_gestiones,
    COUNT(DISTINCT cod_luna) as clientes_unicos,
    COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) as contactos_efectivos,
    COUNT(CASE WHEN es_compromiso THEN 1 END) as compromisos,
    ROUND(SUM(monto_compromiso), 2) as monto_compromisos,
    COUNT(CASE WHEN es_gestion_medible THEN 1 END) as gestiones_medibles,
    COUNT(CASE WHEN es_primera_gestion_dia THEN 1 END) as primeras_gestiones_dia
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = p_fecha_proceso
  GROUP BY canal_origen
  ORDER BY canal_origen;
  
END;
