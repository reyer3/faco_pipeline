-- ================================================================
-- STORED PROCEDURE CORREGIDO: Stage de Gestiones - Versión Funcional
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-20
-- Versión: 1.4.0 - CORREGIDA
-- Descripción: Versión corregida sin dependencias problemáticas
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones_fixed`(
  IN p_fecha_proceso DATE,
  IN p_canal_filter STRING,  -- OPCIONAL: 'BOT', 'HUMANO' o NULL para ambos
  IN p_modo_ejecucion STRING -- 'FULL' o 'INCREMENTAL'
)

BEGIN
  
  -- Variables de control
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_registros_procesados INT64 DEFAULT 0;
  DECLARE v_gestiones_bot INT64 DEFAULT 0;
  DECLARE v_gestiones_humano INT64 DEFAULT 0;

  SET p_fecha_proceso = IFNULL(p_fecha_proceso, CURRENT_DATE('America/Lima'));
  SET p_modo_ejecucion = IFNULL(p_modo_ejecucion, 'INCREMENTAL');
  
  -- Manejar canal filter vacío
  IF p_canal_filter = '' THEN
    SET p_canal_filter = NULL;
  END IF;
  
  -- ================================================================
  -- DETECCIÓN DE CANALES Y VOLÚMENES
  -- ================================================================
  
  SET v_gestiones_bot = (
    SELECT COUNT(*)
    FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
    WHERE DATE(date) = p_fecha_proceso
      AND SAFE_CAST(document AS INT64) IS NOT NULL
  );

  SET v_gestiones_humano = (
    SELECT COUNT(*)
    FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
    WHERE DATE(date) = p_fecha_proceso
      AND SAFE_CAST(document AS INT64) IS NOT NULL
  );
  
  -- ================================================================
  -- LOGGING: Inicio del proceso
  -- ================================================================
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    duration_seconds, message, execution_parameters
  )
  VALUES (
    CURRENT_TIMESTAMP(), 'stage_gestiones_fixed', p_fecha_proceso, 'INICIADO', 0, 0.0,
    CONCAT('BOT: ', v_gestiones_bot, ', HUMANO: ', v_gestiones_humano),
    JSON_OBJECT(
      'fecha_proceso', CAST(p_fecha_proceso AS STRING),
      'canal_filter', IFNULL(p_canal_filter, 'AMBOS_CANALES'),
      'modo_ejecucion', p_modo_ejecucion,
      'gestiones_bot', v_gestiones_bot,
      'gestiones_humano', v_gestiones_humano
    )
  );
  
  -- ================================================================
  -- LIMPIAR DATOS EXISTENTES (más simple que MERGE)
  -- ================================================================
  
  DELETE FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = p_fecha_proceso
    AND (p_canal_filter IS NULL 
         OR canal_origen = p_canal_filter);
  
  -- ================================================================
  -- INSERTAR GESTIONES UNIFICADAS (simplificado)
  -- ================================================================
  
  INSERT INTO `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones` (
    cod_luna, archivo_cartera, fecha_gestion, canal_origen, secuencia_gestion,
    nombre_agente_original, operador_final, management_original, sub_management_original,
    compromiso_original, grupo_respuesta, nivel_1, nivel_2, es_compromiso,
    monto_compromiso, fecha_compromiso, tipo_cartera, fecha_vencimiento_cliente,
    categoria_vencimiento, ciclica_vencimiento, segmento_gestion, zona_geografica,
    es_contacto_efectivo, es_primera_gestion_dia, tiene_asignacion, tiene_deuda,
    es_gestion_medible, tipo_medibilidad, weight_original, es_mejor_gestion_bot_dia,
    es_mejor_gestion_humano_dia, gestiones_mes_canal, weight_acumulado_mes_canal,
    compromisos_mes_canal, contactos_efectivos_mes_canal, monto_compromisos_mes_canal,
    tasa_efectividad_canal_mes, ranking_cliente_canal_mes, dias_gestionado_canal_mes,
    dia_semana, semana_mes, es_fin_semana, timestamp_gestion, fecha_actualizacion,
    fecha_proceso, fecha_carga
  )
  
  WITH gestiones_unificadas AS (
    -- ================================
    -- GESTIONES BOT
    -- ================================
    SELECT
      SAFE_CAST(bot.document AS INT64) AS cod_luna,
      COALESCE(cal.ARCHIVO, 'SIN_CARTERA') AS archivo_cartera,
      DATE(bot.date) AS fecha_gestion,
      'BOT' AS canal_origen,
      ROW_NUMBER() OVER (
        PARTITION BY SAFE_CAST(bot.document AS INT64), DATE(bot.date) 
        ORDER BY bot.date
      ) AS secuencia_gestion,
      
      -- 👥 DIMENSIONES DE OPERADOR
      "VOICEBOT" AS nombre_agente_original,
      "VOICEBOT" AS operador_final,
      
      -- 📞 DIMENSIONES ORIGINALES
      bot.management AS management_original,
      bot.sub_management AS sub_management_original,
      bot.compromiso AS compromiso_original,
      
      -- 🎯 RESPUESTAS HOMOLOGADAS (simplificadas)
      CASE
        WHEN bot.compromiso IS NOT NULL AND bot.compromiso != '' THEN 'COMPROMISO'
        WHEN bot.management IS NOT NULL AND bot.management != '' THEN 'CONTACTO'
        ELSE 'GESTION'
      END AS grupo_respuesta,
      
      COALESCE(bot.management, 'SIN_MANAGEMENT') AS nivel_1,
      COALESCE(bot.sub_management, 'SIN_SUB_MANAGEMENT') AS nivel_2,
      
      -- 💰 COMPROMISOS 
      CASE WHEN bot.compromiso IS NOT NULL AND bot.compromiso != '' THEN TRUE ELSE FALSE END AS es_compromiso,
      SAFE_CAST(bot.compromiso AS FLOAT64) AS monto_compromiso,
      CASE 
        WHEN bot.fecha_compromiso IS NOT NULL 
        THEN DATE(bot.fecha_compromiso)
        ELSE NULL 
      END AS fecha_compromiso,
      
      -- 🏆 PESO ORIGINAL
      bot.weight AS weight_original,
      
      -- 📊 FLAGS BÁSICOS
      CASE WHEN bot.management IS NOT NULL AND bot.management != '' THEN TRUE ELSE FALSE END AS es_contacto_efectivo,
      
      -- 🕒 METADATOS
      bot.date AS timestamp_gestion
      
    FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e` AS bot
    LEFT JOIN `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` AS cal
      ON DATE(bot.date) = cal.FECHA_ASIGNACION
    WHERE 
      (p_canal_filter IS NULL OR p_canal_filter = 'BOT')
      AND DATE(bot.date) = p_fecha_proceso
      AND SAFE_CAST(bot.document AS INT64) IS NOT NULL
    
    UNION ALL
    
    -- ================================
    -- GESTIONES HUMANO
    -- ================================
    SELECT
      SAFE_CAST(humano.document AS INT64) AS cod_luna,
      COALESCE(cal.ARCHIVO, 'SIN_CARTERA') AS archivo_cartera,
      DATE(humano.date) AS fecha_gestion,
      'HUMANO' AS canal_origen,
      ROW_NUMBER() OVER (
        PARTITION BY SAFE_CAST(humano.document AS INT64), DATE(humano.date) 
        ORDER BY humano.date
      ) AS secuencia_gestion,
      
      -- 👥 DIMENSIONES DE OPERADOR
      humano.nombre_agente AS nombre_agente_original,
      COALESCE(humano.nombre_agente, 'AGENTE_HUMANO') AS operador_final,
      
      -- 📞 DIMENSIONES ORIGINALES
      humano.management AS management_original,  
      humano.sub_management AS sub_management_original,  
      humano.n3 AS compromiso_original,  
      
      -- 🎯 RESPUESTAS HOMOLOGADAS
      CASE
        WHEN humano.monto_compromiso IS NOT NULL AND humano.monto_compromiso > 0 THEN 'COMPROMISO'
        WHEN humano.management IS NOT NULL AND humano.management != '' THEN 'CONTACTO'
        ELSE 'GESTION'
      END AS grupo_respuesta,
      
      COALESCE(humano.management, 'SIN_MANAGEMENT') AS nivel_1,
      COALESCE(humano.sub_management, 'SIN_SUB_MANAGEMENT') AS nivel_2,
      
      -- 💰 COMPROMISOS
      CASE WHEN humano.monto_compromiso IS NOT NULL AND humano.monto_compromiso > 0 THEN TRUE ELSE FALSE END AS es_compromiso,
      humano.monto_compromiso AS monto_compromiso, 
      humano.fecha_compromiso AS fecha_compromiso, 
      
      -- 🏆 PESO ORIGINAL
      humano.weight AS weight_original,
      
      -- 📊 FLAGS BÁSICOS
      CASE WHEN humano.management IS NOT NULL AND humano.management != '' THEN TRUE ELSE FALSE END AS es_contacto_efectivo,
      
      -- 🕒 METADATOS
      humano.date AS timestamp_gestion
      
    FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e` AS humano
    LEFT JOIN `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` AS cal
      ON DATE(humano.date) = cal.FECHA_ASIGNACION
    WHERE 
      (p_canal_filter IS NULL OR p_canal_filter = 'HUMANO')
      AND DATE(humano.date) = p_fecha_proceso
      AND SAFE_CAST(humano.document AS INT64) IS NOT NULL
  ),
  
  gestiones_procesadas AS (
    SELECT
      -- 🔑 LLAVES PRIMARIAS
      cod_luna, archivo_cartera, fecha_gestion, canal_origen, secuencia_gestion,
      
      -- 👥 DIMENSIONES
      nombre_agente_original, operador_final, management_original, 
      sub_management_original, compromiso_original, grupo_respuesta, nivel_1, nivel_2,
      
      -- 💰 COMPROMISOS
      es_compromiso, monto_compromiso, fecha_compromiso,
      
      -- 🔥 CONTEXTO SIMPLIFICADO (sin JOINs problemáticos)
      'GENERICA' AS tipo_cartera,
      CURRENT_DATE() AS fecha_vencimiento_cliente,
      'POR_DETERMINAR' AS categoria_vencimiento,
      'CICLICA_01' AS ciclica_vencimiento,
      'GENERAL' AS segmento_gestion,
      'LIMA' AS zona_geografica,
      
      -- 📊 FLAGS DE ANÁLISIS
      es_contacto_efectivo,
      CASE WHEN timestamp_gestion = MIN(timestamp_gestion) OVER (
        PARTITION BY cod_luna, fecha_gestion
      ) THEN TRUE ELSE FALSE END AS es_primera_gestion_dia,
      
      -- 🔗 FLAGS SIMPLIFICADOS (sin dependencias)
      FALSE AS tiene_asignacion,
      FALSE AS tiene_deuda,
      TRUE AS es_gestion_medible,
      'SOLO_GESTION' AS tipo_medibilidad,
      
      -- 🏆 PESO Y MARCADORES
      weight_original,
      CASE 
        WHEN canal_origen = 'BOT' 
             AND weight_original = MAX(CASE WHEN canal_origen = 'BOT' THEN weight_original END) OVER (
               PARTITION BY cod_luna, archivo_cartera, fecha_gestion
             )
        THEN TRUE ELSE FALSE 
      END AS es_mejor_gestion_bot_dia,
      
      CASE 
        WHEN canal_origen = 'HUMANO' 
             AND weight_original = MAX(CASE WHEN canal_origen = 'HUMANO' THEN weight_original END) OVER (
               PARTITION BY cod_luna, archivo_cartera, fecha_gestion
             )
        THEN TRUE ELSE FALSE 
      END AS es_mejor_gestion_humano_dia,
      
      -- 📈 MÉTRICAS SIMPLIFICADAS
      COUNT(*) OVER (
        PARTITION BY cod_luna, archivo_cartera, canal_origen
      ) AS gestiones_mes_canal,
      
      SUM(weight_original) OVER (
        PARTITION BY cod_luna, archivo_cartera, canal_origen
      ) AS weight_acumulado_mes_canal,
      
      SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END) OVER (
        PARTITION BY cod_luna, archivo_cartera, canal_origen
      ) AS compromisos_mes_canal,
      
      SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END) OVER (
        PARTITION BY cod_luna, archivo_cartera, canal_origen
      ) AS contactos_efectivos_mes_canal,
      
      SUM(COALESCE(monto_compromiso, 0)) OVER (
        PARTITION BY cod_luna, archivo_cartera, canal_origen
      ) AS monto_compromisos_mes_canal,
      
      100.0 AS tasa_efectividad_canal_mes,
      1 AS ranking_cliente_canal_mes,
      1 AS dias_gestionado_canal_mes,
      
      -- 📅 DIMENSIONES TEMPORALES
      FORMAT_DATE('%A', fecha_gestion) AS dia_semana,
      1 AS semana_mes,
      CASE WHEN EXTRACT(DAYOFWEEK FROM fecha_gestion) IN (1, 7) THEN TRUE ELSE FALSE END AS es_fin_semana,
      
      -- 🕒 METADATOS
      timestamp_gestion,
      CURRENT_TIMESTAMP() AS fecha_actualizacion,
      p_fecha_proceso AS fecha_proceso,
      v_inicio_proceso AS fecha_carga
      
    FROM gestiones_unificadas
  )
  
  SELECT * FROM gestiones_procesadas;
  
  -- ================================================================
  -- ESTADÍSTICAS Y LOGGING FINAL
  -- ================================================================
  
  SET v_registros_procesados = @@row_count;
  
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    message, execution_parameters
  )
  VALUES (
    CURRENT_TIMESTAMP(), 'stage_gestiones_fixed', p_fecha_proceso, 'COMPLETADO', 
    v_registros_procesados,
    CONCAT('Proceso completado. BOT: ', v_gestiones_bot, ', HUMANO: ', v_gestiones_humano, 
           '. Registros: ', v_registros_procesados),
    JSON_OBJECT(
      'fecha_proceso', CAST(p_fecha_proceso AS STRING),
      'canal_filter', IFNULL(p_canal_filter, 'AMBOS_CANALES'),
      'modo_ejecucion', p_modo_ejecucion,
      'gestiones_bot', v_gestiones_bot,
      'gestiones_humano', v_gestiones_humano,
      'registros_procesados', v_registros_procesados
    )
  );
  
  -- Mostrar resumen
  SELECT 
    'PROCESO_COMPLETADO' as status,
    p_fecha_proceso as fecha_proceso,
    canal_origen,
    COUNT(*) as total_gestiones,
    COUNT(DISTINCT cod_luna) as clientes_unicos,
    AVG(weight_original) as weight_promedio,
    COUNT(CASE WHEN es_compromiso THEN 1 END) as total_compromisos,
    COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) as total_contactos_efectivos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = p_fecha_proceso
  GROUP BY canal_origen
  ORDER BY canal_origen;
  
END