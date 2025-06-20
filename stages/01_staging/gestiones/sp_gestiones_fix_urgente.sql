-- ================================================================
-- FIX URGENTE: SP Gestiones - Reemplazar MERGE por DELETE+INSERT
-- ================================================================
-- Usar la lógica que ya funciona en SELECT, pero con DELETE+INSERT
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones_fix_urgente`(
  IN p_fecha_proceso DATE,
  IN p_canal_filter STRING,
  IN p_modo_ejecucion STRING
)

BEGIN
  
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_gestiones_bot INT64 DEFAULT 0;
  DECLARE v_gestiones_humano INT64 DEFAULT 0;
  DECLARE v_registros_procesados INT64 DEFAULT 0;

  SET p_fecha_proceso = IFNULL(p_fecha_proceso, CURRENT_DATE('America/Lima'));
  SET p_modo_ejecucion = IFNULL(p_modo_ejecucion, 'INCREMENTAL');
  
  -- Manejar canal filter
  IF p_canal_filter = '' THEN
    SET p_canal_filter = NULL;
  END IF;
  
  -- Detectar gestiones disponibles
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
  
  -- Log inicio
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    message, execution_parameters
  )
  VALUES (
    CURRENT_TIMESTAMP(), 'stage_gestiones_fix', p_fecha_proceso, 'INICIADO', 0,
    CONCAT('BOT: ', v_gestiones_bot, ', HUMANO: ', v_gestiones_humano),
    JSON_OBJECT(
      'fecha_proceso', CAST(p_fecha_proceso AS STRING),
      'canal_filter', IFNULL(p_canal_filter, 'AMBOS_CANALES'),
      'modo_ejecucion', p_modo_ejecucion
    )
  );
  
  -- ================================================================
  -- PASO 1: LIMPIAR DATOS EXISTENTES
  -- ================================================================
  
  DELETE FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = p_fecha_proceso
    AND (p_canal_filter IS NULL OR canal_origen = p_canal_filter);
  
  -- ================================================================
  -- PASO 2: INSERTAR DATOS - USAR LA LÓGICA QUE YA FUNCIONA
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
  
  -- ✅ AQUÍ VA EL SELECT QUE YA FUNCIONA (exactamente igual)
  WITH gestiones_unificadas AS (
    -- ================================
    -- UNIÓN DE GESTIONES BOT + HUMANO
    -- ================================
    
    SELECT
      -- 🔑 LLAVES PRIMARIAS
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
      COALESCE("VOICEBOT", 'BOT_AUTOMATICO') AS operador_final,
      
      -- 📞 DIMENSIONES ORIGINALES
      bot.management AS management_original,
      bot.sub_management AS sub_management_original,
      bot.compromiso AS compromiso_original,
      
      -- 🎯 RESPUESTAS HOMOLOGADAS (simplificadas para BOT)
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
      AND (p_modo_ejecucion = 'FULL' OR DATE(bot.date) >= p_fecha_proceso)
    
    UNION ALL
    
    SELECT
      -- 🔑 LLAVES PRIMARIAS
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
      AND (p_modo_ejecucion = 'FULL' OR DATE(humano.date) >= p_fecha_proceso)
  ),
  
  gestiones_enriquecidas AS (
    SELECT
      -- 🔑 LLAVES PRIMARIAS
      gest.cod_luna,
      gest.archivo_cartera,
      gest.fecha_gestion,
      gest.canal_origen,
      gest.secuencia_gestion,
      
      -- 👥 DIMENSIONES DE OPERADOR
      gest.nombre_agente_original,
      gest.operador_final,
      
      -- 📞 DIMENSIONES ORIGINALES
      gest.management_original,
      gest.sub_management_original,
      gest.compromiso_original,
      
      -- 🎯 RESPUESTAS HOMOLOGADAS
      gest.grupo_respuesta,
      gest.nivel_1,
      gest.nivel_2,
      
      -- 💰 COMPROMISOS
      gest.es_compromiso,
      gest.monto_compromiso,
      gest.fecha_compromiso,
      
      -- 🔥 CONTEXTO DE CARTERA (SIMPLIFICADO - SIN JOINS PROBLEMÁTICOS)
      'GENERAL' AS tipo_cartera,
      CURRENT_DATE() AS fecha_vencimiento_cliente,
      'POR_DETERMINAR' AS categoria_vencimiento,
      'CICLICA_01' AS ciclica_vencimiento,
      'GENERAL' AS segmento_gestion,
      'LIMA' AS zona_geografica,
      
      -- 📊 FLAGS DE ANÁLISIS
      gest.es_contacto_efectivo,
      
      -- Primera gestión del día para el cliente (considerando ambos canales)
      CASE WHEN gest.timestamp_gestion = MIN(gest.timestamp_gestion) OVER (
        PARTITION BY gest.cod_luna, gest.fecha_gestion
      ) THEN TRUE ELSE FALSE END AS es_primera_gestion_dia,
      
      -- 🔗 REFERENCIAS SIMPLIFICADAS
      FALSE AS tiene_asignacion,
      FALSE AS tiene_deuda,
      TRUE AS es_gestion_medible,
      'SOLO_GESTION' AS tipo_medibilidad,
      
      -- 🏆 PESO ORIGINAL
      gest.weight_original,
      
      -- 🔥 MARCADORES DE MEJOR GESTIÓN POR CANAL
      CASE 
        WHEN gest.canal_origen = 'BOT' 
             AND gest.weight_original = MAX(CASE WHEN gest.canal_origen = 'BOT' THEN gest.weight_original END) OVER (
               PARTITION BY gest.cod_luna, gest.archivo_cartera, gest.fecha_gestion
             )
        THEN TRUE 
        ELSE FALSE 
      END AS es_mejor_gestion_bot_dia,
      
      CASE 
        WHEN gest.canal_origen = 'HUMANO' 
             AND gest.weight_original = MAX(CASE WHEN gest.canal_origen = 'HUMANO' THEN gest.weight_original END) OVER (
               PARTITION BY gest.cod_luna, gest.archivo_cartera, gest.fecha_gestion
             )
        THEN TRUE 
        ELSE FALSE 
      END AS es_mejor_gestion_humano_dia,
      
      -- 📈 MÉTRICAS MENSUALES POR CANAL
      COUNT(*) OVER (
        PARTITION BY gest.cod_luna, gest.archivo_cartera, gest.canal_origen, 
                     EXTRACT(YEAR FROM gest.fecha_gestion), EXTRACT(MONTH FROM gest.fecha_gestion)
      ) AS gestiones_mes_canal,
      
      SUM(CASE WHEN gest.es_compromiso THEN 1 ELSE 0 END) OVER (
        PARTITION BY gest.cod_luna, gest.archivo_cartera, gest.canal_origen,
                     EXTRACT(YEAR FROM gest.fecha_gestion), EXTRACT(MONTH FROM gest.fecha_gestion)
      ) AS compromisos_mes_canal,
      
      SUM(CASE WHEN gest.es_contacto_efectivo THEN 1 ELSE 0 END) OVER (
        PARTITION BY gest.cod_luna, gest.archivo_cartera, gest.canal_origen,
                     EXTRACT(YEAR FROM gest.fecha_gestion), EXTRACT(MONTH FROM gest.fecha_gestion)
      ) AS contactos_efectivos_mes_canal,
      
      SUM(COALESCE(gest.monto_compromiso, 0)) OVER (
        PARTITION BY gest.cod_luna, gest.archivo_cartera, gest.canal_origen,
                     EXTRACT(YEAR FROM gest.fecha_gestion), EXTRACT(MONTH FROM gest.fecha_gestion)
      ) AS monto_compromisos_mes_canal,
      
      COUNT(DISTINCT gest.fecha_gestion) OVER (
        PARTITION BY gest.cod_luna, gest.archivo_cartera, gest.canal_origen,
                     EXTRACT(YEAR FROM gest.fecha_gestion), EXTRACT(MONTH FROM gest.fecha_gestion)
      ) AS dias_gestionado_canal_mes,
      
      -- 📅 DIMENSIONES TEMPORALES
      FORMAT_DATE('%A', gest.fecha_gestion) AS dia_semana,
      EXTRACT(WEEK FROM gest.fecha_gestion) - EXTRACT(WEEK FROM DATE_TRUNC(gest.fecha_gestion, MONTH)) + 1 AS semana_mes,
      CASE WHEN EXTRACT(DAYOFWEEK FROM gest.fecha_gestion) IN (1, 7) THEN TRUE ELSE FALSE END AS es_fin_semana,
      
      -- 🕒 METADATOS
      gest.timestamp_gestion,
      CURRENT_TIMESTAMP() AS fecha_actualizacion,
      p_fecha_proceso AS fecha_proceso,
      v_inicio_proceso AS fecha_carga
      
    FROM gestiones_unificadas AS gest
  ),
  
  gestiones_con_metricas AS (
    SELECT 
      *,
      
      -- 📈 CALCULAR WEIGHT ACUMULADO MES (solo mejores gestiones por canal)
      SUM(CASE 
        WHEN (canal_origen = 'BOT' AND es_mejor_gestion_bot_dia) 
             OR (canal_origen = 'HUMANO' AND es_mejor_gestion_humano_dia)
        THEN weight_original 
        ELSE 0 
      END) OVER (
        PARTITION BY cod_luna, archivo_cartera, canal_origen,
                     EXTRACT(YEAR FROM fecha_gestion), EXTRACT(MONTH FROM fecha_gestion)
        ORDER BY fecha_gestion
        ROWS UNBOUNDED PRECEDING
      ) AS weight_acumulado_mes_canal,
      
      -- 🎯 TASA DE EFECTIVIDAD POR CANAL
      CASE 
        WHEN gestiones_mes_canal > 0 
        THEN ROUND((contactos_efectivos_mes_canal + compromisos_mes_canal) / gestiones_mes_canal * 100, 2)
        ELSE NULL 
      END AS tasa_efectividad_canal_mes
      
    FROM gestiones_enriquecidas
  ),
  
  gestiones_con_ranking AS (
    SELECT 
      *,
      
      -- 🏆 RANKING POR CANAL Y MES
      ROW_NUMBER() OVER (
        PARTITION BY canal_origen, archivo_cartera,
                     EXTRACT(YEAR FROM fecha_gestion), EXTRACT(MONTH FROM fecha_gestion)
        ORDER BY weight_acumulado_mes_canal DESC, dias_gestionado_canal_mes DESC
      ) AS ranking_cliente_canal_mes
      
    FROM gestiones_con_metricas
  )
  
  SELECT * FROM gestiones_con_ranking;
  
  -- ================================================================
  -- LOGGING FINAL
  -- ================================================================
  
  SET v_registros_procesados = @@row_count;
  
  INSERT INTO `BI_USA.pipeline_logs` (
    timestamp, stage_name, fecha_proceso, status, records_processed, 
    message, execution_parameters
  )
  VALUES (
    CURRENT_TIMESTAMP(), 'stage_gestiones_fix', p_fecha_proceso, 'COMPLETADO', 
    v_registros_procesados,
    CONCAT('ÉXITO - BOT: ', v_gestiones_bot, ', HUMANO: ', v_gestiones_humano, 
           '. Registros: ', v_registros_procesados),
    JSON_OBJECT(
      'fecha_proceso', CAST(p_fecha_proceso AS STRING),
      'registros_procesados', v_registros_procesados
    )
  );
  
  -- Mostrar resultado
  SELECT 
    'PROCESO_EXITOSO' as status,
    p_fecha_proceso as fecha_proceso,
    canal_origen,
    COUNT(*) as total_gestiones,
    COUNT(DISTINCT cod_luna) as clientes_unicos,
    AVG(weight_original) as weight_promedio
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = p_fecha_proceso
  GROUP BY canal_origen
  ORDER BY canal_origen;
  
END