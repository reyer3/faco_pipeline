-- ================================================================
-- STORED PROCEDURE: Stage de Gestiones - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-19
-- Versión: 1.3.1 (CORREGIDO)
-- Descripción: Procesamiento de gestiones unificadas BOT + HUMANO
--              con marcadores de mejor gestión por canal separado
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`(
  IN p_fecha_proceso DATE,
  IN p_modo_ejecucion STRING, -- 'FULL' o 'INCREMENTAL'
  IN p_canal_filter STRING  -- OPCIONAL: 'BOT', 'HUMANO' o NULL para ambos
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

  SET p_fecha_proceso = IFNULL(p_fecha_proceso, CURRENT_DATE('America/Lima'));
  SET p_modo_ejecucion = IFNULL(p_modo_ejecucion, 'INCREMENTAL');
  
  -- ================================================================
  -- DETECCIÓN DE CANALES Y VOLÚMENES
  -- ================================================================
  
  -- Detectar gestiones BOT disponibles para la fecha
  SET v_gestiones_bot = (
    SELECT COUNT(*)
    FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
    WHERE DATE(date) = p_fecha_proceso
      AND SAFE_CAST(document AS INT64) IS NOT NULL
  );

  -- Detectar gestiones HUMANO disponibles para la fecha
  SET v_gestiones_humano = (
    SELECT COUNT(*)
    FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
    WHERE DATE(date) = p_fecha_proceso
      AND SAFE_CAST(document AS INT64) IS NOT NULL
  );

  -- Construir resumen de canales detectados
  SET v_canales_detectados = CONCAT(
    'BOT: ', CAST(v_gestiones_bot AS STRING),
    ', HUMANO: ', CAST(v_gestiones_humano AS STRING)
  );
  
 -- ================================================================
  -- LOGGING: Inicio del proceso
  -- ================================================================
  INSERT INTO `BI_USA.pipeline_logs` (
  timestamp,
  stage_name,
  fecha_proceso,
  status,
  records_processed,
  duration_seconds,
  message,
  execution_parameters
)
VALUES (
  CURRENT_TIMESTAMP(),                          -- timestamp
  'stage_gestiones',                            -- stage_name
  p_fecha_proceso,                              -- fecha_proceso
  'INICIADO',                                   -- status
  0,                                            -- records_processed (puedes ajustar más adelante)
  0.0,                                          -- duration_seconds (puedes actualizar al final del stage)
  CONCAT('Canales detectados: ', v_canales_detectados),  -- mensaje descriptivo
  JSON_OBJECT(                                  -- execution_parameters
    'fecha_proceso', CAST(p_fecha_proceso AS STRING),
    'canal_filter', IFNULL(p_canal_filter, 'AMBOS_CANALES'),
    'modo_ejecucion', p_modo_ejecucion,
    'gestiones_bot', v_gestiones_bot,
    'gestiones_humano', v_gestiones_humano
  )
);
  
  -- ================================================================
  -- MERGE/UPSERT: Datos de gestiones unificadas
  -- ================================================================
  
  MERGE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones` AS target
  USING (
    
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
        
        -- 💰 COMPROMISOS (CORREGIDO)
        CASE WHEN bot.compromiso IS NOT NULL AND bot.compromiso != '' THEN TRUE ELSE FALSE END AS es_compromiso,
        SAFE_CAST(bot.compromiso AS FLOAT64) AS monto_compromiso,
        -- CORREGIDO: Usar fecha_compromiso directamente y convertir a DATE
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
        
        -- 📞 DIMENSIONES ORIGINALES (CORREGIDO)
        humano.management AS management_original,  -- Este campo existe en el esquema
        humano.sub_management AS sub_management_original,  -- Este campo existe en el esquema
        humano.n3 AS compromiso_original,  -- CORREGIDO: era N3, ahora n3
        
        -- 🎯 RESPUESTAS HOMOLOGADAS (CORREGIDO)
        CASE
          WHEN humano.monto_compromiso IS NOT NULL AND humano.monto_compromiso > 0 THEN 'COMPROMISO'
          WHEN humano.management IS NOT NULL AND humano.management != '' THEN 'CONTACTO'
          ELSE 'GESTION'
        END AS grupo_respuesta,
        
        COALESCE(humano.management, 'SIN_MANAGEMENT') AS nivel_1,
        COALESCE(humano.sub_management, 'SIN_SUB_MANAGEMENT') AS nivel_2,
        
        -- 💰 COMPROMISOS (CORREGIDO)
        CASE WHEN humano.monto_compromiso IS NOT NULL AND humano.monto_compromiso > 0 THEN TRUE ELSE FALSE END AS es_compromiso,
        humano.monto_compromiso AS monto_compromiso,  -- CORREGIDO: era compromise
        humano.fecha_compromiso AS fecha_compromiso,  -- Ya es DATE en el esquema
        
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
        
        -- 🔥 CONTEXTO DE CARTERA
        COALESCE(asig.tipo_cartera, 'OTRAS') AS tipo_cartera,
        asig.fecha_vencimiento AS fecha_vencimiento_cliente,
        COALESCE(asig.categoria_vencimiento, 'SIN_VENCIMIENTO') AS categoria_vencimiento,
        
        -- Calcular cíclica desde día de vencimiento
        CASE 
          WHEN asig.fecha_vencimiento IS NOT NULL THEN
            CONCAT('CICLICA_', FORMAT('%02d', EXTRACT(DAY FROM asig.fecha_vencimiento)))
          ELSE 'SIN_CICLICA'
        END AS ciclica_vencimiento,
        
        COALESCE(asig.segmento_gestion, 'SIN_SEGMENTO') AS segmento_gestion,
        COALESCE(asig.zona_geografica, 'SIN_ZONA') AS zona_geografica,
        
        -- 📊 FLAGS DE ANÁLISIS
        gest.es_contacto_efectivo,
        
        -- Primera gestión del día para el cliente (considerando ambos canales)
        CASE WHEN gest.timestamp_gestion = MIN(gest.timestamp_gestion) OVER (
          PARTITION BY gest.cod_luna, gest.fecha_gestion
        ) THEN TRUE ELSE FALSE END AS es_primera_gestion_dia,
        
        -- 🔗 REFERENCIAS A OTROS STAGES
        CASE WHEN asig.cod_luna IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_asignacion,
        CASE WHEN deud.cod_cuenta IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_deuda,
        
        -- Medibilidad general
        CASE WHEN asig.cod_luna IS NOT NULL OR deud.cod_cuenta IS NOT NULL 
             THEN TRUE ELSE FALSE END AS es_gestion_medible,
        
        -- Tipo de medibilidad
        CASE 
          WHEN asig.cod_luna IS NOT NULL AND deud.cod_cuenta IS NOT NULL THEN 'ASIGNACION_Y_DEUDA'
          WHEN asig.cod_luna IS NOT NULL THEN 'SOLO_ASIGNACION'
          WHEN deud.cod_cuenta IS NOT NULL THEN 'SOLO_DEUDA'
          ELSE 'NO_MEDIBLE'
        END AS tipo_medibilidad,
        
        -- 🏆 PESO ORIGINAL
        gest.weight_original,
        
        -- 🔥 MARCADORES DE MEJOR GESTIÓN POR CANAL (NUEVO ENFOQUE)
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
      
      -- Join con asignación para contexto de cartera
      LEFT JOIN `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion` AS asig
        ON CAST(gest.cod_luna AS STRING) = asig.cod_luna
        AND gest.fecha_gestion = asig.fecha_asignacion
      
      -- Join con deudas para verificar medibilidad
      LEFT JOIN `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas` AS deud
        ON CAST(gest.cod_luna AS STRING) = deud.cod_cuenta
        AND gest.fecha_gestion = deud.fecha_deuda
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
    
    SELECT * FROM gestiones_con_ranking
    
  ) AS source
  
  ON target.cod_luna = source.cod_luna
     AND target.archivo_cartera = source.archivo_cartera
     AND target.fecha_gestion = source.fecha_gestion
     AND target.canal_origen = source.canal_origen
     AND target.secuencia_gestion = source.secuencia_gestion
  
  -- 🔄 ACTUALIZAR REGISTROS EXISTENTES
  WHEN MATCHED THEN UPDATE SET
    target.weight_original = source.weight_original,
    target.es_mejor_gestion_bot_dia = source.es_mejor_gestion_bot_dia,
    target.es_mejor_gestion_humano_dia = source.es_mejor_gestion_humano_dia,
    target.weight_acumulado_mes_canal = source.weight_acumulado_mes_canal,
    target.gestiones_mes_canal = source.gestiones_mes_canal,
    target.compromisos_mes_canal = source.compromisos_mes_canal,
    target.contactos_efectivos_mes_canal = source.contactos_efectivos_mes_canal,
    target.tasa_efectividad_canal_mes = source.tasa_efectividad_canal_mes,
    target.ranking_cliente_canal_mes = source.ranking_cliente_canal_mes,
    target.fecha_actualizacion = source.fecha_actualizacion,
    target.fecha_proceso = source.fecha_proceso
  
  -- ➕ INSERTAR NUEVOS REGISTROS
  WHEN NOT MATCHED THEN INSERT (
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
  VALUES (
    source.cod_luna, source.archivo_cartera, source.fecha_gestion, source.canal_origen,
    source.secuencia_gestion, source.nombre_agente_original, source.operador_final,
    source.management_original, source.sub_management_original, source.compromiso_original,
    source.grupo_respuesta, source.nivel_1, source.nivel_2, source.es_compromiso,
    source.monto_compromiso, source.fecha_compromiso, source.tipo_cartera,
    source.fecha_vencimiento_cliente, source.categoria_vencimiento, source.ciclica_vencimiento,
    source.segmento_gestion, source.zona_geografica, source.es_contacto_efectivo,
    source.es_primera_gestion_dia, source.tiene_asignacion, source.tiene_deuda,
    source.es_gestion_medible, source.tipo_medibilidad, source.weight_original,
    source.es_mejor_gestion_bot_dia, source.es_mejor_gestion_humano_dia,
    source.gestiones_mes_canal, source.weight_acumulado_mes_canal, source.compromisos_mes_canal,
    source.contactos_efectivos_mes_canal, source.monto_compromisos_mes_canal,
    source.tasa_efectividad_canal_mes, source.ranking_cliente_canal_mes,
    source.dias_gestionado_canal_mes, source.dia_semana, source.semana_mes,
    source.es_fin_semana, TIMESTAMP(source.timestamp_gestion), source.fecha_actualizacion,
    source.fecha_proceso, source.fecha_carga
  );
  
  -- ================================================================
  -- ESTADÍSTICAS Y LOGGING FINAL
  -- ================================================================
  
  SET v_registros_procesados = @@row_count;
  
  -- Obtener estadísticas detalladas
  SET v_registros_nuevos = (
    SELECT COUNT(*)
    FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
    WHERE fecha_carga = v_inicio_proceso
  );

  SET v_registros_actualizados = v_registros_procesados - v_registros_nuevos;
  
  -- Log final con métricas de negocio (ESTRUCTURA UNIFICADA)
  -- ================================================================
  INSERT INTO `BI_USA.pipeline_logs` (
  timestamp,
  stage_name,
  fecha_proceso,
  status,
  records_processed,
  message,
  execution_parameters
)
VALUES (
  CURRENT_TIMESTAMP(),
  'stage_gestiones',
  CURRENT_DATE("America/Lima"),
  'COMPLETADO',
  v_registros_procesados,
  CONCAT(
    'Proceso completado. Canales: ', v_canales_detectados,
    '. Enfoque: mejor gestión por canal separado. ',
    'Duración: ', CAST(DATETIME_DIFF(CURRENT_TIMESTAMP(), v_inicio_proceso, SECOND) AS STRING), 's'
  ),
  JSON_OBJECT(
    'fecha_proceso', CAST(p_fecha_proceso AS STRING),
    'canal_filter', IFNULL(p_canal_filter, 'AMBOS_CANALES'),
    'modo_ejecucion', p_modo_ejecucion,
    'gestiones_bot', v_gestiones_bot,
    'gestiones_humano', v_gestiones_humano
  )
);


  
  
  -- ================================================================
  -- RESUMEN DE NEGOCIO POR CANAL
  -- ================================================================
  
  -- Mostrar métricas de gestiones por canal
  SELECT 
    'RESUMEN_GESTIONES_POR_CANAL' as tipo,
    p_fecha_proceso as fecha_proceso,
    canal_origen,
    COUNT(*) as total_gestiones,
    COUNT(DISTINCT cod_luna) as clientes_unicos,
    SUM(CASE WHEN es_mejor_gestion_bot_dia OR es_mejor_gestion_humano_dia THEN 1 ELSE 0 END) as mejores_gestiones,
    AVG(weight_original) as weight_promedio,
    COUNT(CASE WHEN es_compromiso THEN 1 END) as total_compromisos,
    COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) as total_contactos_efectivos,
    ROUND(SUM(COALESCE(monto_compromiso, 0)), 2) as monto_total_compromisos
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_proceso = p_fecha_proceso
  GROUP BY canal_origen
  ORDER BY canal_origen;
  
END;