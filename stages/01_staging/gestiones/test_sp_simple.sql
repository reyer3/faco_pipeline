-- ================================================================
-- SP SIMPLIFICADO PARA TEST - Gestiones
-- ================================================================
-- Versión mínima para diagnosticar el problema
-- ================================================================

CREATE OR REPLACE PROCEDURE `BI_USA.test_sp_gestiones_simple`(
  IN p_fecha_proceso DATE
)

BEGIN
  
  DECLARE v_registros_bot INT64 DEFAULT 0;
  DECLARE v_registros_humano INT64 DEFAULT 0;
  
  -- Verificar datos BOT
  SET v_registros_bot = (
    SELECT COUNT(*)
    FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
    WHERE DATE(date) = p_fecha_proceso
      AND SAFE_CAST(document AS INT64) IS NOT NULL
  );

  -- Verificar datos HUMANO
  SET v_registros_humano = (
    SELECT COUNT(*)
    FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
    WHERE DATE(date) = p_fecha_proceso
      AND SAFE_CAST(document AS INT64) IS NOT NULL
  );
  
  -- Insertar resultado simple en la tabla
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
  
  -- SOLO BOT - versión super simplificada
  SELECT 
    SAFE_CAST(document AS INT64) as cod_luna,
    'TEST_CARTERA' as archivo_cartera,
    DATE(date) as fecha_gestion,
    'BOT' as canal_origen,
    1 as secuencia_gestion,
    
    'VOICEBOT' as nombre_agente_original,
    'VOICEBOT' as operador_final,
    management as management_original,
    sub_management as sub_management_original,
    compromiso as compromiso_original,
    'TEST' as grupo_respuesta,
    COALESCE(management, 'SIN_MANAGEMENT') as nivel_1,
    COALESCE(sub_management, 'SIN_SUB') as nivel_2,
    FALSE as es_compromiso,
    
    NULL as monto_compromiso,
    NULL as fecha_compromiso,
    'TEST' as tipo_cartera,
    NULL as fecha_vencimiento_cliente,
    'TEST' as categoria_vencimiento,
    'TEST' as ciclica_vencimiento,
    'TEST' as segmento_gestion,
    'TEST' as zona_geografica,
    
    TRUE as es_contacto_efectivo,
    TRUE as es_primera_gestion_dia,
    FALSE as tiene_asignacion,
    FALSE as tiene_deuda,
    TRUE as es_gestion_medible,
    'TEST' as tipo_medibilidad,
    weight as weight_original,
    TRUE as es_mejor_gestion_bot_dia,
    FALSE as es_mejor_gestion_humano_dia,
    
    1 as gestiones_mes_canal,
    weight as weight_acumulado_mes_canal,
    0 as compromisos_mes_canal,
    1 as contactos_efectivos_mes_canal,
    0 as monto_compromisos_mes_canal,
    100.0 as tasa_efectividad_canal_mes,
    1 as ranking_cliente_canal_mes,
    1 as dias_gestionado_canal_mes,
    
    'TEST' as dia_semana,
    1 as semana_mes,
    FALSE as es_fin_semana,
    date as timestamp_gestion,
    CURRENT_TIMESTAMP() as fecha_actualizacion,
    p_fecha_proceso as fecha_proceso,
    CURRENT_TIMESTAMP() as fecha_carga
    
  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
  WHERE DATE(date) = p_fecha_proceso
    AND SAFE_CAST(document AS INT64) IS NOT NULL
  LIMIT 100;  -- Solo 100 registros para test
  
  -- Mostrar resultado
  SELECT 
    CONCAT('TEST COMPLETADO. BOT: ', v_registros_bot, ', HUMANO: ', v_registros_humano) as resultado,
    @@row_count as registros_insertados;
    
END