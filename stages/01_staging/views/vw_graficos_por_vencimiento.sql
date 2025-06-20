-- ================================================================
-- VIEW: Gráficos por Vencimiento - Dashboard Ejecutivo
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-20
-- Descripción: View para generar gráficos por cartera y vencimiento
--              7 indicadores clave con estructura lista para dashboards
-- ================================================================

CREATE OR REPLACE VIEW `BI_USA.vw_graficos_por_vencimiento` AS

WITH base_gestiones AS (
  SELECT 
    -- 🔑 DIMENSIONES PRINCIPALES
    tipo_cartera,
    fecha_gestion,
    categoria_vencimiento,
    
    -- 📊 CÁLCULO DE DÍAS DE VENCIMIENTO ESPECÍFICOS
    CASE 
      WHEN fecha_vencimiento_cliente IS NULL THEN 99
      ELSE DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY)
    END as dias_al_vencimiento,
    
    -- 📊 AGRUPACIÓN POR BUCKETS DE VENCIMIENTO (como en reporte)
    CASE 
      WHEN DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY) BETWEEN 1 AND 5 THEN 'VCTO_05'
      WHEN DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY) BETWEEN 6 AND 9 THEN 'VCTO_09'
      WHEN DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY) BETWEEN 10 AND 13 THEN 'VCTO_13'
      WHEN DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY) BETWEEN 14 AND 17 THEN 'VCTO_17'
      WHEN DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY) BETWEEN 18 AND 21 THEN 'VCTO_21'
      WHEN DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY) BETWEEN 22 AND 25 THEN 'VCTO_25'
      WHEN DATE_DIFF(fecha_vencimiento_cliente, fecha_gestion, DAY) <= 0 THEN 'VENCIDO'
      ELSE 'MAS_25D'
    END as bucket_vencimiento,
    
    -- 🎯 FLAGS DE GESTIÓN
    es_contacto_efectivo,
    es_compromiso,
    weight_original,
    canal_origen,
    
    -- 📞 TIPO DE CONTACTO (para CD/CI)
    CASE 
      WHEN nivel_1 LIKE '%CONTACTO_DIRECTO%' OR nivel_1 LIKE '%HABLA_TITULAR%' THEN 'DIRECTO'
      WHEN nivel_1 LIKE '%CONTACTO_INDIRECTO%' OR nivel_1 LIKE '%TERCERO%' THEN 'INDIRECTO'  
      ELSE 'OTROS'
    END as tipo_contacto,
    
    -- 🔗 LLAVES PARA JOINS
    cod_luna,
    archivo_cartera,
    
    -- 📅 METADATOS
    fecha_proceso,
    EXTRACT(YEAR FROM fecha_gestion) as anio,
    EXTRACT(MONTH FROM fecha_gestion) as mes
    
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`
  WHERE fecha_gestion >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) -- Últimos 3 meses
),

pagos_resumidos AS (
  SELECT 
    cod_luna,
    archivo_cartera,
    fecha_compromiso,
    COUNT(*) as total_pagos,
    SUM(monto_pago) as monto_total_pagado
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`
  WHERE fecha_pago IS NOT NULL
  GROUP BY cod_luna, archivo_cartera, fecha_compromiso
),

metricas_calculadas AS (
  SELECT 
    -- 🏷️ DIMENSIONES
    g.tipo_cartera,
    g.bucket_vencimiento,
    g.fecha_gestion,
    g.anio,
    g.mes,
    g.canal_origen,
    
    -- 📊 CONTADORES BASE
    COUNT(*) as total_gestiones,
    COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END) as total_contactos,
    COUNT(CASE WHEN g.es_compromiso THEN 1 END) as total_compromisos,
    COUNT(CASE WHEN g.tipo_contacto = 'DIRECTO' AND g.es_contacto_efectivo THEN 1 END) as contactos_directos,
    COUNT(CASE WHEN g.tipo_contacto = 'INDIRECTO' AND g.es_contacto_efectivo THEN 1 END) as contactos_indirectos,
    COUNT(CASE WHEN p.total_pagos > 0 THEN 1 END) as total_pagos_efectivos,
    
    -- 🔢 MÉTRICAS DE INTENSIDAD
    AVG(g.weight_original) as intensidad_promedio,
    MAX(g.weight_original) as intensidad_maxima,
    
    -- 📈 CÁLCULO DE INDICADORES (en porcentaje)
    ROUND(
      SAFE_DIVIDE(COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END), COUNT(*)) * 100, 2
    ) as contactabilidad_pct,
    
    ROUND(
      SAFE_DIVIDE(COUNT(CASE WHEN g.es_compromiso THEN 1 END), 
                   COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END)) * 100, 2
    ) as conversion_pct,
    
    ROUND(
      SAFE_DIVIDE(COUNT(CASE WHEN p.total_pagos > 0 THEN 1 END),
                   COUNT(CASE WHEN g.es_compromiso THEN 1 END)) * 100, 2
    ) as tasa_cierre_pct,
    
    ROUND(
      SAFE_DIVIDE(COUNT(CASE WHEN g.es_contacto_efectivo OR g.es_compromiso THEN 1 END), 
                   COUNT(*)) * 100, 2
    ) as efectividad_pct,
    
    ROUND(
      SAFE_DIVIDE(COUNT(CASE WHEN g.tipo_contacto = 'DIRECTO' AND g.es_contacto_efectivo THEN 1 END),
                   COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END)) * 100, 2
    ) as cd_pct,
    
    ROUND(
      SAFE_DIVIDE(COUNT(CASE WHEN g.tipo_contacto = 'INDIRECTO' AND g.es_contacto_efectivo THEN 1 END),
                   COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END)) * 100, 2
    ) as ci_pct
    
  FROM base_gestiones g
  LEFT JOIN pagos_resumidos p 
    ON g.cod_luna = p.cod_luna 
    AND g.archivo_cartera = p.archivo_cartera
    AND g.fecha_gestion = p.fecha_compromiso
  GROUP BY 
    g.tipo_cartera, g.bucket_vencimiento, g.fecha_gestion, 
    g.anio, g.mes, g.canal_origen
)

-- 📊 RESULTADO FINAL PARA GRÁFICOS
SELECT 
  -- 🏷️ DIMENSIONES PARA GRÁFICOS
  tipo_cartera as cartera,
  bucket_vencimiento as vencimiento,
  fecha_gestion as dia_calendario,
  anio,
  mes,
  canal_origen,
  
  -- 📈 ORDENAMIENTO PARA VENCIMIENTOS
  CASE bucket_vencimiento
    WHEN 'VCTO_05' THEN 1
    WHEN 'VCTO_09' THEN 2  
    WHEN 'VCTO_13' THEN 3
    WHEN 'VCTO_17' THEN 4
    WHEN 'VCTO_21' THEN 5
    WHEN 'VCTO_25' THEN 6
    WHEN 'VENCIDO' THEN 0
    ELSE 7
  END as orden_vencimiento,
  
  -- 🎯 LOS 7 INDICADORES PRINCIPALES
  contactabilidad_pct as contactabilidad,
  conversion_pct as conversion,
  tasa_cierre_pct as tasa_cierre,
  efectividad_pct as efectividad,
  ROUND(intensidad_promedio, 2) as intensidad,
  cd_pct as cd_porcentaje,
  ci_pct as ci_porcentaje,
  
  -- 📊 CONTADORES ADICIONALES (para validación)
  total_gestiones,
  total_contactos,
  total_compromisos,
  total_pagos_efectivos,
  
  -- 📅 METADATA
  CURRENT_TIMESTAMP() as fecha_actualizacion

FROM metricas_calculadas
WHERE bucket_vencimiento IN ('VCTO_05', 'VCTO_09', 'VCTO_13', 'VCTO_17', 'VCTO_21', 'VCTO_25')
ORDER BY 
  tipo_cartera, 
  orden_vencimiento, 
  fecha_gestion DESC;

-- ================================================================
-- OPCIONES DE VIEW
-- ================================================================
-- Esta view está lista para:
-- 1. Gráficos con carteras como líneas diferentes
-- 2. Eje X = vencimientos (05, 09, 13, 17, 21, 25)  
-- 3. Eje Y = valor del indicador
-- 4. 7 cuadritos separados (uno por indicador)
-- 5. Filtros por fecha, canal, etc.
--
-- EJEMPLOS DE USO:
-- 
-- Para gráfico de CONTACTABILIDAD:
-- SELECT cartera, vencimiento, contactabilidad 
-- FROM vw_graficos_por_vencimiento 
-- WHERE mes = 6 AND anio = 2025
--
-- Para comparativo temporal:
-- SELECT cartera, vencimiento, contactabilidad, mes
-- FROM vw_graficos_por_vencimiento 
-- WHERE anio = 2025 AND mes IN (5,6)
-- ================================================================