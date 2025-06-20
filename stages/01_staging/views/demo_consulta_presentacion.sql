-- ================================================================
-- CONSULTA DE DEMOSTRACIÓN - Dashboard Ejecutivo
-- ================================================================
-- Para mostrar en presentación: réplica exacta del reporte OnbotGo
-- Ejecutar en BigQuery para demostrar funcionamiento
-- ================================================================

-- 🎯 CONSULTA PRINCIPAL: Los 7 gráficos por vencimiento
-- (Replica exactamente el formato del reporte que mostraste)

SELECT 
  cartera,
  vencimiento,
  
  -- 📊 LOS 7 INDICADORES CLAVE
  contactabilidad,
  conversion, 
  tasa_cierre,
  efectividad,
  intensidad,
  cd_porcentaje as contacto_directo,
  ci_porcentaje as contacto_indirecto,
  
  -- 📈 CONTADORES PARA VALIDAR
  total_gestiones,
  total_contactos,
  total_compromisos,
  
  -- 📅 CONTEXTO TEMPORAL
  mes,
  anio

FROM `BI_USA.vw_graficos_por_vencimiento`
WHERE 
  -- Filtra mes actual o específico
  mes = EXTRACT(MONTH FROM CURRENT_DATE())
  AND anio = EXTRACT(YEAR FROM CURRENT_DATE())
  
  -- Solo vencimientos principales (como en reporte)
  AND vencimiento IN ('VCTO_05', 'VCTO_09', 'VCTO_13', 'VCTO_17')
  
ORDER BY 
  cartera, 
  orden_vencimiento;

-- ================================================================
-- 🚀 RESULTADO ESPERADO (ejemplo):
-- ================================================================
/*
cartera        | vencimiento | contactabilidad | conversion | efectividad | intensidad
TEMPRANA       | VCTO_05     | 42.3            | 35.7      | 38.1        | 18.4
TEMPRANA       | VCTO_09     | 38.1            | 32.2      | 35.6        | 15.2  
TEMPRANA       | VCTO_13     | 33.5            | 28.9      | 31.7        | 12.8
TEMPRANA       | VCTO_17     | 29.8            | 25.4      | 28.3        | 10.5
FRACCIONAMIENT | VCTO_05     | 45.1            | 38.2      | 41.2        | 20.1
...
*/

-- ================================================================
-- 📊 CONSULTA PARA GRÁFICO DE LÍNEAS (por cartera)
-- ================================================================

-- Específicamente para Looker Studio o similar
SELECT 
  vencimiento,
  
  -- Una columna por cartera (para líneas separadas)
  MAX(CASE WHEN cartera = 'TEMPRANA' THEN contactabilidad END) as temprana_contactabilidad,
  MAX(CASE WHEN cartera = 'FRACCIONAMIENTO' THEN contactabilidad END) as fraccionamiento_contactabilidad,
  MAX(CASE WHEN cartera = 'ALTAS_NUEVAS' THEN contactabilidad END) as altas_nuevas_contactabilidad,
  
  -- Orden para eje X
  MIN(orden_vencimiento) as orden

FROM `BI_USA.vw_graficos_por_vencimiento`
WHERE mes = EXTRACT(MONTH FROM CURRENT_DATE())
  AND anio = EXTRACT(YEAR FROM CURRENT_DATE())
GROUP BY vencimiento
ORDER BY orden;

-- ================================================================
-- 🔍 CONSULTA DE VALIDACIÓN (para mostrar que datos son correctos)
-- ================================================================

SELECT 
  'RESUMEN EJECUTIVO' as seccion,
  cartera,
  COUNT(DISTINCT vencimiento) as vencimientos_disponibles,
  AVG(contactabilidad) as contactabilidad_promedio,
  AVG(efectividad) as efectividad_promedio,
  SUM(total_gestiones) as gestiones_totales,
  MAX(dia_calendario) as ultima_fecha_disponible

FROM `BI_USA.vw_graficos_por_vencimiento`
WHERE mes = EXTRACT(MONTH FROM CURRENT_DATE())
  AND anio = EXTRACT(YEAR FROM CURRENT_DATE())
GROUP BY cartera
ORDER BY cartera;

-- ================================================================
-- 🎯 PARA TU PRESENTACIÓN - PUNTOS CLAVE:
-- ================================================================
/*
1. "Mismos datos del reporte manual, pero automatizados"
2. "7 indicadores listos para dashboard en tiempo real"
3. "Filtros dinámicos por cartera, mes, canal"
4. "De Excel a BigQuery: escalabilidad total"
5. "Base sólida para crecer: más carteras, más métricas"

DEMOSTRACIÓN:
- Ejecutar consulta principal → mostrar datos
- Cambiar filtro de mes → mostrar flexibilidad  
- Explicar que cada gráfico del reporte sale de aquí
- Mostrar que se puede conectar directo a Looker Studio
*/
