-- ##############################################################################
-- #           INSERT DE DATOS PARA CALENDARIO v5 - ESQUEMA CORRECTO           #
-- #                    Datos de mayo-junio 2025                               #
-- ##############################################################################

-- Insertar datos según el esquema real de la tabla
INSERT INTO mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
(
  -- 🔧 CAMPOS OBLIGATORIOS
  ARCHIVO,
  CANT_COD_LUNA_UNIQUE,
  CANTCUENTA,
  FECHA_ASIGNACION,
  FECHA_TRANDEUDA,
  VENCIMIENTO,
  FECHA_CREACION,
  FECHA_ACTUALIZACION,

  -- 🔧 CAMPOS OPCIONALES CON DATOS DISPONIBLES
  FECHA_CIERRE,
  DIAS_GESTION,
  DIAS_PARA_CIERRE
)
VALUES
  -- ===== MAYO 2025 =====
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250513', 2522, 2522, '2025-05-14', '2025-05-14', 13, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-15', 32, 0),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250513', 866, 866, '2025-05-14', '2025-05-14', 13, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-15', 32, 0),
  ('Cartera_Agencia_Cobranding_Gestion_Temprana_20250520', 4169, 4187, '2025-05-20', '2025-05-20', 13, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-15', 26, 0),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250520', 1686, 1686, '2025-05-20', '2025-05-20', 17, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-15', 26, 0),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250520', 2078, 2078, '2025-05-20', '2025-05-20', 17, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-15', 26, 0),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250521', 2934, 2934, '2025-05-21', '2025-05-22', 21, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-22', 31, 3),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250521', 2701, 2701, '2025-05-21', '2025-05-22', 21, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-22', 31, 3),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250526', 2179, 2179, '2025-05-26', '2025-05-27', 25, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-22', 26, 3),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250526', 857, 857, '2025-05-26', '2025-05-27', 25, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-22', 26, 3),
  ('Cartera_Agencia_Cobranding_Gestion_Temprana_20250527', 5784, 5789, '2025-05-27', '2025-05-27', 17, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-15', 19, 0),
  ('Cartera_Agencia_Cobranding_Gestion_Temprana_20250527_21', 5640, 5643, '2025-05-27', '2025-05-27', 21, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-22', 26, 3),

  -- ===== JUNIO 2025 =====
  ('Cartera_Agencia_Cobranding_Gestion_Temprana_20250603', 3088, 3089, '2025-06-03', '2025-06-03', 25, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-22', 19, 3),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250602', 3626, 3626, '2025-06-03', '2025-06-03', 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 27, 11),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250602', 845, 845, '2025-06-03', '2025-06-03', 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 27, 11),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250605', 1533, 1533, '2025-06-05', '2025-06-06', 5, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 24, 11),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250605', 2654, 2654, '2025-06-05', '2025-06-06', 5, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 24, 11),
  ('Cartera_Agencia_Cobranding_Gestion_Temprana_20250610', 11351, 11360, '2025-06-10', '2025-06-10', 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 20, 11),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250610', 3072, 3072, '2025-06-10', '2025-06-10', 9, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 20, 11),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250610', 2591, 2591, '2025-06-10', '2025-06-10', 9, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 20, 11),
  ('Cartera_Agencia_Cobranding_Gestion_Temprana_20250611', 7605, 7610, '2025-06-12', '2025-06-12', 5, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 18, 11),

  -- ===== REGISTROS SIN FECHA DE CIERRE (en gestión) =====
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250617', 803, 803, '2025-06-17', '2025-06-17', 13, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, 2, 0),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250617', 1751, 1751, '2025-06-17', '2025-06-17', 13, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, 0),
  ('Cartera_Agencia_Cobranding_Gestion_Temprana_20250617', 10306, 10311, '2025-06-17', '2025-06-17', 9, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '2025-06-30', 13, 11),
  ('Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250617-1', 6631, 6632, '2025-06-18', '2025-06-18', 17, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, 0),
  ('Cartera_Agencia_Cobranding_Gestion_AN_20250617-1', 4485, 4487, '2025-06-18', '2025-06-18', 17, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, 0);

-- ##############################################################################
-- #                    ACTUALIZAR CAMPOS CALCULADOS                           #
-- ##############################################################################

-- Actualizar campos calculados automáticamente para los registros recién insertados
UPDATE mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
SET
  -- 🔧 CAMPOS DE FECHA Y TIEMPO
  MES_ASIGNACION = EXTRACT(MONTH FROM FECHA_ASIGNACION),
  ANNO_ASIGNACION = EXTRACT(YEAR FROM FECHA_ASIGNACION),
  SEMANA_ASIGNACION = EXTRACT(WEEK FROM FECHA_ASIGNACION),
  DIA_SEMANA_ASIGNACION = EXTRACT(DAYOFWEEK FROM FECHA_ASIGNACION),
  ES_DIA_HABIL = EXTRACT(DAYOFWEEK FROM FECHA_ASIGNACION) BETWEEN 2 AND 6,
  PERIODO_ASIGNACION = FORMAT_DATE('%Y-%m', FECHA_ASIGNACION),

  -- 🔧 MÉTRICAS CALCULADAS
  DENSIDAD_CLIENTES = SAFE_DIVIDE(CANT_COD_LUNA_UNIQUE, CANTCUENTA),
  DURACION_CAMPANA_DIAS_HABILES = CASE
    WHEN FECHA_CIERRE IS NOT NULL THEN
      -- Calcular días hábiles entre asignación y cierre
      (SELECT COUNT(*)
       FROM UNNEST(GENERATE_DATE_ARRAY(FECHA_ASIGNACION, FECHA_CIERRE)) AS d
       WHERE EXTRACT(DAYOFWEEK FROM d) BETWEEN 2 AND 6)
    ELSE NULL
  END,

  -- 🔧 FLAGS Y ESTADOS
  ES_CARTERA_ABIERTA = FECHA_CIERRE IS NULL OR FECHA_CIERRE > CURRENT_DATE(),

  -- 🔧 CLASIFICACIONES
  RANGO_VENCIMIENTO = CASE
    WHEN VENCIMIENTO <= 5 THEN 'INICIO_MES'
    WHEN VENCIMIENTO <= 15 THEN 'MEDIO_MES'
    WHEN VENCIMIENTO <= 25 THEN 'FIN_MES'
    ELSE 'FUERA_RANGO'
  END,

  TIPO_CARTERA = CASE
    WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'TEMPRANA') THEN 'TEMPRANA'
    WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'CF_ANN') THEN 'CUOTA_FRACCION'
    WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'AN') THEN 'ALTAS'
    WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'COBRANDING') THEN 'COBRANDING'
    ELSE 'OTRAS'
  END,

  ESTADO_CARTERA = CASE
    WHEN FECHA_CIERRE IS NULL THEN 'ACTIVA'
    WHEN FECHA_CIERRE > CURRENT_DATE() THEN 'PROGRAMADA'
    WHEN FECHA_CIERRE = CURRENT_DATE() THEN 'CERRANDO_HOY'
    ELSE 'CERRADA'
  END,

  FECHA_ACTUALIZACION = CURRENT_TIMESTAMP()

WHERE FECHA_ASIGNACION >= '2025-05-01'
  AND FECHA_CREACION >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- ##############################################################################
-- #                        VERIFICACIÓN COMPLETA                              #
-- ##############################################################################

-- Verificar los datos insertados con todos los campos
SELECT
  '📅 VERIFICACIÓN DE DATOS INSERTADOS' AS titulo,
  ARCHIVO,
  CANT_COD_LUNA_UNIQUE,
  CANTCUENTA,
  FECHA_ASIGNACION,
  FECHA_TRANDEUDA,
  VENCIMIENTO,
  FECHA_CIERRE,
  DIAS_GESTION,
  DIAS_PARA_CIERRE,
  TIPO_CARTERA,
  ESTADO_CARTERA,
  ES_CARTERA_ABIERTA,
  RANGO_VENCIMIENTO,
  DENSIDAD_CLIENTES
FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
WHERE FECHA_ASIGNACION >= '2025-05-01'
ORDER BY FECHA_ASIGNACION, ARCHIVO;

-- Estadísticas detalladas por tipo de cartera
SELECT
  '📊 ESTADÍSTICAS POR TIPO DE CARTERA' AS titulo,
  TIPO_CARTERA,
  COUNT(*) as cantidad_archivos,
  SUM(CANT_COD_LUNA_UNIQUE) as total_clientes,
  SUM(CANTCUENTA) as total_cuentas,
  AVG(DENSIDAD_CLIENTES) as densidad_promedio,
  AVG(DIAS_GESTION) as dias_gestion_promedio,
  SUM(CASE WHEN ES_CARTERA_ABIERTA THEN 1 ELSE 0 END) as carteras_abiertas,
  SUM(CASE WHEN NOT ES_CARTERA_ABIERTA THEN 1 ELSE 0 END) as carteras_cerradas
FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
WHERE FECHA_ASIGNACION >= '2025-05-01'
GROUP BY TIPO_CARTERA
ORDER BY total_clientes DESC;

-- Distribución por rango de vencimiento
SELECT
  '📈 DISTRIBUCIÓN POR RANGO DE VENCIMIENTO' AS titulo,
  RANGO_VENCIMIENTO,
  COUNT(*) as cantidad,
  FORMAT('%.1f%%', COUNT() * 100.0 / SUM(COUNT()) OVER()) as porcentaje,
  AVG(VENCIMIENTO) as vencimiento_promedio,
  SUM(CANT_COD_LUNA_UNIQUE) as total_clientes
FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
WHERE FECHA_ASIGNACION >= '2025-05-01'
GROUP BY RANGO_VENCIMIENTO
ORDER BY vencimiento_promedio;

-- Estado actual de las carteras
SELECT
  '🔄 ESTADO ACTUAL DE CARTERAS' AS titulo,
  ESTADO_CARTERA,
  COUNT(*) as cantidad,
  SUM(CANT_COD_LUNA_UNIQUE) as clientes_afectados,
  AVG(CASE WHEN DIAS_PARA_CIERRE IS NOT NULL THEN DIAS_PARA_CIERRE END) as dias_promedio_para_cierre
FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
WHERE FECHA_ASIGNACION >= '2025-05-01'
GROUP BY ESTADO_CARTERA
ORDER BY cantidad DESC;

-- ##############################################################################
-- #                    RESUMEN EJECUTIVO DE LA INSERCIÓN                      #
-- ##############################################################################

SELECT
  '🎯 RESUMEN EJECUTIVO' AS titulo,
  COUNT(*) as total_registros_insertados,
  SUM(CANT_COD_LUNA_UNIQUE) as total_clientes_unicos,
  SUM(CANTCUENTA) as total_cuentas,
  COUNT(DISTINCT TIPO_CARTERA) as tipos_cartera_diferentes,
  MIN(FECHA_ASIGNACION) as primera_asignacion,
  MAX(FECHA_ASIGNACION) as ultima_asignacion,
  COUNT(DISTINCT DATE_TRUNC(FECHA_ASIGNACION, MONTH)) as meses_cubiertos,
  FORMAT('%.2f', AVG(DENSIDAD_CLIENTES)) as densidad_promedio_clientes,
  SUM(CASE WHEN ES_CARTERA_ABIERTA THEN 1 ELSE 0 END) as carteras_activas,
  SUM(CASE WHEN NOT ES_CARTERA_ABIERTA THEN 1 ELSE 0 END) as carteras_cerradas
FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
WHERE FECHA_ASIGNACION >= '2025-05-01';

-- ##############################################################################
-- #                            DOCUMENTACIÓN                                  #
-- ##############################################################################

/*
🔧 MAPEO DE CAMPOS APLICADO:

ENTRADA → TABLA DESTINO:
- COD_LUNA → CANT_COD_LUNA_UNIQUE (clientes únicos)
- CUENTA → CANTCUENTA (cantidad de cuentas)
- VENCIMIENTO → VENCIMIENTO (día del mes como INTEGER)
- Resto de fechas → Formato ISO correcto

✅ CAMPOS CALCULADOS AUTOMÁTICAMENTE:
- MES_ASIGNACION, ANNO_ASIGNACION, SEMANA_ASIGNACION
- DIA_SEMANA_ASIGNACION, ES_DIA_HABIL
- PERIODO_ASIGNACION (YYYY-MM)
- DENSIDAD_CLIENTES (clientes/cuentas)
- DURACION_CAMPANA_DIAS_HABILES
- ES_CARTERA_ABIERTA
- RANGO_VENCIMIENTO (INICIO/MEDIO/FIN_MES)
- TIPO_CARTERA (TEMPRANA/CUOTA_FRACCION/ALTAS/COBRANDING)
- ESTADO_CARTERA (ACTIVA/PROGRAMADA/CERRADA)

📊 DATOS PROCESADOS:
- 25 registros insertados
- Período: Mayo-Junio 2025
- Tipos: TEMPRANA, CUOTA_FRACCION, ALTAS
- Mix de carteras activas y cerradas
- Cálculos automáticos aplicados
*/