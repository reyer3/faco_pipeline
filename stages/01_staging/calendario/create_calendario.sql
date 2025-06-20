-- ##############################################################################
-- #                    DDL TABLA CALENDARIO v5 - CON PREFIJO                  #
-- #                    bi_P3fV4dWNeMkN5RJMhV8e                                 #
-- #              Sin columnas GENERATED - Compatible con BigQuery              #
-- ##############################################################################

CREATE TABLE IF NOT EXISTS mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
(
  -- ==================================================================
  -- 🔧 CAMPOS OBLIGATORIOS DE ENTRADA
  -- ==================================================================

  ARCHIVO STRING NOT NULL,                         -- Nombre del archivo de asignación
  CANT_COD_LUNA_UNIQUE INT64 NOT NULL,            -- Cantidad de códigos luna únicos
  CANTCUENTA INT64 NOT NULL,                      -- Cantidad de cuentas
  FECHA_ASIGNACION DATE NOT NULL,                 -- Fecha de asignación de la cartera
  FECHA_TRANDEUDA DATE NOT NULL,                  -- Fecha de la transacción de deuda
  VENCIMIENTO INT64 NOT NULL,                     -- Día de la cíclica (1-31)

  -- ==================================================================
  -- 🔧 CAMPOS OPCIONALES (Se pueden actualizar durante la gestión)
  -- ==================================================================

  FECHA_CIERRE DATE,                              -- Fecha de cierre real (se puede dar más adelante)
  FECHA_CIERRE_PLANIFICADA DATE,                  -- Fecha de cierre planificada inicialmente

  -- ==================================================================
  -- 🔧 CAMPOS CALCULADOS (Se actualizan via stored procedure)
  -- ==================================================================

  DIAS_GESTION INT64,                             -- Días hábiles entre asignación y cierre
  DIAS_PARA_CIERRE INT64,                         -- Días hábiles restantes hasta cierre

  -- ==================================================================
  -- 🔧 CAMPOS DERIVADOS DE FECHA
  -- ==================================================================

  MES_ASIGNACION INT64,                           -- Mes de asignación
  ANNO_ASIGNACION INT64,                          -- Año de asignación
  SEMANA_ASIGNACION INT64,                        -- Semana de asignación
  DIA_SEMANA_ASIGNACION INT64,                    -- Día de la semana (1=domingo, 7=sábado)
  ES_DIA_HABIL BOOL,                              -- True si es día hábil (L-V)
  PERIODO_ASIGNACION STRING,                      -- Formato YYYY-MM

  -- ==================================================================
  -- 🔧 MÉTRICAS DE GESTIÓN
  -- ==================================================================

  DENSIDAD_CLIENTES FLOAT64,                     -- Ratio clientes/cuentas
  DURACION_CAMPANA_DIAS_HABILES INT64,           -- Duración real en días hábiles
  ES_CARTERA_ABIERTA BOOL,                       -- True si cartera sigue activa

  -- ==================================================================
  -- 🔧 CATEGORIZACIÓN
  -- ==================================================================

  RANGO_VENCIMIENTO STRING,                      -- INICIO_MES, MEDIO_MES, etc.
  TIPO_CARTERA STRING,                           -- TEMPRANA, ALTAS, etc.
  ESTADO_CARTERA STRING,                         -- ACTIVA, CERRADA, VENCIDA, etc.

  -- ==================================================================
  -- 🔧 METADATA Y CONTROL
  -- ==================================================================

  FECHA_CREACION TIMESTAMP NOT NULL,
  FECHA_ACTUALIZACION TIMESTAMP NOT NULL
)

-- ==================================================================
-- 🔧 OPTIMIZACIÓN DE PERFORMANCE
-- ==================================================================
PARTITION BY FECHA_ASIGNACION                     -- Partición por fecha de asignación
CLUSTER BY ARCHIVO, VENCIMIENTO                   -- Clustering por archivo y vencimiento

-- ==================================================================
-- 🔧 OPCIONES DE TABLA
-- ==================================================================
OPTIONS(
  description="Tabla calendario v5 para control de asignaciones y campañas de gestión. Días hábiles calculados automáticamente via stored procedures. Prefijo: bi_P3fV4dWNeMkN5RJMhV8e",
  labels=[("proyecto", "faco"), ("tipo", "calendario"), ("version", "v5"), ("entorno", "prod")]
);

-- ##############################################################################
-- #           STORED PROCEDURE PARA CALCULAR CAMPOS AUTOMÁTICAMENTE           #
-- ##############################################################################

CREATE OR REPLACE PROCEDURE mibot-222814.BI_USA.sp_calcular_calendario_v5(
  IN p_archivo STRING   -- Si es NULL, calcula todos los archivos
)
BEGIN

  DECLARE v_registros_actualizados INT64 DEFAULT 0;

  SELECT CONCAT('🔄 Calculando campos automáticos del calendario v5...') AS estado;

  -- Función UDF para calcular días hábiles entre dos fechas
  CREATE TEMP FUNCTION calcular_dias_habiles(fecha_inicio DATE, fecha_fin DATE)
  RETURNS INT64
  LANGUAGE js AS '''
    if (!fecha_inicio || !fecha_fin) return null;

    let inicio = new Date(fecha_inicio);
    let fin = new Date(fecha_fin);
    let dias_habiles = 0;

    // Asegurar que inicio <= fin
    if (inicio > fin) {
      let temp = inicio;
      inicio = fin;
      fin = temp;
    }

    let current = new Date(inicio);
    while (current <= fin) {
      let diaSemana = current.getDay(); // 0=domingo, 6=sábado
      if (diaSemana >= 1 && diaSemana <= 5) { // Lunes(1) a Viernes(5)
        dias_habiles++;
      }
      current.setDate(current.getDate() + 1);
    }

    return dias_habiles;
  ''';

  -- Actualizar todos los campos calculados
  UPDATE mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
  SET
    -- 🔧 DÍAS HÁBILES CALCULADOS
    DIAS_GESTION = CASE
      WHEN FECHA_CIERRE IS NOT NULL THEN
        calcular_dias_habiles(FECHA_ASIGNACION, FECHA_CIERRE)
      WHEN FECHA_CIERRE_PLANIFICADA IS NOT NULL THEN
        calcular_dias_habiles(FECHA_ASIGNACION, FECHA_CIERRE_PLANIFICADA)
      ELSE
        calcular_dias_habiles(FECHA_ASIGNACION, DATE_ADD(FECHA_ASIGNACION, INTERVAL 42 DAY))
    END,

    DIAS_PARA_CIERRE = CASE
      WHEN FECHA_CIERRE IS NOT NULL AND FECHA_CIERRE <= CURRENT_DATE() THEN 0
      WHEN FECHA_CIERRE IS NOT NULL THEN
        calcular_dias_habiles(CURRENT_DATE(), FECHA_CIERRE)
      WHEN FECHA_CIERRE_PLANIFICADA IS NOT NULL THEN
        calcular_dias_habiles(CURRENT_DATE(), FECHA_CIERRE_PLANIFICADA)
      ELSE -1  -- Pendiente de definir
    END,

    -- 🔧 CAMPOS DERIVADOS DE FECHA
    MES_ASIGNACION = EXTRACT(MONTH FROM FECHA_ASIGNACION),
    ANNO_ASIGNACION = EXTRACT(YEAR FROM FECHA_ASIGNACION),
    SEMANA_ASIGNACION = EXTRACT(WEEK FROM FECHA_ASIGNACION),
    DIA_SEMANA_ASIGNACION = EXTRACT(DAYOFWEEK FROM FECHA_ASIGNACION),
    ES_DIA_HABIL = EXTRACT(DAYOFWEEK FROM FECHA_ASIGNACION) BETWEEN 2 AND 6,
    PERIODO_ASIGNACION = FORMAT_DATE('%Y-%m', FECHA_ASIGNACION),

    -- 🔧 MÉTRICAS DE GESTIÓN
    DENSIDAD_CLIENTES = SAFE_DIVIDE(CANT_COD_LUNA_UNIQUE, CANTCUENTA),
    DURACION_CAMPANA_DIAS_HABILES = CASE
      WHEN FECHA_CIERRE IS NOT NULL THEN
        calcular_dias_habiles(FECHA_ASIGNACION, FECHA_CIERRE)
      ELSE NULL
    END,
    ES_CARTERA_ABIERTA = (FECHA_CIERRE IS NULL OR FECHA_CIERRE > CURRENT_DATE()),

    -- 🔧 CATEGORIZACIÓN
    RANGO_VENCIMIENTO = CASE
      WHEN VENCIMIENTO BETWEEN 1 AND 5 THEN 'INICIO_MES'
      WHEN VENCIMIENTO BETWEEN 6 AND 15 THEN 'MEDIO_MES'
      WHEN VENCIMIENTO BETWEEN 16 AND 25 THEN 'FINAL_MES'
      WHEN VENCIMIENTO BETWEEN 26 AND 31 THEN 'CIERRE_MES'
      ELSE 'INVALIDO'
    END,

    TIPO_CARTERA = CASE
      WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'TEMPRANA') THEN 'TEMPRANA'
      WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'CF_ANN') THEN 'CUOTA_FRACCION'
      WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'AN') THEN 'ALTAS'
      WHEN CONTAINS_SUBSTR(UPPER(ARCHIVO), 'COBRANDING') THEN 'COBRANDING'
      ELSE 'OTRAS'
    END,

    ESTADO_CARTERA = CASE
      WHEN FECHA_CIERRE IS NOT NULL AND FECHA_CIERRE <= CURRENT_DATE() THEN 'CERRADA'
      WHEN FECHA_CIERRE IS NULL AND FECHA_CIERRE_PLANIFICADA IS NULL THEN 'PENDIENTE_FECHA_CIERRE'
      WHEN FECHA_CIERRE_PLANIFICADA <= CURRENT_DATE() AND FECHA_CIERRE IS NULL THEN 'VENCIDA'
      WHEN COALESCE(FECHA_CIERRE, FECHA_CIERRE_PLANIFICADA) <= DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY) THEN 'PROXIMA_A_CERRAR'
      ELSE 'ACTIVA'
    END,

    -- 🔧 METADATA
    FECHA_ACTUALIZACION = CURRENT_TIMESTAMP()

  WHERE (p_archivo IS NULL OR ARCHIVO = p_archivo);

  SET v_registros_actualizados = @@row_count;

  SELECT
    CONCAT('✅ Campos calculados actualizados: ', CAST(v_registros_actualizados AS STRING), ' registros') AS resultado;

  -- Mostrar estadísticas de los cálculos
  SELECT
    '📊 ESTADÍSTICAS DE CÁLCULOS' AS titulo,
    COUNT(*) as total_registros,
    AVG(DIAS_GESTION) as promedio_dias_gestion_habiles,
    COUNT(CASE WHEN DIAS_PARA_CIERRE > 0 THEN 1 END) as carteras_activas,
    COUNT(CASE WHEN DIAS_PARA_CIERRE = 0 THEN 1 END) as carteras_cerradas,
    COUNT(CASE WHEN DIAS_PARA_CIERRE = -1 THEN 1 END) as carteras_pendientes_fecha
  FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
  WHERE (p_archivo IS NULL OR ARCHIVO = p_archivo);

END;

-- ##############################################################################
-- #              STORED PROCEDURE PARA INSERTAR CON CÁLCULOS                  #
-- ##############################################################################

CREATE OR REPLACE PROCEDURE mibot-222814.BI_USA.sp_insertar_calendario_v5(
  IN p_archivo STRING,
  IN p_cant_cod_luna_unique INT64,
  IN p_cantcuenta INT64,
  IN p_fecha_asignacion DATE,
  IN p_fecha_trandeuda DATE,
  IN p_vencimiento INT64,
  IN p_fecha_cierre_planificada DATE,
  IN p_fecha_cierre DATE
)
BEGIN

  -- Insertar registro básico
  INSERT INTO mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
  (
    ARCHIVO, CANT_COD_LUNA_UNIQUE, CANTCUENTA, FECHA_ASIGNACION,
    FECHA_TRANDEUDA, VENCIMIENTO, FECHA_CIERRE_PLANIFICADA, FECHA_CIERRE,
    FECHA_CREACION, FECHA_ACTUALIZACION
  )
  VALUES
  (
    p_archivo, p_cant_cod_luna_unique, p_cantcuenta, p_fecha_asignacion,
    p_fecha_trandeuda, p_vencimiento, p_fecha_cierre_planificada, p_fecha_cierre,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );

  -- Calcular campos automáticamente para este archivo
  CALL mibot-222814.BI_USA.sp_calcular_calendario_v5(p_archivo);

  SELECT CONCAT('✅ Archivo insertado y calculado: ', p_archivo) AS resultado;

END;

-- ##############################################################################
-- #         STORED PROCEDURE PARA ACTUALIZAR FECHAS DE CIERRE                 #
-- ##############################################################################

CREATE OR REPLACE PROCEDURE mibot-222814.BI_USA.sp_actualizar_fechas_cierre_calendario_v5(
  IN p_patron_archivo STRING,
  IN p_nueva_fecha_cierre DATE
)
BEGIN

  DECLARE v_archivos_actualizados INT64 DEFAULT 0;

  -- Actualizar fechas de cierre
  UPDATE mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
  SET
    FECHA_CIERRE = p_nueva_fecha_cierre,
    FECHA_ACTUALIZACION = CURRENT_TIMESTAMP()
  WHERE ARCHIVO LIKE p_patron_archivo
    AND (FECHA_CIERRE IS NULL OR FECHA_CIERRE != p_nueva_fecha_cierre);

  SET v_archivos_actualizados = @@row_count;

  -- Recalcular campos automáticamente
  CALL mibot-222814.BI_USA.sp_calcular_calendario_v5(NULL);

  SELECT
    CONCAT('✅ Fechas actualizadas y recalculadas: ', CAST(v_archivos_actualizados AS STRING), ' archivos') AS resultado;

END;

-- ##############################################################################
-- #                          EJEMPLOS DE USO                                  #
-- ##############################################################################

/*
-- EJEMPLO 1: Insertar nuevo archivo con cálculos automáticos
CALL mibot-222814.BI_USA.sp_insertar_calendario_v5(
  'FACO_TEMPRANA_2025_01_15.txt',  -- archivo
  2500,                             -- cant_cod_luna_unique
  3200,                             -- cantcuenta
  '2025-01-15',                     -- fecha_asignacion
  '2025-01-14',                     -- fecha_trandeuda
  21,                               -- vencimiento
  '2025-02-15',                     -- fecha_cierre_planificada
  NULL                              -- fecha_cierre (se define después)
);

-- EJEMPLO 2: Inserción manual (si prefieres INSERT directo)
INSERT INTO mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
(
  ARCHIVO, CANT_COD_LUNA_UNIQUE, CANTCUENTA, FECHA_ASIGNACION,
  FECHA_TRANDEUDA, VENCIMIENTO, FECHA_CIERRE_PLANIFICADA,
  FECHA_CREACION, FECHA_ACTUALIZACION
)
VALUES
(
  'FACO_ALTAS_2025_01_10.txt',     -- ARCHIVO
  1800,                             -- CANT_COD_LUNA_UNIQUE
  2100,                             -- CANTCUENTA
  '2025-01-10',                     -- FECHA_ASIGNACION
  '2025-01-09',                     -- FECHA_TRANDEUDA
  5,                                -- VENCIMIENTO
  '2025-02-28',                     -- FECHA_CIERRE_PLANIFICADA
  CURRENT_TIMESTAMP(),              -- FECHA_CREACION
  CURRENT_TIMESTAMP()               -- FECHA_ACTUALIZACION
);

-- Después del INSERT manual, ejecutar cálculos
CALL mibot-222814.BI_USA.sp_calcular_calendario_v5('FACO_ALTAS_2025_01_10.txt');

-- EJEMPLO 3: Actualizar fechas de cierre masivamente
CALL mibot-222814.BI_USA.sp_actualizar_fechas_cierre_calendario_v5(
  'FACO_TEMPRANA%',
  '2025-02-20'
);

-- EJEMPLO 4: Recalcular todos los campos automáticos
CALL mibot-222814.BI_USA.sp_calcular_calendario_v5(NULL);

-- EJEMPLO 5: Consultar estado actual
SELECT
  ARCHIVO,
  FECHA_ASIGNACION,
  FECHA_CIERRE_PLANIFICADA,
  FECHA_CIERRE,
  DIAS_GESTION,
  DIAS_PARA_CIERRE,
  ESTADO_CARTERA,
  TIPO_CARTERA
FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5
ORDER BY FECHA_ASIGNACION DESC;
*/

-- ##############################################################################
-- #                   VISTA SIMPLIFICADA PARA CONSULTAS                       #
-- ##############################################################################

CREATE OR REPLACE VIEW mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_view_calendario_simple AS
SELECT
  ARCHIVO,
  CANT_COD_LUNA_UNIQUE,
  CANTCUENTA,
  FECHA_ASIGNACION,
  FECHA_TRANDEUDA,
  VENCIMIENTO,
  FECHA_CIERRE_PLANIFICADA,
  FECHA_CIERRE,
  DIAS_GESTION,
  DIAS_PARA_CIERRE,
  TIPO_CARTERA,
  ESTADO_CARTERA,

  -- Campos calculados en tiempo real para verificación
  CASE
    WHEN DIAS_PARA_CIERRE = -1 THEN '🔸 PENDIENTE_FECHA_CIERRE'
    WHEN DIAS_PARA_CIERRE <= 0 THEN '🔴 VENCIDA/CERRADA'
    WHEN DIAS_PARA_CIERRE <= 5 THEN '🟡 PROXIMA_A_CERRAR'
    ELSE '🟢 ACTIVA'
  END AS ESTADO_VISUAL,

  FORMAT('%.1f%%', DENSIDAD_CLIENTES * 100) AS DENSIDAD_CLIENTES_PCT

FROM mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5;