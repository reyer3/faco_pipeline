-- ================================================================
-- TABLA: Stage de Asignación - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-20
-- Versión: 1.1.0 - CORREGIDA particionado
-- Descripción: Tabla staging para datos de asignación de cartera
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion` (
  
  -- 🔑 LLAVES PRIMARIAS
  cod_luna STRING NOT NULL
    OPTIONS(description="Código único del cliente en sistema Luna"),
  cod_cuenta STRING NOT NULL
    OPTIONS(description="Código de cuenta del cliente"),
  archivo STRING NOT NULL
    OPTIONS(description="Identificador del archivo de asignación"),
  
  -- 📊 DIMENSIONES CLIENTE
  cliente STRING
    OPTIONS(description="Nombre del cliente"),
  telefono STRING
    OPTIONS(description="Teléfono de contacto del cliente"),
  servicio STRING NOT NULL
    OPTIONS(description="Tipo de servicio/negocio del cliente"),
  segmento_gestion STRING NOT NULL
    OPTIONS(description="Segmento de gestión asignado"),
  zona_geografica STRING NOT NULL
    OPTIONS(description="Zona geográfica del cliente"),
  
  -- 📅 DIMENSIONES TEMPORALES
  fecha_vencimiento DATE
    OPTIONS(description="Fecha de vencimiento mínima"),
  fecha_asignacion DATE
    OPTIONS(description="Fecha de asignación de la cartera"),
  fecha_cierre DATE
    OPTIONS(description="Fecha de cierre de gestión"),
  fecha_trandeuda DATE
    OPTIONS(description="Fecha de transferencia de deuda"),
  dias_gestion INT64
    OPTIONS(description="Días disponibles para gestión"),
  
  -- 🎯 DIMENSIONES DE ANÁLISIS
  categoria_vencimiento STRING NOT NULL
    OPTIONS(description="Categorización del estado de vencimiento"),
  tipo_cartera STRING NOT NULL
    OPTIONS(description="Tipo de cartera según archivo"),
  objetivo_recupero FLOAT64 NOT NULL
    OPTIONS(description="Objetivo de recupero asignado (0.0-1.0)"),
  tipo_fraccionamiento STRING NOT NULL
    OPTIONS(description="Tipo de fraccionamiento de deuda"),
  
  -- 🔢 FLAGS Y MÉTRICAS
  flag_cliente_unico INT64
    OPTIONS(description="ROW_NUMBER para identificar cliente único por archivo"),
  saldo_dia FLOAT64
    OPTIONS(description="Saldo del día para la cuenta"),
  estado_cartera STRING NOT NULL
    OPTIONS(description="Estado actual de la cartera"),
  
  -- 🕒 METADATOS
  fecha_actualizacion TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de última actualización"),
  fecha_proceso DATE NOT NULL
    OPTIONS(description="Fecha del proceso que generó el registro"),
  fecha_carga TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de carga inicial del registro")
)

-- 🔍 CONFIGURACIÓN DE PARTICIONADO Y CLUSTERING
PARTITION BY DATE(fecha_asignacion)
CLUSTER BY cod_luna, tipo_cartera, segmento_gestion

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para datos de asignación de cartera de cobranzas. Contiene información procesada y enriquecida de clientes asignados para gestión.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (cod_luna, cod_cuenta, archivo)
-- FOREIGN KEY: archivo -> calendario.archivo
-- CHECK: objetivo_recupero BETWEEN 0.0 AND 1.0
-- CHECK: estado_cartera IN ('ACTIVO', 'CERRADO', 'SUSPENDIDO')

-- ================================================================
-- COMENTARIOS DE NEGOCIO
-- ================================================================

-- Esta tabla es el resultado del primer stage del pipeline de cobranzas.
-- Combina datos de asignación con información de calendario para crear
-- una vista enriquecida que será consumida por las capas analíticas.
--
-- Reglas de merge:
-- - Se añaden nuevos registros cuando no existe la combinación de llaves
-- - Se actualiza estado_cartera, saldo_dia y fecha_actualizacion en registros existentes
-- - Los datos históricos se preservan mediante particionado por fecha_asignacion
