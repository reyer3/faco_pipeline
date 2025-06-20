-- ================================================================
-- TABLA: Stage de Pagos - Pipeline de Cobranzas
-- ================================================================
-- Autor: FACO Team
-- Fecha: 2025-06-20
-- Versión: 1.2.0 - CORREGIDA clustering
-- Descripción: Tabla staging para pagos con atribución de gestiones
--              y análisis de efectividad
-- ================================================================

CREATE OR REPLACE TABLE `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos` (
  
  -- 🔑 LLAVES PRIMARIAS
  nro_documento STRING NOT NULL
    OPTIONS(description="Número de documento del pago"),
  fecha_pago DATE NOT NULL
    OPTIONS(description="Fecha del pago realizado"),
  
  -- 💰 DATOS DEL PAGO
  monto_pagado FLOAT64 NOT NULL
    OPTIONS(description="Monto cancelado/pagado"),
  
  -- 🔗 CONTEXTO DE CARTERA (desde asignación)
  cod_luna STRING
    OPTIONS(description="Código Luna del cliente (desde asignación)"),
  cod_cuenta STRING
    OPTIONS(description="Código de cuenta del cliente"),
  cartera STRING
    OPTIONS(description="Tipo de cartera (desde asignación)"),
  servicio STRING
    OPTIONS(description="Servicio del cliente (desde asignación)"),
  vencimiento DATE
    OPTIONS(description="Fecha de vencimiento (crítica para tipo)"),
  categoria_vencimiento STRING
    OPTIONS(description="Categorización del vencimiento"),
  id_archivo_asignacion STRING
    OPTIONS(description="Archivo de asignación original"),
  
  -- 🎯 ATRIBUCIÓN DE GESTIÓN
  fecha_gestion_atribuida DATE
    OPTIONS(description="Fecha de la gestión atribuida al pago"),
  canal_atribuido STRING
    OPTIONS(description="Canal de la gestión atribuida - DEFAULT: SIN_GESTION_PREVIA"),
  operador_atribuido STRING
    OPTIONS(description="Operador de la gestión atribuida - DEFAULT: SIN_GESTION_PREVIA"),
  
  -- 📅 DATOS DE COMPROMISO (PDP)
  fecha_compromiso DATE
    OPTIONS(description="Fecha de compromiso de pago (si existe)"),
  monto_compromiso FLOAT64
    OPTIONS(description="Monto comprometido"),
  
  -- 🏷️ FLAGS DE ANÁLISIS
  es_pago_con_pdp BOOLEAN
    OPTIONS(description="TRUE si el pago tiene promesa de pago asociada - DEFAULT: FALSE"),
  pdp_estaba_vigente BOOLEAN
    OPTIONS(description="TRUE si PDP estaba vigente al momento del pago - DEFAULT: FALSE"),
  pago_es_puntual BOOLEAN
    OPTIONS(description="TRUE si pago coincide exactamente con fecha compromiso - DEFAULT: FALSE"),
  tiene_gestion_previa BOOLEAN
    OPTIONS(description="TRUE si hay gestión atribuible - DEFAULT: FALSE"),
  
  -- 📊 MÉTRICAS DE TIEMPO
  dias_entre_gestion_y_pago INT64
    OPTIONS(description="Días transcurridos entre gestión y pago"),
  
  -- 🎯 SCORE DE EFECTIVIDAD
  efectividad_atribucion FLOAT64
    OPTIONS(description="Score de efectividad de atribución (0.0-1.0) - DEFAULT: 0.0"),
  
  -- 📈 CLASIFICACIÓN DE PAGO
  tipo_pago STRING NOT NULL
    OPTIONS(description="PUNTUAL, TARDIO_PDP, POST_GESTION, ESPONTANEO"),
  categoria_efectividad STRING NOT NULL
    OPTIONS(description="ALTA, MEDIA, BAJA, SIN_ATRIBUCION"),
  
  -- 🕒 METADATOS
  fecha_ultima_asignacion DATE
    OPTIONS(description="Fecha de la última asignación considerada"),
  fecha_ultima_gestion DATE
    OPTIONS(description="Fecha de la última gestión considerada"),
  fecha_actualizacion TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de última actualización"),
  fecha_proceso DATE NOT NULL
    OPTIONS(description="Fecha del proceso que generó el registro"),
  fecha_carga TIMESTAMP NOT NULL
    OPTIONS(description="Timestamp de carga inicial del registro")
)

-- 🔍 CONFIGURACIÓN DE PARTICIONADO Y CLUSTERING
PARTITION BY DATE(fecha_pago)
CLUSTER BY cod_luna, cartera, es_pago_con_pdp

-- 📋 OPCIONES DE TABLA
OPTIONS(
  description="Tabla staging para pagos con atribución de gestiones y análisis de efectividad. Incluye contexto de cartera, vencimiento y scoring de atribución.",
  labels=[("ambiente", "produccion"), ("pipeline", "faco_cobranzas"), ("capa", "staging")]
);

-- ================================================================
-- ÍNDICES Y CONSTRAINTS (comentados para BigQuery)
-- ================================================================

-- BigQuery no soporta constraints explícitos, pero documentamos las reglas:
-- PRIMARY KEY: (nro_documento, fecha_pago)
-- FOREIGN KEY: cod_luna -> asignacion.cod_luna
-- FOREIGN KEY: nro_documento -> deudas.nro_documento
-- CHECK: efectividad_atribucion BETWEEN 0.0 AND 1.0
-- CHECK: monto_pagado > 0
-- CHECK: tipo_pago IN ('PUNTUAL', 'TARDIO_PDP', 'POST_GESTION', 'ESPONTANEO')
-- CHECK: categoria_efectividad IN ('ALTA', 'MEDIA', 'BAJA', 'SIN_ATRIBUCION')

-- ================================================================
-- VALORES DEFAULT MANEJADOS EN STORED PROCEDURES
-- ================================================================

-- NOTA: Los valores DEFAULT se manejan en los stored procedures:
-- - canal_atribuido: 'SIN_GESTION_PREVIA' si es NULL
-- - operador_atribuido: 'SIN_GESTION_PREVIA' si es NULL
-- - es_pago_con_pdp: FALSE si es NULL
-- - pdp_estaba_vigente: FALSE si es NULL
-- - pago_es_puntual: FALSE si es NULL
-- - tiene_gestion_previa: FALSE si es NULL
-- - efectividad_atribucion: 0.0 si es NULL

-- ================================================================
-- COMENTARIOS DE NEGOCIO
-- ================================================================

-- Esta tabla consolida pagos con su contexto completo de cartera y gestión:
--
-- ATRIBUCIÓN DE GESTIONES:
-- - Se busca la última gestión antes del pago para cada cliente
-- - Se considera el contexto de cartera (vencimiento crítico para clasificación)
-- - Se evalúan promesas de pago (PDP) y su cumplimiento
--
-- SCORING DE EFECTIVIDAD:
-- - 1.0: Pago puntual en fecha compromiso
-- - 0.8: Pago dentro de 3 días post-compromiso
-- - 0.6: Pago dentro de semana post-compromiso
-- - 0.4: Pago dentro de semana post-gestión
-- - 0.2: Hay gestión previa
-- - 0.0: Sin gestión atribuible
--
-- CLASIFICACIÓN DE PAGOS:
-- - PUNTUAL: Coincide con fecha compromiso
-- - TARDIO_PDP: Fuera de fecha pero con PDP vigente
-- - POST_GESTION: Después de gestión sin compromiso específico
-- - ESPONTANEO: Sin gestión previa atribuible
--
-- REGLAS DE MERGE:
-- - Se actualizan datos de atribución si cambian gestiones
-- - Se preserva historial de pagos
-- - Se recalculan scores de efectividad
