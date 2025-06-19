# Stage de Gestiones - Especificación Técnica

## 📋 Resumen Ejecutivo

El **Stage de Gestiones** es el tercer componente del pipeline de datos de gestión de cobranzas FACO. Unifica y procesa las gestiones realizadas por **canales BOT y HUMANO**, aplicando homologación de respuestas, operadores y calculando métricas de efectividad.

## 🎯 Objetivos

- **Unificación de Canales**: Consolidar gestiones BOT (voicebot) y HUMANO (mibotair)
- **Homologación**: Estandarizar respuestas y operadores usando tablas de homologación
- **Métricas de Efectividad**: Calcular contactabilidad, compromisos y conversión
- **Medibilidad**: Determinar qué gestiones cuentan para análisis de performance
- **Análisis Temporal**: Enriquecer con dimensiones de tiempo para reportería

## 🏗️ Arquitectura

### Entrada
- **Gestiones BOT**: `voicebot_P3fV4dWNeMkN5RJMhV8e`
- **Gestiones HUMANO**: `mibotair_P3fV4dWNeMkN5RJMhV8e`
- **Homologación BOT**: `homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot`
- **Homologación HUMANO**: `homologacion_P3fV4dWNeMkN5RJMhV8e_v2`
- **Homologación USUARIOS**: `homologacion_P3fV4dWNeMkN5RJMhV8e_usuarios`

### Salida
- **Tabla Staging**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`

### Procesamiento
- **Stored Procedure**: `bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`

## 🔑 Modelo de Datos

### Llaves Primarias
```sql
PRIMARY KEY (cod_luna, fecha_gestion, canal_origen, secuencia_gestion)
```

### Dimensiones Principales

| Categoría | Campos | Descripción |
|-----------|--------|--------------| 
| **Gestión** | `cod_luna`, `fecha_gestion`, `canal_origen` | Identificación de la gestión |
| **Operador** | `operador_final`, `nombre_agente_original` | Quien realizó la gestión |
| **Respuesta** | `grupo_respuesta`, `nivel_1`, `nivel_2` | Respuestas homologadas |
| **Compromisos** | `es_compromiso`, `monto_compromiso`, `fecha_compromiso` | Acuerdos de pago |
| **Efectividad** | `es_contacto_efectivo`, `es_gestion_medible` | Métricas de performance |

## 🔄 Proceso de Unificación

### 1. Extracción por Canal

#### **Gestiones BOT**
```sql
SELECT 
  SAFE_CAST(document AS INT64) AS cod_luna,
  DATE(date) AS fecha_gestion,
  COALESCE(management, 'SIN_MANAGEMENT') AS management_original,
  COALESCE(compromiso, '') AS compromiso_original,
  'SISTEMA_BOT' AS nombre_agente_original,
  'BOT' AS canal_origen
FROM voicebot_P3fV4dWNeMkN5RJMhV8e
```

#### **Gestiones HUMANO**
```sql
SELECT 
  SAFE_CAST(document AS INT64) AS cod_luna,
  DATE(date) AS fecha_gestion,
  COALESCE(management, 'SIN_MANAGEMENT') AS management_original,
  COALESCE(nombre_agente, 'SIN_AGENTE') AS nombre_agente_original,
  'HUMANO' AS canal_origen
FROM mibotair_P3fV4dWNeMkN5RJMhV8e
```

### 2. Proceso de Homologación

#### **Homologación BOT**
```sql
LEFT JOIN homologacion_voicebot AS h_bot 
  ON canal_origen = 'BOT' 
  AND management_original = h_bot.bot_management 
  AND sub_management_original = h_bot.bot_sub_management 
  AND compromiso_original = h_bot.bot_compromiso
```

#### **Homologación HUMANO**
```sql
LEFT JOIN homologacion_v2 AS h_call 
  ON canal_origen = 'HUMANO' 
  AND management_original = h_call.management
```

#### **Homologación USUARIOS**
```sql
LEFT JOIN homologacion_usuarios AS h_user 
  ON canal_origen = 'HUMANO' 
  AND nombre_agente_original = h_user.usuario
```

## 🎯 Lógica de Negocio

### Operador Final
```sql
operador_final = CASE 
  WHEN canal_origen = 'BOT' THEN 'SISTEMA_BOT'
  WHEN canal_origen = 'HUMANO' THEN COALESCE(h_user.usuario, nombre_agente_original, 'SIN_AGENTE')
  ELSE 'NO_IDENTIFICADO'
END
```

### Grupo de Respuesta Homologado
```sql
grupo_respuesta = COALESCE(
  CASE
    WHEN canal_origen = 'BOT' THEN h_bot.contactabilidad_homologada
    WHEN canal_origen = 'HUMANO' THEN h_call.contactabilidad
  END,
  management_original,
  'NO_IDENTIFICADO'
)
```

### Flag de Compromiso/PDP
```sql
es_compromiso = CASE
  WHEN canal_origen = 'BOT' THEN COALESCE(h_bot.es_pdp_homologado, 0) = 1
  WHEN canal_origen = 'HUMANO' THEN UPPER(COALESCE(h_call.pdp, '')) = 'SI'
  ELSE FALSE
END
```

### Contacto Efectivo
```sql
es_contacto_efectivo = CASE 
  WHEN UPPER(management_original) LIKE '%CONTACTO_EFECTIVO%' 
       OR UPPER(management_original) LIKE '%EFECTIVO%' 
  THEN TRUE
  ELSE FALSE 
END
```

### Medibilidad de Gestión
```sql
es_gestion_medible = CASE 
  WHEN tiene_asignacion = TRUE OR tiene_deuda = TRUE THEN TRUE 
  ELSE FALSE 
END
```

## 🔗 Integración con Otros Stages

### Join con Asignación
```sql
LEFT JOIN stg_asignacion AS asig
  ON cod_luna = asig.cod_luna
  AND fecha_gestion = asig.fecha_asignacion
```

### Join con Deudas
```sql
LEFT JOIN stg_deudas AS deuda
  ON CAST(cod_luna AS STRING) = deuda.cod_cuenta
  AND fecha_gestion = deuda.fecha_deuda
```

## 📊 Métricas Calculadas

### Secuencia de Gestiones
- **secuencia_gestion**: Orden de gestiones por cliente/fecha/canal
- **es_primera_gestion_dia**: TRUE para la primera gestión del cliente en el día

### Dimensiones Temporales
- **dia_semana**: Día de la semana de la gestión
- **semana_mes**: Semana del mes (1-5)
- **es_fin_semana**: TRUE si es sábado o domingo

### Flags de Análisis
- **es_contacto_efectivo**: Contacto real con el cliente
- **es_compromiso**: Acuerdo de pago establecido
- **es_gestion_medible**: Cuenta para métricas de performance

## 🎮 Modos de Ejecución

### 1. Automático por Fecha (Recomendado)
```sql
-- Procesa todos los canales para la fecha
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`(
  '2025-06-19'  -- fecha_proceso
);
```

### 2. Filtro por Canal
```sql
-- Solo gestiones BOT
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`(
  CURRENT_DATE(),
  'BOT'  -- canal_filter
);

-- Solo gestiones HUMANO
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`(
  CURRENT_DATE(),
  'HUMANO'  -- canal_filter
);
```

### 3. Pipeline Completo
```sql
-- Secuencia recomendada
DECLARE fecha_proceso DATE DEFAULT CURRENT_DATE();

-- 1. Asignación (base)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(fecha_proceso);

-- 2. Deudas (requiere asignación)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(fecha_proceso);

-- 3. Gestiones (requiere asignación y deudas para medibilidad)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`(fecha_proceso);
```

## 🧪 Tests de Calidad

### Tests Específicos de Gestiones
1. **Unicidad de llaves primarias**
2. **Canales válidos** (BOT, HUMANO)
3. **Secuencia de gestiones** (>= 1)
4. **Consistencia homologación BOT/HUMANO**
5. **Validación montos compromiso**
6. **Flags primera gestión día** (solo uno por cliente/día)
7. **Operadores BOT** (deben ser SISTEMA_BOT)
8. **Medibilidad consistente** con asignación/deudas
9. **Días de semana válidos**
10. **Análisis de homologación** por canal

### Métricas de Calidad
- **% Homologación**: Respuestas homologadas vs originales
- **% Efectividad**: Contactos efectivos / Total gestiones
- **% Conversión**: Compromisos / Contactos efectivos
- **Cobertura temporal**: Distribución por días de semana

## 📈 Métricas de Negocio

### Por Canal
```sql
SELECT 
  canal_origen,
  COUNT(*) as total_gestiones,
  COUNT(DISTINCT cod_luna) as clientes_unicos,
  COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) as contactos_efectivos,
  COUNT(CASE WHEN es_compromiso THEN 1 END) as compromisos,
  ROUND(COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) / COUNT(*) * 100, 2) as pct_efectividad
FROM stg_gestiones
GROUP BY canal_origen;
```

### Efectividad General
- **Total gestiones**: Volumen de actividad
- **Clientes únicos**: Alcance
- **% Efectividad**: (Contactos efectivos / Total) * 100
- **% Conversión**: (Compromisos / Contactos efectivos) * 100
- **Monto promedio compromiso**: Calidad de acuerdos

## 🔍 Monitoreo

### Alertas Específicas
- **Baja homologación**: < 80% respuestas homologadas
- **Sin gestiones detectadas**: Cero registros para la fecha
- **Efectividad anormal**: Variación > 30% vs histórico
- **Montos inconsistentes**: Compromisos sin monto o viceversa

### Dashboard de Homologación
```sql
SELECT 
  canal_origen,
  COUNT(*) as total,
  COUNT(CASE WHEN grupo_respuesta != management_original THEN 1 END) as homologadas,
  ROUND(COUNT(CASE WHEN grupo_respuesta != management_original THEN 1 END) / COUNT(*) * 100, 2) as pct_homologacion
FROM stg_gestiones
GROUP BY canal_origen;
```

## 📈 Optimización

### Particionado
- **Partición**: `DATE(fecha_gestion)`
- **Beneficio**: Mejora consultas temporales

### Clustering
- **Campos**: `cod_luna`, `canal_origen`, `es_contacto_efectivo`
- **Beneficio**: Optimiza filtros por cliente y efectividad

## 🛠️ Casos de Uso

### Análisis de Efectividad Diaria
```sql
SELECT 
  fecha_gestion,
  canal_origen,
  COUNT(*) as gestiones,
  ROUND(COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) / COUNT(*) * 100, 2) as efectividad
FROM stg_gestiones
WHERE fecha_gestion >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY fecha_gestion, canal_origen
ORDER BY fecha_gestion DESC;
```

### Ranking de Operadores
```sql
SELECT 
  operador_final,
  canal_origen,
  COUNT(*) as total_gestiones,
  ROUND(COUNT(CASE WHEN es_contacto_efectivo THEN 1 END) / COUNT(*) * 100, 2) as efectividad,
  SUM(monto_compromiso) as monto_compromisos
FROM stg_gestiones
WHERE fecha_gestion = CURRENT_DATE()
  AND canal_origen = 'HUMANO'
GROUP BY operador_final, canal_origen
ORDER BY efectividad DESC;
```

### Análisis de Compromiso por Hora
```sql
SELECT 
  EXTRACT(HOUR FROM timestamp_original) as hora,
  COUNT(CASE WHEN es_compromiso THEN 1 END) as compromisos,
  ROUND(AVG(monto_compromiso), 2) as monto_promedio
FROM stg_gestiones
WHERE fecha_gestion = CURRENT_DATE()
  AND es_compromiso = TRUE
GROUP BY hora
ORDER BY hora;
```

## 🛠️ Troubleshooting

| Problema | Causa Probable | Solución |
|----------|----------------|-----------| 
| **Baja homologación BOT** | Nuevas respuestas sin mapear | Actualizar tabla homologacion_voicebot |
| **Operadores sin homologar** | Agentes nuevos | Actualizar tabla homologacion_usuarios |
| **Gestiones no medibles** | Falta asignación/deuda | Verificar ejecución previa de stages |
| **Montos sin compromiso** | Error en join homologación | Revisar mapeo PDP en homologacion_v2 |

---

**Versión**: 1.0.0  
**Fecha**: 2025-06-19  
**Autor**: FACO Team  
**Dependencias**: Stage de Asignación y Stage de Deudas (para medibilidad)  
**Funcionalidad**: Unificación BOT + HUMANO con homologación completa
