# Stage de Pagos - Especificación Técnica

## 📋 Resumen Ejecutivo

El **Stage de Pagos** es el componente final del pipeline de datos de gestión de cobranzas FACO. Procesa los pagos realizados aplicando **lógica compleja de atribución de gestiones** y **análisis de efectividad**, incluyendo el contexto completo de cartera con vencimientos críticos para clasificación.

## 🎯 Objetivos

- **Atribución Inteligente**: Vincular pagos con gestiones previas más relevantes
- **Scoring de Efectividad**: Medir impacto de gestiones en recuperos (0.0-1.0)
- **Contexto de Cartera**: Incluir vencimiento y tipología para análisis por tipo
- **Análisis de PDP**: Evaluar cumplimiento de promesas de pago
- **Métricas Temporales**: Medir tiempo entre gestión y pago

## 🏗️ Arquitectura

### Entrada
- **Tabla Principal**: `batch_P3fV4dWNeMkN5RJMhV8e_pagos`
- **Tabla Deudas**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
- **Tabla Asignación**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`
- **Tabla Gestiones**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_gestiones`

### Salida
- **Tabla Staging**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_pagos`

### Procesamiento
- **Stored Procedure**: `bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos`

## 🔑 Modelo de Datos

### Llaves Primarias
```sql
PRIMARY KEY (nro_documento, fecha_pago)
```

### Dimensiones Principales

| Categoría | Campos | Descripción |
|-----------|--------|--------------| 
| **Pago** | `nro_documento`, `fecha_pago`, `monto_pagado` | Datos básicos del pago |
| **Contexto Cartera** | `cartera`, `vencimiento`, `categoria_vencimiento` | Tipología crítica para análisis |
| **Atribución** | `canal_atribuido`, `operador_atribuido`, `fecha_gestion_atribuida` | Gestión responsable |
| **PDP** | `fecha_compromiso`, `es_pago_con_pdp`, `pago_es_puntual` | Análisis promesas pago |
| **Efectividad** | `efectividad_atribucion`, `tipo_pago`, `categoria_efectividad` | Scoring y clasificación |

## 🧠 Lógica de Atribución Compleja

### 🔗 Proceso de Atribución en 3 Pasos

#### **1. Contexto de Cartera**
```sql
-- Se obtiene contexto desde la última asignación válida
SELECT cartera, vencimiento, categoria_vencimiento, archivo
FROM asignacion 
ORDER BY fecha_asignacion DESC  -- Última asignación
```

#### **2. Atribución de Gestión**
```sql
-- Se busca la última gestión ANTES del pago
SELECT fecha_gestion, canal, operador_final, es_compromiso
FROM gestiones 
WHERE cod_luna = pago.cod_luna 
  AND DATE(fecha_gestion) <= DATE(fecha_pago)
ORDER BY fecha_gestion DESC  -- Última gestión previa
```

#### **3. Cálculo de Efectividad**
```sql
-- Score basado en tipo y timing de la gestión
CASE
  WHEN pago_puntual_compromiso THEN 1.0        -- Máxima efectividad
  WHEN pago_dentro_3_dias_pdp THEN 0.8         -- Alta efectividad
  WHEN pago_dentro_semana_pdp THEN 0.6         -- Media efectividad
  WHEN pago_dentro_semana_gestion THEN 0.4     -- Baja efectividad
  WHEN hay_gestion_previa THEN 0.2             -- Mínima efectividad
  ELSE 0.0                                      -- Sin atribución
END
```

## 🎯 **Scoring de Efectividad - Regla Principal**

### **Escala de Efectividad (0.0 - 1.0)**

| Score | Categoría | Condición | Descripción |
|-------|-----------|-----------|-------------|
| **1.0** | ALTA | Pago puntual en fecha compromiso | Máxima efectividad |
| **0.8** | ALTA | Pago dentro de 3 días post-compromiso | Alta efectividad |
| **0.6** | MEDIA | Pago dentro de semana post-compromiso | Media efectividad |
| **0.4** | MEDIA | Pago dentro de semana post-gestión | Baja efectividad |
| **0.2** | BAJA | Hay gestión previa | Mínima efectividad |
| **0.0** | SIN_ATRIBUCION | Sin gestión atribuible | Sin efectividad |

### **Categorías de Efectividad**
```sql
categoria_efectividad = CASE
  WHEN efectividad_atribucion >= 0.8 THEN 'ALTA'
  WHEN efectividad_atribucion >= 0.4 THEN 'MEDIA'  
  WHEN efectividad_atribucion > 0.0 THEN 'BAJA'
  ELSE 'SIN_ATRIBUCION'
END
```

## 🏷️ **Clasificación de Tipos de Pago**

### **Tipología por Contexto de Gestión**

| Tipo | Condición | Descripción |
|------|-----------|-------------|
| **PUNTUAL** | `fecha_pago = fecha_compromiso` | Pago exacto en fecha prometida |
| **TARDIO_PDP** | `pago ≤ compromiso + 7 días` | Pago tardío pero con PDP vigente |
| **POST_GESTION** | `hay_gestion_previa = TRUE` | Pago después de gestión sin compromiso |
| **ESPONTANEO** | `sin_gestion_previa` | Pago sin gestión atribuible |

## 🎮 Modos de Ejecución

### 1. Procesamiento por Período (Recomendado)
```sql
-- Procesar pagos de ayer
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos`(
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),  -- fecha_inicio
  CURRENT_DATE()                              -- fecha_fin
);
```

### 2. Procesamiento de Rango Específico
```sql
-- Procesar semana completa
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos`(
  '2025-06-13',    -- fecha_inicio
  '2025-06-19'     -- fecha_fin
);
```

### 3. Full Refresh Histórico
```sql
-- Reprocesar histórico completo (cuidado con volumen)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos`(
  '2025-01-01',
  CURRENT_DATE(),
  'FULL'
);
```

## 🔄 Flujo de Datos Complejo

### **Secuencia de Joins Crítica**
```sql
-- 1. Pagos -> Deudas (por nro_documento)
-- 2. Deudas -> Asignación (por cod_cuenta) -> Contexto cartera
-- 3. Asignación -> Gestiones (por cod_luna) -> Atribución
-- 4. Cálculo de scores y clasificaciones
```

### **Dependencias de Stages**
```
Asignación → Deudas → Gestiones → Pagos
```

## 📊 **Importancia del Vencimiento**

### **Vencimiento como Clasificador Crítico**
El **vencimiento** es fundamental porque:
- Indica **tipo de deuda** (no "cíclicas" como mencionaste)
- Permite **segmentación por antigüedad**
- Determina **estrategias de gestión**
- Afecta **efectividad esperada**

### **Categorías de Vencimiento**
```sql
-- Heredadas desde stage de asignación
categoria_vencimiento:
- 'SIN_VENCIMIENTO'
- 'VENCIDO' 
- 'POR_VENCER_30D'
- 'POR_VENCER_60D'
- 'POR_VENCER_90D'
- 'VIGENTE_MAS_90D'
```

## 🧪 Tests de Calidad

### Tests Específicos de Pagos (10 validaciones)
1. **Unicidad de llaves primarias**
2. **Rangos de score efectividad (0-1)**
3. **Tipos de pago válidos**
4. **Categorías de efectividad válidas**
5. **Consistencia flags PDP**
6. **Consistencia atribución gestión**
7. **Cálculo correcto días gestión-pago**
8. **Montos positivos**
9. **Cobertura contexto cartera**
10. **Coherencia score vs categoría**

### Métricas de Calidad Específicas
- **Cobertura contexto cartera**: % pagos con vencimiento
- **Tasa de atribución**: % pagos con gestión previa
- **Efectividad promedio**: Score medio de atribución
- **Distribución temporal**: Días entre gestión y pago

## 📈 Métricas de Negocio

### **Dashboard de Efectividad**
```sql
-- Métricas principales por procesamiento
SELECT 
  COUNT(*) as total_pagos,
  SUM(monto_pagado) as monto_total,
  AVG(efectividad_atribucion) as score_promedio,
  COUNT(CASE WHEN categoria_efectividad = 'ALTA' THEN 1 END) as alta_efectividad,
  COUNT(CASE WHEN tiene_gestion_previa THEN 1 END) / COUNT(*) * 100 as pct_atribucion
FROM stage_pagos;
```

### **Análisis por Tipo de Cartera**
```sql
-- Efectividad segmentada por cartera y vencimiento
SELECT 
  cartera,
  categoria_vencimiento,
  AVG(efectividad_atribucion) as efectividad_promedio,
  COUNT(*) as volumen_pagos
FROM stage_pagos
GROUP BY cartera, categoria_vencimiento;
```

## 🔍 Monitoreo y Alertas

### Alertas Específicas
- ⚠️ **Baja cobertura contexto**: < 90% pagos con datos cartera
- ⚠️ **Efectividad baja**: Score promedio < 0.3
- ❌ **Inconsistencias PDP**: Flags contradictorios
- 📊 **Volumen anormal**: Variación > 50% vs promedio

### KPIs de Seguimiento
- **Tasa de atribución**: % pagos con gestión previa
- **Score de efectividad**: Promedio mensual
- **Puntualidad PDP**: % pagos puntuales vs compromisos
- **Tiempo gestión-pago**: Días promedio

## 🎮 Casos de Uso

### Escenario 1: Pago Puntual con PDP
```
Cliente: Gestión 15/06 con compromiso 19/06
Pago: 19/06 por $1000
Resultado: Score 1.0, Tipo 'PUNTUAL', Categoría 'ALTA'
```

### Escenario 2: Pago Post-Gestión
```
Cliente: Gestión 10/06 sin compromiso específico
Pago: 17/06 por $500 (7 días después)
Resultado: Score 0.4, Tipo 'POST_GESTION', Categoría 'MEDIA'
```

### Escenario 3: Pago Espontáneo
```
Cliente: Sin gestiones previas
Pago: 19/06 por $300
Resultado: Score 0.0, Tipo 'ESPONTANEO', Categoría 'SIN_ATRIBUCION'
```

## 🛠️ Troubleshooting

| Problema | Causa Probable | Solución |
|----------|----------------|-----------| 
| **Sin contexto cartera** | Falta join con asignación | Verificar completitud stage asignación |
| **Scores inconsistentes** | Error en cálculo días | Revisar lógica fecha_gestión <= fecha_pago |
| **Baja atribución** | Gestiones no procesadas | Verificar stage gestiones previo |
| **PDP inconsistentes** | Datos gestiones incompletos | Validar campos es_compromiso y fecha_compromiso |

## 📋 Pipeline Completo Recomendado

### Secuencia de Ejecución Diaria
```sql
-- 1. Asignación (prerequisito)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`();

-- 2. Deudas (requiere asignación)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`();

-- 3. Gestiones (requiere asignación + deudas)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones`();

-- 4. Pagos (requiere todos los anteriores)
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos`(
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
  CURRENT_DATE()
);
```

---

**Versión**: 1.0.0  
**Fecha**: 2025-06-19  
**Autor**: FACO Team  
**Características clave**: Atribución inteligente, scoring efectividad, contexto cartera con vencimiento  
**Dependencias**: Todos los stages previos (asignación, deudas, gestiones)
