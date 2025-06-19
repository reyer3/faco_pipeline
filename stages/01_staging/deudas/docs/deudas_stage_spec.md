# Stage de Deudas - Especificación Técnica

## 📋 Resumen Ejecutivo

El **Stage de Deudas** es el segundo componente del pipeline de datos de gestión de cobranzas FACO. Procesa las deudas diarias refrescadas aplicando **lógica específica de medibilidad basada en la coincidencia entre la fecha del archivo TRAN_DEUDA y el campo FECHA_TRANDEUDA del calendario**.

## 🎯 Objetivos

- **Procesamiento Diario**: Manejo de deudas refrescadas diariamente desde archivos TRAN_DEUDA
- **Medibilidad Precisa**: Determinar clientes medibles por coincidencia fecha archivo vs FECHA_TRANDEUDA
- **Filtrado Inteligente**: Identificar clientes gestionables basado en asignación
- **Construcción de Fechas**: Extraer fecha del nombre de archivo usando regex
- **Integración**: Join con calendario y asignación para determinar medibilidad

## 🏗️ Arquitectura

### Entrada
- **Tabla Principal**: `batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
- **Tabla Calendario**: `bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
- **Tabla Asignación**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`

### Salida
- **Tabla Staging**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`

### Procesamiento
- **Stored Procedure**: `bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`

## 🔑 Modelo de Datos

### Llaves Primarias
```sql
PRIMARY KEY (cod_cuenta, nro_documento, archivo, fecha_deuda)
```

### Dimensiones Principales

| Categoría | Campos | Descripción |
|-----------|--------|--------------| 
| **Deuda** | `cod_cuenta`, `nro_documento`, `monto_exigible` | Identificación y valor de deuda |
| **Temporal** | `fecha_deuda`, `fecha_trandeuda` | Fechas clave para medibilidad |
| **Negocio** | `es_gestionable`, `es_medible` | Flags de lógica de negocio |
| **Financiera** | `monto_gestionable`, `monto_medible` | Montos calculados por reglas |

## 🔥 **Lógica de Medibilidad - Regla Principal**

### **Condición de Medibilidad**
Un cliente es **MEDIBLE** solo cuando se cumplen **AMBAS** condiciones:

1. ✅ **Tiene asignación** (`es_gestionable = TRUE`)
2. ✅ **Coincidencia de fechas**: `fecha_deuda_construida = FECHA_TRANDEUDA`

```sql
-- Lógica implementada
es_medible = CASE 
  WHEN asig.cod_cuenta IS NOT NULL 
       AND fecha_deuda_construida = cal.FECHA_TRANDEUDA 
  THEN TRUE 
  ELSE FALSE 
END
```

### **Join Crítico con Calendario**
```sql
-- El join se hace por FECHA_TRANDEUDA, no por FECHA_ASIGNACION
LEFT JOIN calendario AS cal
  ON fecha_deuda_construida = cal.FECHA_TRANDEUDA
```

## 🧠 Lógica de Negocio Corregida

### 🔄 Nueva Tabla de Decisiones

| Escenario | tiene_asignacion | fecha_coincide_trandeuda | es_gestionable | es_medible | monto_medible |
|-----------|------------------|--------------------------|----------------|------------|---------------|
| **Con asignación + Coincide fecha** | ✅ TRUE | ✅ TRUE | ✅ TRUE | ✅ TRUE | = monto_exigible |
| **Con asignación + No coincide** | ✅ TRUE | ❌ FALSE | ✅ TRUE | ❌ FALSE | 0 |
| **Sin asignación + Coincide fecha** | ❌ FALSE | ✅ TRUE | ❌ FALSE | ❌ FALSE | 0 |
| **Sin asignación + No coincide** | ❌ FALSE | ❌ FALSE | ❌ FALSE | ❌ FALSE | 0 |

### 🎯 Diferencias Clave vs Versión Anterior

#### **❌ Antes (Incorrecto)**
- Medible = Gestionable AND día_apertura
- Join por FECHA_ASIGNACION
- Todos los gestionables del día de apertura eran medibles

#### **✅ Ahora (Correcto)**
- Medible = Gestionable AND (fecha_archivo = FECHA_TRANDEUDA)
- Join por FECHA_TRANDEUDA
- Solo los que coinciden específicamente con FECHA_TRANDEUDA son medibles

## 🔧 Construcción de Fecha desde Archivo

### Patrón de Archivo
```
TRAN_DEUDA_DDMM
Ejemplo: TRAN_DEUDA_1906 → 19/06/2025
```

### Lógica de Extracción (sin cambios)
```sql
CASE 
  WHEN REGEXP_CONTAINS(archivo, r'TRAN_DEUDA_(\\d{4})') THEN
    SAFE.PARSE_DATE('%Y-%m-%d', 
      CONCAT(
        CAST(EXTRACT(YEAR FROM creado_el) AS STRING), '-',
        SUBSTR(REGEXP_EXTRACT(archivo, r'TRAN_DEUDA_(\\d{4})'), 3, 2), '-',
        SUBSTR(REGEXP_EXTRACT(archivo, r'TRAN_DEUDA_(\\d{4})'), 1, 2)
      )
    )
  ELSE DATE(creado_el)
END AS fecha_deuda_construida
```

## 🔍 Detección y Joins

### Detección de Archivos TRAN_DEUDA
```sql
-- Sin cambios - sigue detectando por formato DDMM
DECLARE fecha_ddmm STRING DEFAULT FORMAT_DATE('%d%m', p_fecha_proceso);
SELECT STRING_AGG(DISTINCT archivo, ', ')
FROM batch_tran_deuda
WHERE REGEXP_CONTAINS(archivo, CONCAT(r'TRAN_DEUDA_', fecha_ddmm))
```

### Join con Calendario (CORREGIDO)
```sql
-- NUEVO: Join por FECHA_TRANDEUDA en lugar de FECHA_ASIGNACION
LEFT JOIN calendario AS cal
  ON fecha_deuda_construida = cal.FECHA_TRANDEUDA
```

### Join con Asignación
```sql
-- Mantiene join con asignación para determinar gestionabilidad
LEFT JOIN asignacion AS asig
  ON deu.cod_cuenta = asig.cod_cuenta
  AND cal.FECHA_ASIGNACION = asig.fecha_asignacion
```

## 📊 Métricas de Negocio Corregidas

### Cálculos Automáticos
```sql
-- Monto gestionable (sin cambios)
monto_gestionable = CASE 
  WHEN es_gestionable THEN monto_exigible 
  ELSE 0 
END

-- Monto medible (CORREGIDO)
monto_medible = CASE 
  WHEN es_gestionable AND fecha_deuda_construida = fecha_trandeuda 
  THEN monto_exigible 
  ELSE 0 
END
```

### Resumen por Procesamiento
- **Total deudas**: Todas las deudas procesadas
- **Deudas gestionables**: Con asignación
- **Deudas medibles**: Gestionables + coincidencia FECHA_TRANDEUDA
- **Fechas con calendario**: Cuántas fechas tienen entrada en calendario
- **Monto medible por TRANDEUDA**: Solo las que coinciden específicamente

## 🎮 Modos de Ejecución

### 1. Automático por Fecha (Recomendado)
```sql
-- Detecta automáticamente archivos y determina medibilidad
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(
  '2025-06-19'  -- fecha_proceso
);
```

### 2. Verificación de Coincidencias
```sql
-- Consulta para verificar coincidencias FECHA_TRANDEUDA
SELECT 
  fecha_deuda_construida,
  COUNT(*) as deudas_total,
  COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) as con_calendario,
  COUNT(CASE WHEN es_medible THEN 1 END) as medibles
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso = CURRENT_DATE()
GROUP BY fecha_deuda_construida
ORDER BY fecha_deuda_construida;
```

## 🧪 Tests de Calidad Actualizados

### Tests Específicos Corregidos
1. **Unicidad de llaves primarias**
2. **Construcción correcta de fechas desde archivo**
3. **Consistencia join con FECHA_TRANDEUDA**
4. **Validación lógica medibilidad**: `es_medible = TRUE` solo si `fecha_deuda = fecha_trandeuda`
5. **Cálculos correctos de montos medibles**
6. **Consistencia asignación vs gestionabilidad**

### Test Específico de Medibilidad
```sql
-- Verificar que todos los medibles tienen coincidencia FECHA_TRANDEUDA
WITH test_medibilidad AS (
  SELECT COUNT(*) as registros_inconsistentes
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
  WHERE es_medible = TRUE 
    AND (fecha_trandeuda IS NULL OR fecha_deuda != fecha_trandeuda)
    AND fecha_proceso = CURRENT_DATE()
)
SELECT 
  'TEST_MEDIBILIDAD_TRANDEUDA' as test_name,
  registros_inconsistentes as violaciones,
  CASE WHEN registros_inconsistentes = 0 THEN 'PASS' ELSE 'FAIL' END as resultado
FROM test_medibilidad;
```

## 📈 Métricas de Monitoreo

### Alertas Específicas Actualizadas
- ⚠️ **Pocas coincidencias TRANDEUDA**: Si < 50% de archivos tienen calendario
- ⚠️ **Sin deudas medibles**: Cuando hay archivos pero no coincidencias
- ❌ **Inconsistencia medibilidad**: Medibles sin coincidencia FECHA_TRANDEUDA
- 📊 **Ratio anormal**: Variación > 30% en % medibles vs histórico

### Dashboard de Coincidencias
```sql
-- Monitoreo de coincidencias por fecha
SELECT 
  fecha_deuda_construida,
  COUNT(*) as total_deudas,
  COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) as con_calendario,
  COUNT(CASE WHEN es_medible THEN 1 END) as medibles,
  ROUND(COUNT(CASE WHEN fecha_trandeuda IS NOT NULL THEN 1 END) / COUNT(*) * 100, 2) as pct_con_calendario,
  ROUND(COUNT(CASE WHEN es_medible THEN 1 END) / COUNT(*) * 100, 2) as pct_medible
FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_stg_deudas`
WHERE fecha_proceso >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY fecha_deuda_construida
ORDER BY fecha_deuda_construida DESC;
```

## 🔄 Casos de Uso Específicos

### Escenario 1: Archivo con Coincidencia TRANDEUDA
```
Archivo: TRAN_DEUDA_1906 → fecha_construida = 2025-06-19
Calendario: FECHA_TRANDEUDA = 2025-06-19
Cliente: Tiene asignación
Resultado: es_medible = TRUE, monto_medible = monto_exigible
```

### Escenario 2: Archivo sin Coincidencia TRANDEUDA  
```
Archivo: TRAN_DEUDA_2006 → fecha_construida = 2025-06-20
Calendario: No existe FECHA_TRANDEUDA = 2025-06-20
Cliente: Tiene asignación
Resultado: es_medible = FALSE, monto_medible = 0
```

### Escenario 3: Coincidencia sin Asignación
```
Archivo: TRAN_DEUDA_1906 → fecha_construida = 2025-06-19
Calendario: FECHA_TRANDEUDA = 2025-06-19  
Cliente: No tiene asignación
Resultado: es_medible = FALSE, monto_medible = 0
```

## 🛠️ Troubleshooting Actualizado

| Problema | Causa Probable | Solución |
|----------|----------------|-----------| 
| **Pocas deudas medibles** | Pocas coincidencias FECHA_TRANDEUDA | Verificar configuración calendario |
| **Sin coincidencias** | FECHA_TRANDEUDA mal configurada | Validar fechas en tabla calendario |
| **Medibles sin calendario** | Error en join FECHA_TRANDEUDA | Revisar lógica de join |
| **Inconsistencia fechas** | Formato archivo incorrecto | Verificar regex construcción fecha |

---

**Versión**: 1.1.0  
**Fecha**: 2025-06-19  
**Autor**: FACO Team  
**Cambio Crítico**: Medibilidad basada en coincidencia con FECHA_TRANDEUDA del calendario  
**Dependencias**: Stage de Asignación (debe ejecutarse primero)
