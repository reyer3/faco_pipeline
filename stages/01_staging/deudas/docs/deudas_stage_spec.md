# Stage de Deudas - Especificación Técnica

## 📋 Resumen Ejecutivo

El **Stage de Deudas** es el segundo componente del pipeline de datos de gestión de cobranzas FACO. Procesa las deudas diarias refrescadas aplicando **lógica compleja de día de apertura vs días subsiguientes**, determinando qué clientes son gestionables y medibles para competencia.

## 🎯 Objetivos

- **Procesamiento Diario**: Manejo de deudas refrescadas diariamente desde archivos TRAN_DEUDA
- **Lógica de Apertura**: Distinguir día de apertura vs días subsiguientes de cartera
- **Filtrado Inteligente**: Identificar clientes gestionables y medibles según tipo de día
- **Construcción de Fechas**: Extraer fecha del nombre de archivo usando regex
- **Integración**: Join con datos de asignación para determinar gestionabilidad

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
| **Temporal** | `fecha_deuda`, `fecha_deuda_construida` | Fechas extraídas del archivo |
| **Negocio** | `es_dia_apertura`, `es_gestionable`, `es_medible` | Flags de lógica de negocio |
| **Activación** | `tipo_activacion`, `secuencia_activacion` | Control de activaciones |
| **Financiera** | `monto_gestionable`, `monto_medible` | Montos calculados por reglas |

## 🧠 Lógica de Negocio Compleja

### 🟢 Día de Apertura de Cartera

**Condición**: `fecha_proceso` coincide con `FECHA_ASIGNACION` en calendario

**Comportamiento**:
- ✅ **Filtro Estricto**: Solo clientes que pasan a "gestionables y medibles"
- ✅ **es_gestionable = TRUE**: Solo si tienen asignación
- ✅ **es_medible = TRUE**: Solo si son gestionables Y es día de apertura
- ✅ **tipo_activacion = 'APERTURA'**
- ✅ **Cuentan para competencia**: `monto_medible > 0`

### 🟡 Días Subsiguientes

**Condición**: `fecha_proceso` NO coincide con `FECHA_ASIGNACION`

**Comportamiento**:
- ⚠️ **Inclusión Amplia**: Pueden sumarse/activarse deudas de otros clientes
- ⚠️ **es_gestionable**: Depende si tienen asignación (pueden no tenerla)
- ❌ **es_medible = FALSE**: NO cuentan para competencia
- ⚠️ **tipo_activacion = 'SUBSIGUIENTE'**
- ❌ **No miden competencia**: `monto_medible = 0`

### 🔄 Tabla de Decisiones

| Escenario | es_dia_apertura | tiene_asignacion | es_gestionable | es_medible | monto_medible |
|-----------|-----------------|------------------|----------------|------------|---------------|
| Apertura + Asignado | TRUE | TRUE | TRUE | TRUE | = monto_exigible |
| Apertura + No Asignado | TRUE | FALSE | FALSE | FALSE | 0 |
| Subsiguiente + Asignado | FALSE | TRUE | TRUE | FALSE | 0 |
| Subsiguiente + No Asignado | FALSE | FALSE | FALSE | FALSE | 0 |

## 🔧 Construcción de Fecha desde Archivo

### Patrón de Archivo
```
TRAN_DEUDA_DDMM
Ejemplo: TRAN_DEUDA_1906 → 19/06/2025
```

### Lógica de Extracción
```sql
CASE 
  WHEN REGEXP_CONTAINS(archivo, r'TRAN_DEUDA_(\\d{4})') THEN
    SAFE.PARSE_DATE('%Y-%m-%d', 
      CONCAT(
        CAST(EXTRACT(YEAR FROM creado_el) AS STRING), '-',
        SUBSTR(REGEXP_EXTRACT(archivo, r'TRAN_DEUDA_(\\d{4})'), 3, 2), '-',  -- MM
        SUBSTR(REGEXP_EXTRACT(archivo, r'TRAN_DEUDA_(\\d{4})'), 1, 2)       -- DD
      )
    )
  ELSE DATE(creado_el)
END AS fecha_deuda_construida
```

## 🔍 Detección Automática

### Detección de Día de Apertura
```sql
-- Verificar si es día de apertura
SELECT COUNT(*) > 0
FROM calendario 
WHERE FECHA_ASIGNACION = p_fecha_proceso
```

### Detección de Archivos TRAN_DEUDA
```sql
-- Formato esperado: TRAN_DEUDA_DDMM para la fecha
DECLARE fecha_ddmm STRING DEFAULT FORMAT_DATE('%d%m', p_fecha_proceso);

SELECT STRING_AGG(DISTINCT archivo, ', ')
FROM batch_tran_deuda
WHERE REGEXP_CONTAINS(archivo, CONCAT(r'TRAN_DEUDA_', fecha_ddmm))
```

## 🎮 Modos de Ejecución

### 1. Automático por Fecha (Recomendado)
```sql
-- Detecta automáticamente archivos y tipo de día
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(
  '2025-06-19'  -- fecha_proceso
);
```

### 2. Filtro Manual por Archivo
```sql
-- Procesa archivo específico
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(
  CURRENT_DATE(),
  'TRAN_DEUDA_1906'  -- archivo específico
);
```

### 3. Full Refresh
```sql
-- Reprocesa histórico completo
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas`(
  CURRENT_DATE(),
  NULL,
  'FULL'
);
```

## 📊 Métricas de Negocio

### Cálculos Automáticos
```sql
-- Monto gestionable
monto_gestionable = CASE 
  WHEN es_gestionable THEN monto_exigible 
  ELSE 0 
END

-- Monto medible (solo día apertura)
monto_medible = CASE 
  WHEN es_gestionable AND es_dia_apertura THEN monto_exigible 
  ELSE 0 
END
```

### Resumen por Procesamiento
- **Total deudas**: Todas las deudas procesadas
- **Deudas gestionables**: Con asignación
- **Deudas medibles**: Gestionables en día de apertura
- **Monto total**: Suma de todos los `monto_exigible`
- **Monto gestionable**: Solo deudas con asignación
- **Monto medible**: Solo día apertura + asignación

## 🔄 Proceso de Merge

### WHEN MATCHED (Actualización)
- `monto_exigible`
- `estado_deuda`
- `monto_gestionable`
- `monto_medible`
- `fecha_actualizacion`

### WHEN NOT MATCHED (Inserción)
- Todos los campos del registro nuevo
- Preserva secuencia de activación
- Mantiene historial de cambios

## 🧪 Tests de Calidad

### Tests Específicos de Deudas
1. **Unicidad de llaves primarias**
2. **Construcción correcta de fechas desde archivo**
3. **Consistencia de lógica día de apertura**
4. **Cálculos correctos de montos medibles/gestionables**
5. **Validación de tipos de activación**
6. **Consistencia asignación vs gestionabilidad**
7. **Rangos válidos de montos**
8. **Comparativo métricas por tipo de día**

### Alertas de Negocio
- ⚠️ **Sin deudas medibles en apertura**: Posible problema
- ⚠️ **Deudas medibles en subsiguiente**: Inconsistencia lógica
- ❌ **Montos negativos**: Error de datos
- ❌ **Fechas no construidas**: Problema de regex

## 📈 Optimización

### Particionado
- **Partición**: `DATE(fecha_deuda)`
- **Beneficio**: Optimiza consultas temporales

### Clustering
- **Campos**: `cod_cuenta`, `tipo_activacion`, `es_medible`
- **Beneficio**: Mejora filtros por gestionabilidad

## 🔍 Monitoreo Específico

### Métricas Clave
- **Ratio día apertura**: % de días que son apertura
- **% Gestionables**: Deudas con asignación / Total
- **% Medibles**: Solo en días de apertura
- **Monto promedio**: Por tipo de activación
- **Distribución tipos**: APERTURA vs SUBSIGUIENTE

### Alertas Recomendadas
- **No archivos detectados**: Para fecha específica
- **Sin deudas medibles en apertura**: Revisión necesaria
- **Ratio anormal**: Variación > 30% vs histórico
- **Montos inconsistentes**: Diferencias en cálculos

## 📝 Casos de Uso

### Escenario 1: Día de Apertura Nueva Cartera
```
Fecha: 2025-06-19 (nueva cartera TEMPRANA)
Resultado: 
- es_dia_apertura = TRUE
- Solo clientes asignados son gestionables
- Solo gestionables son medibles
- tipo_activacion = 'APERTURA'
```

### Escenario 2: Día Subsiguiente
```
Fecha: 2025-06-20 (día siguiente)
Resultado:
- es_dia_apertura = FALSE  
- Pueden activarse nuevos clientes
- Ninguno es medible (monto_medible = 0)
- tipo_activacion = 'SUBSIGUIENTE'
```

### Escenario 3: Reactivación
```
Cliente con deuda previa que se reactiva
- Mantiene secuencia_activacion incremental
- Sigue reglas según tipo de día
- Preserva historial de activaciones
```

## 🛠️ Troubleshooting

| Problema | Causa Probable | Solución |
|----------|----------------|-----------| 
| Fecha no construida | Formato archivo incorrecto | Verificar patrón TRAN_DEUDA_DDMM |
| Sin deudas medibles en apertura | Falta join con asignación | Verificar carga previa de asignación |
| Inconsistencia día apertura | Error en calendario | Validar FECHA_ASIGNACION |
| Montos incorrectos | Lógica de cálculo fallida | Revisar reglas es_gestionable/es_medible |

---

**Versión**: 1.0.0  
**Fecha**: 2025-06-19  
**Autor**: FACO Team  
**Dependencias**: Stage de Asignación (debe ejecutarse primero)
