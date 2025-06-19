# Stage de Asignación - Especificación Técnica

## 📋 Resumen Ejecutivo

El **Stage de Asignación** es la primera capa del pipeline de datos de gestión de cobranzas FACO. Su objetivo es procesar, enriquecer y estandarizar los datos de asignación de cartera, creando una base sólida para las capas analíticas posteriores.

## 🎯 Objetivos

- **Consolidación**: Unificar datos de asignación con información de calendario
- **Enriquecimiento**: Agregar dimensiones calculadas y categorizaciones
- **Estandarización**: Aplicar reglas de negocio consistentes
- **Automatización**: Detección automática de archivos por fecha de proceso
- **Calidad**: Garantizar integridad y completitud de los datos

## 🏗️ Arquitectura

### Entrada
- **Tabla Principal**: `batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
- **Tabla Calendario**: `bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`

### Salida
- **Tabla Staging**: `bi_P3fV4dWNeMkN5RJMhV8e_stg_asignacion`

### Procesamiento
- **Stored Procedure**: `bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`

## 🔑 Modelo de Datos

### Llaves Primarias
```sql
PRIMARY KEY (cod_luna, cod_cuenta, archivo)
```

### Dimensiones Principales

| Categoría | Campos | Descripción |
|-----------|--------|--------------| 
| **Cliente** | `cod_luna`, `cliente`, `telefono` | Identificación del cliente |
| **Producto** | `servicio`, `zona_geografica` | Características del servicio |
| **Gestión** | `segmento_gestion`, `tipo_cartera` | Clasificación para gestión |
| **Temporal** | `fecha_vencimiento`, `fecha_asignacion` | Dimensiones de tiempo |
| **Financiera** | `objetivo_recupero`, `saldo_dia` | Métricas financieras |

## 🧠 Lógica de Detección Automática

### Funcionamiento Inteligente
El procedimiento ahora incluye **detección automática de archivos** basada en la fecha de proceso:

```sql
-- Si no se especifica archivo_filter (NULL), el sistema:
SELECT STRING_AGG(ARCHIVO, ', ') 
FROM calendario
WHERE FECHA_ASIGNACION = p_fecha_proceso
```

### Filtros de Datos
```sql
WHERE 
  -- 🎯 FILTRO INTELIGENTE
  (
    -- Filtro manual (si se especifica)
    (p_archivo_filter IS NOT NULL AND archivo LIKE '%filtro%')
    OR
    -- Detección automática (si es NULL)
    (p_archivo_filter IS NULL AND FECHA_ASIGNACION = p_fecha_proceso)
  )
```

## 🎮 Modos de Ejecución

### 1. Automático por Fecha (Recomendado)
```sql
-- Procesa automáticamente todos los archivos de la fecha
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  '2025-06-19',  -- fecha_proceso
  NULL,          -- archivo_filter (AUTO_DETECT)
  'INCREMENTAL'  -- modo_ejecucion
);
```

### 2. Filtro Manual por Archivo
```sql
-- Procesa solo archivos que contengan 'TEMPRANA'
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  CURRENT_DATE(),
  'TEMPRANA',    -- archivo_filter específico
  'INCREMENTAL'
);
```

### 3. Full Refresh
```sql
-- Reprocesa toda la información histórica
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  CURRENT_DATE(),
  NULL,
  'FULL'
);
```

## 🧮 Reglas de Negocio

### Categorización de Vencimiento
```sql
CASE
  WHEN min_vto IS NULL THEN 'SIN_VENCIMIENTO'
  WHEN min_vto <= CURRENT_DATE() THEN 'VENCIDO'
  WHEN min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'POR_VENCER_30D'
  WHEN min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 60 DAY) THEN 'POR_VENCER_60D'
  WHEN min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'POR_VENCER_90D'
  ELSE 'VIGENTE_MAS_90D'
END
```

### Objetivos de Recupero
| Condición | Objetivo |
|-----------|----------|
| `tramo_gestion = 'AL VCTO'` | 15% |
| `tramo_gestion = 'ENTRE 4 Y 15D'` | 25% |
| `archivo CONTAINS 'TEMPRANA'` | 20% |
| **Defecto** | 20% |

### Tipificación de Cartera
| Patrón en Archivo | Tipo |
|-------------------|------|
| `TEMPRANA` | TEMPRANA |
| `CF_ANN` | CUOTA_FRACCIONAMIENTO |
| `AN` | ALTAS_NUEVAS |
| **Otros** | OTRAS |

## 🔄 Proceso de Merge

El proceso utiliza la sentencia `MERGE` de BigQuery con la siguiente lógica:

### WHEN MATCHED (Actualización)
- `estado_cartera`
- `saldo_dia` 
- `fecha_actualizacion`

### WHEN NOT MATCHED (Inserción)
- Todos los campos del registro nuevo

## 📊 Calidad de Datos

### Tests Implementados
1. **Unicidad de llaves primarias**
2. **Validación de valores obligatorios**
3. **Rangos de objetivo de recupero (0-1)**
4. **Categorías de vencimiento válidas**
5. **Completitud de join con calendario**

### Métricas de Calidad
- **Success Rate**: % de tests pasados
- **Completitud**: % de registros sin nulos
- **Consistencia**: % de registros con joins exitosos

## 📝 Logging Mejorado

### Información Rastreada
```json
{
  "fecha_proceso": "2025-06-19",
  "archivo_filter": "AUTO_DETECT",
  "modo_ejecucion": "INCREMENTAL",
  "archivos_detectados": "archivo1, archivo2, archivo3"
}
```

### Observaciones
- Lista de archivos detectados automáticamente
- Modo de filtrado aplicado (manual vs automático)
- Métricas de procesamiento detalladas

## 📈 Optimización

### Particionado
- **Partición**: `DATE(fecha_asignacion)`
- **Beneficio**: Mejora performance en consultas temporales

### Clustering
- **Campos**: `cod_luna`, `tipo_cartera`, `segmento_gestion`
- **Beneficio**: Optimiza joins y filtros frecuentes

## 🔍 Monitoreo

### Logging
Todos los procesos registran en `BI_USA.pipeline_logs`:
- Archivos detectados automáticamente
- Tiempo de inicio/fin
- Registros procesados/nuevos/actualizados
- Parámetros de ejecución
- Estado final

### Alertas Recomendadas
- **Sin archivos detectados**: Cuando no hay archivos para la fecha
- **Duración**: > 30 minutos
- **Volumen**: Variación > 50% vs promedio
- **Calidad**: Success rate < 95%

## 🛠️ Casos de Uso

### Uso Diario Automatizado
```sql
-- Procesar automáticamente la cartera del día
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`();
```

### Reprocesamiento Específico
```sql
-- Reprocesar solo archivos de temprana de ayer
CALL `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion`(
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
  'TEMPRANA',
  'INCREMENTAL'
);
```

### Troubleshooting

| Problema | Causa Probable | Solución |
|----------|----------------|-----------| 
| "No archivos detectados" | Fecha sin asignaciones | Verificar tabla calendario |
| Join fallido con calendario | Formato de archivo inconsistente | Verificar `archivo = CONCAT(ARCHIVO, '.txt')` |
| Duplicados en llaves | Múltiples cargas del mismo archivo | Implementar validación pre-carga |
| Performance lenta | Particiones no utilizadas | Asegurar filtros por `fecha_asignacion` |

---

**Versión**: 1.1.0  
**Fecha**: 2025-06-19  
**Autor**: FACO Team  
**Cambios**: Agregada detección automática de archivos por fecha
