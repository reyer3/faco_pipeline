# Stages del Pipeline FACO

## 🏗️ Arquitectura de 3 Capas

El pipeline de gestión de cobranzas está estructurado en tres capas principales:

```
📁 stages/
├── 01_staging/          # Capa de Staging
├── 02_analytics/        # Capa Analítica  
└── 03_presentation/     # Capa de Presentación
```

## 🔄 Flujo de Datos

### 1️⃣ Staging Layer (01_staging)
**Propósito**: Ingesta, limpieza y estandarización de datos raw

- **Asignación**: Procesamiento de datos de asignación de cartera
- **Gestiones**: Consolidación de actividades de gestión
- **Pagos**: Normalización de información de pagos
- **Calendarios**: Gestión de dimensiones temporales

### 2️⃣ Analytics Layer (02_analytics)
**Propósito**: Transformaciones de negocio y métricas calculadas

- **Indicadores**: KPIs y métricas de gestión
- **Segmentación**: Análisis de carteras y clientes
- **Tendencias**: Análisis temporal y forecasting
- **Cohorts**: Análisis de cohortes de recupero

### 3️⃣ Presentation Layer (03_presentation)
**Propósito**: Datos optimizados para consumo en herramientas de BI

- **Dashboards**: Vistas agregadas para Looker Studio
- **Reports**: Reportes predefinidos
- **APIs**: Endpoints para consumo externo
- **Exports**: Formatos para exportación

## 📋 Convenciones

### Nomenclatura
```
{capa}_{entidad}_{tipo}

Ejemplos:
- stg_asignacion (staging de asignación)
- anl_indicadores_recupero (analytics de indicadores)
- prs_dashboard_gestiones (presentation para dashboard)
```

### Estructura de Directorios
```
📁 {stage}/
├── 📁 {entidad}/
│   ├── 📄 sp_{entidad}.sql          # Stored Procedure principal
│   ├── 📄 create_table_{entidad}.sql # DDL de tabla
│   ├── 📁 tests/                    # Tests de calidad
│   └── 📁 docs/                     # Documentación específica
```

## 🔑 Principios de Diseño

1. **Idempotencia**: Todos los procesos deben ser re-ejecutables
2. **Logging**: Registro completo de ejecuciones y métricas
3. **Testing**: Validación automática de calidad de datos
4. **Particionado**: Optimización para consultas temporales
5. **Documentación**: Especificaciones técnicas detalladas

## 🚀 Estado Actual

| Stage | Entidad | Estado | Prioridad |
|-------|---------|--------|-----------| 
| 01_staging | asignacion | ✅ Completado | Alta |
| 01_staging | gestiones | 🔄 En desarrollo | Alta |
| 01_staging | pagos | 📋 Planeado | Media |
| 02_analytics | indicadores | 📋 Planeado | Alta |
| 03_presentation | dashboards | 📋 Planeado | Media |

---

**Próximos Pasos**: 
1. Finalizar stage de gestiones
2. Implementar tests automatizados
3. Configurar monitoreo y alertas
