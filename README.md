# Pipeline de Datos FACO - Gestión de Cobranzas

## 🎯 Visión General

Pipeline de datos estructurado en 3 capas para la gestión y reportería de cobranzas, diseñado para procesar, analizar y presentar información de manera sistemática y escalable.

## 🏗️ Arquitectura

```
📁 faco_pipeline/
├── 📁 stages/
│   ├── 📁 01_staging/          # Capa de Staging
│   ├── 📁 02_analytics/        # Capa Analítica  
│   └── 📁 03_presentation/     # Capa de Presentación
├── 📁 config/                 # Configuración del pipeline
├── 📁 utils/                  # Utilidades y herramientas
└── 📁 docs/                   # Documentación
```

## 🚀 Estado Actual

### ✅ Completado
- [x] Estructura del repositorio
- [x] Stage de Asignación (01_staging)
- [x] Sistema de logging
- [x] Tests de calidad de datos

### 🔄 En Desarrollo
- [ ] Stage de Gestiones
- [ ] Stage de Pagos
- [ ] Capa Analítica

## 🛠️ Tecnologías

- **BigQuery**: Base de datos y procesamiento
- **SQL**: Stored Procedures y transformaciones
- **YAML**: Configuración del pipeline

---

**Versión**: 1.0.0  
**Equipo**: FACO Team
