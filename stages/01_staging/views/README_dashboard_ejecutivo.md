# 📊 Views para Dashboard Ejecutivo - FACO Pipeline

## 🎯 VW_Graficos_Por_Vencimiento

### ✅ **Estructura de Datos**
```sql
SELECT * FROM `BI_USA.vw_graficos_por_vencimiento` LIMIT 5;
```

| Campo | Descripción | Ejemplo |
|-------|-------------|---------|
| `cartera` | Tipo de cartera | TEMPRANA, FRACCIONAMIENTO |
| `vencimiento` | Bucket de vencimiento | VCTO_05, VCTO_09, VCTO_13, VCTO_17 |
| `dia_calendario` | Fecha específica | 2025-06-19 |
| `contactabilidad` | % de contactos efectivos | 42.5 |
| `conversion` | % conversión a compromisos | 35.2 |
| `tasa_cierre` | % compromisos que pagan | 78.1 |
| `efectividad` | % efectividad general | 38.7 |
| `intensidad` | Weight promedio | 15.3 |
| `cd_porcentaje` | % Contacto Directo | 68.2 |
| `ci_porcentaje` | % Contacto Indirecto | 31.8 |

---

## 🎨 **Consultas para Dashboard (exactas como el reporte)**

### **1. Gráfico de Contactabilidad por Cartera**
```sql
-- Para replicar slide 5 del reporte
SELECT 
  cartera,
  vencimiento,
  contactabilidad
FROM `BI_USA.vw_graficos_por_vencimiento`
WHERE mes = 6 AND anio = 2025
ORDER BY cartera, orden_vencimiento;
```

### **2. Comparativo Temporal (Marzo vs Abril)**
```sql
-- Para replicar tendencias del reporte
SELECT 
  cartera,
  vencimiento,
  mes,
  contactabilidad,
  conversion,
  efectividad
FROM `BI_USA.vw_graficos_por_vencimiento`
WHERE anio = 2025 AND mes IN (3, 4, 5, 6)
ORDER BY cartera, vencimiento, mes;
```

### **3. Todos los Indicadores para un Mes**
```sql
-- Para generar los 7 cuadritos del dashboard
SELECT 
  cartera,
  vencimiento,
  contactabilidad,
  conversion, 
  tasa_cierre,
  efectividad,
  intensidad,
  cd_porcentaje,
  ci_porcentaje
FROM `BI_USA.vw_graficos_por_vencimiento`
WHERE mes = 6 AND anio = 2025
ORDER BY cartera, orden_vencimiento;
```

### **4. Resumen Ejecutivo por Cartera**
```sql
-- KPIs agregados por cartera (como slide 3)
SELECT 
  cartera,
  mes,
  ROUND(AVG(contactabilidad), 1) as contactabilidad_promedio,
  ROUND(AVG(conversion), 1) as conversion_promedio,
  ROUND(AVG(efectividad), 1) as efectividad_promedio,
  ROUND(AVG(intensidad), 1) as intensidad_promedio,
  SUM(total_gestiones) as total_gestiones_cartera,
  SUM(total_compromisos) as total_compromisos_cartera
FROM `BI_USA.vw_graficos_por_vencimiento`
WHERE anio = 2025
GROUP BY cartera, mes
ORDER BY cartera, mes;
```

---

## 📈 **Configuración para Looker Studio**

### **Estructura de Gráficos Recomendada:**

#### **Gráfico 1: Contactabilidad**
- **Tipo**: Líneas
- **Eje X**: `vencimiento` 
- **Eje Y**: `contactabilidad`
- **Series**: `cartera` (cada cartera = línea diferente)
- **Filtro**: Mes actual

#### **Gráfico 2: Conversión**
- **Tipo**: Líneas  
- **Eje X**: `vencimiento`
- **Eje Y**: `conversion`
- **Series**: `cartera`

#### **Gráfico 3: Efectividad**
- **Tipo**: Líneas
- **Eje X**: `vencimiento` 
- **Eje Y**: `efectividad`
- **Series**: `cartera`

*...y así para los 7 indicadores*

---

## 🔧 **Configuración Adicional**

### **Filtros Recomendados:**
- **Selector de Mes**: `mes`
- **Selector de Año**: `anio` 
- **Selector de Cartera**: `cartera`
- **Selector de Canal**: `canal_origen`

### **Métricas de Control:**
- **Total Gestiones**: `SUM(total_gestiones)`
- **Total Compromisos**: `SUM(total_compromisos)`
- **Cobertura**: `COUNT(DISTINCT dia_calendario)`

---

## ⚡ **Ventajas vs Reporte Manual**

### ✅ **Automatización Completa**
- **Datos en tiempo real** desde el pipeline
- **Sin cálculos manuales** - todo procesado automáticamente
- **Consistencia garantizada** - misma lógica en todos los stages

### ✅ **Flexibilidad Total**
- **Filtros dinámicos** por cualquier dimensión
- **Comparativos temporales** automáticos
- **Drill-down** a nivel cliente si es necesario

### ✅ **Escalabilidad**
- **Nuevas carteras** se agregan automáticamente
- **Nuevos indicadores** fáciles de añadir
- **Histórico completo** disponible

---

## 🎯 **Para Tu Presentación**

### **Demostración Sugerida:**
1. **Mostrar la view funcionando** en BigQuery
2. **Replicar un gráfico** del reporte actual
3. **Comparar la velocidad** (segundos vs horas)
4. **Mostrar filtros dinámicos** que no existen en Excel

### **Puntos Clave:**
- **"Misma información, pero automatizada"**
- **"De Excel manual a dashboard en tiempo real"**
- **"Base sólida para crecer - más carteras, más indicadores"**

---

**¡Listo para tu presentación!** 🚀