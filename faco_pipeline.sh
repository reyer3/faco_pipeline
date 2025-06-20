#!/bin/bash
# ================================================================
# FACO Pipeline - Script Principal de Despliegue
# ================================================================
# Uso: ./faco_pipeline.sh [comando] [parametros]
# ================================================================

PROJECT_ID="mibot-222814"
DATASET="BI_USA"
REPO_URL="https://github.com/reyer3/faco_pipeline.git"
LOCAL_PATH="./faco_pipeline"
FECHA_INICIO="2025-05-14"

# ================================================================
# FUNCIÓN 1: SETUP INICIAL
# ================================================================
setup_environment() {
    echo "🔧 Configurando ambiente para FACO Pipeline..."
    
    # Verificar gcloud CLI
    if ! command -v gcloud &> /dev/null; then
        echo "❌ gcloud CLI no encontrado. Instálalo primero."
        exit 1
    fi
    
    # Verificar bq CLI
    if ! command -v bq &> /dev/null; then
        echo "❌ bq CLI no encontrado. Instálalo primero."
        exit 1
    fi
    
    # Configurar proyecto
    echo "🔑 Configurando proyecto $PROJECT_ID..."
    gcloud config set project $PROJECT_ID
    
    # Verificar acceso a BigQuery
    echo "📊 Verificando acceso a BigQuery..."
    if ! bq ls --project_id=$PROJECT_ID --max_results=1 > /dev/null 2>&1; then
        echo "❌ Sin acceso a BigQuery. Verifica permisos."
        exit 1
    fi
    
    # Clonar o actualizar repositorio
    if [ -d "$LOCAL_PATH" ]; then
        echo "📂 Actualizando repositorio existente..."
        cd $LOCAL_PATH && git pull && cd ..
    else
        echo "📥 Clonando repositorio FACO Pipeline..."
        git clone $REPO_URL $LOCAL_PATH
    fi
    
    echo "✅ Setup completado!"
}

# ================================================================
# FUNCIÓN 2: DESPLEGAR TABLAS Y SPs
# ================================================================
deploy_staging_layer() {
    echo "🏗️ Desplegando capa de staging..."
    
    if [ ! -d "$LOCAL_PATH" ]; then
        echo "❌ Repositorio no encontrado. Ejecuta 'setup' primero."
        exit 1
    fi
    
    cd $LOCAL_PATH
    
    # Orden importante por dependencias
    STAGES=("asignacion" "deudas" "gestiones" "pagos")
    
    for stage in "${STAGES[@]}"; do
        echo "📊 Desplegando stage: $stage"
        
        # Verificar archivos existen
        DDL_FILE="stages/01_staging/$stage/create_table_$stage.sql"
        SP_FILE="stages/01_staging/$stage/sp_$stage.sql"
        
        if [ ! -f "$DDL_FILE" ]; then
            echo "❌ No encontrado: $DDL_FILE"
            continue
        fi
        
        if [ ! -f "$SP_FILE" ]; then
            echo "❌ No encontrado: $SP_FILE"
            continue
        fi
        
        # Crear tabla
        echo "   📋 Creando tabla de $stage..."
        if bq query --use_legacy_sql=false --project_id=$PROJECT_ID < "$DDL_FILE"; then
            echo "   ✅ Tabla de $stage creada"
        else
            echo "   ❌ Error creando tabla de $stage"
            continue
        fi
        
        # Crear stored procedure
        echo "   🔧 Creando stored procedure de $stage..."
        if bq query --use_legacy_sql=false --project_id=$PROJECT_ID < "$SP_FILE"; then
            echo "   ✅ SP de $stage creado"
        else
            echo "   ❌ Error creando SP de $stage"
        fi
    done
    
    cd ..
    echo "🎯 ¡Capa de staging desplegada!"
}

# ================================================================
# FUNCIÓN 3: EJECUTAR PIPELINE DIARIO
# ================================================================
execute_daily_pipeline() {
    local fecha_proceso=${1:-$(date +%Y-%m-%d)}
    
    echo "⚡ Ejecutando pipeline FACO para: $fecha_proceso"
    
    # 1. ASIGNACIÓN
    echo "   📊 Stage Asignación..."
    if bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
        "CALL \`$PROJECT_ID.$DATASET.bi_P3fV4dWNeMkN5RJMhV8e_sp_asignacion\`('$fecha_proceso');"; then
        echo "   ✅ Asignación OK"
    else
        echo "   ❌ Fallo en Asignación"
        return 1
    fi
    
    # 2. DEUDAS
    echo "   💰 Stage Deudas..."
    if bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
        "CALL \`$PROJECT_ID.$DATASET.bi_P3fV4dWNeMkN5RJMhV8e_sp_deudas\`('$fecha_proceso');"; then
        echo "   ✅ Deudas OK"
    else
        echo "   ❌ Fallo en Deudas"
        return 1
    fi
    
    # 3. GESTIONES
    echo "   📞 Stage Gestiones..."
    if bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
        "CALL \`$PROJECT_ID.$DATASET.bi_P3fV4dWNeMkN5RJMhV8e_sp_gestiones\`('$fecha_proceso');"; then
        echo "   ✅ Gestiones OK"
    else
        echo "   ❌ Fallo en Gestiones"
        return 1
    fi
    
    # 4. PAGOS
    echo "   💳 Stage Pagos..."
    if bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
        "CALL \`$PROJECT_ID.$DATASET.bi_P3fV4dWNeMkN5RJMhV8e_sp_pagos\`('$fecha_proceso');"; then
        echo "   ✅ Pagos OK"
    else
        echo "   ❌ Fallo en Pagos"
        return 1
    fi
    
    echo "🎯 Pipeline completado para $fecha_proceso"
    
    # Mostrar resumen
    show_summary $fecha_proceso
}

# ================================================================
# FUNCIÓN 4: PROCESAMIENTO HISTÓRICO
# ================================================================
execute_historical_data() {
    echo "📅 Procesando desde $FECHA_INICIO hasta hoy..."
    
    current_date=$FECHA_INICIO
    fecha_fin=$(date +%Y-%m-%d)
    
    while [[ "$current_date" < "$fecha_fin" ]] || [[ "$current_date" == "$fecha_fin" ]]; do
        echo ""
        echo "🔄 Procesando: $current_date"
        
        if execute_daily_pipeline $current_date; then
            echo "✅ $current_date completado"
        else
            echo "❌ Fallo en $current_date - continuando..."
        fi
        
        # Avanzar un día
        current_date=$(date -I -d "$current_date + 1 day")
    done
    
    echo ""
    echo "🏁 Procesamiento histórico terminado!"
}

# ================================================================
# FUNCIÓN 5: MOSTRAR RESUMEN
# ================================================================
show_summary() {
    local fecha=${1:-$(date +%Y-%m-%d)}
    
    echo ""
    echo "📊 RESUMEN para $fecha:"
    echo "========================"
    
    # Contar registros por stage
    for stage in "asignacion" "deudas" "gestiones" "pagos"; do
        table_name="bi_P3fV4dWNeMkN5RJMhV8e_stg_$stage"
        
        count=$(bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
            --format=csv --quiet \
            "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.$table_name\` WHERE fecha_proceso = '$fecha'" | tail -1)
        
        printf "%-12s: %'d registros\n" "$stage" "$count"
    done
    
    echo "========================"
}

# ================================================================
# FUNCIÓN 6: MONITOREAR
# ================================================================
monitor_pipeline() {
    local fecha=${1:-$(date +%Y-%m-%d)}
    
    echo "📊 Monitoreando pipeline para: $fecha"
    
    # Mostrar resumen
    show_summary $fecha
    
    # Buscar logs (si existen)
    echo ""
    echo "📝 Logs recientes:"
    bq query --use_legacy_sql=false --project_id=$PROJECT_ID --max_results=10 \
        "SELECT timestamp, stage_name, status, duration_seconds, message 
         FROM \`$PROJECT_ID.$DATASET.pipeline_logs\` 
         WHERE DATE(timestamp) = '$fecha' 
         ORDER BY timestamp DESC" 2>/dev/null || echo "   (No hay tabla de logs aún)"
}

# ================================================================
# FUNCIÓN 7: DESPLIEGUE COMPLETO
# ================================================================
full_deployment() {
    echo "🚀 DESPLIEGUE COMPLETO FACO PIPELINE"
    echo "===================================="
    
    # 1. Setup
    setup_environment
    
    # 2. Deploy
    deploy_staging_layer
    
    # 3. Test con fecha actual
    echo ""
    echo "🧪 Probando con fecha actual..."
    if execute_daily_pipeline $(date +%Y-%m-%d); then
        echo "✅ Test exitoso!"
    else
        echo "❌ Test falló. Revisa configuración."
        return 1
    fi
    
    # 4. Preguntar por histórico
    echo ""
    echo "📅 ¿Procesar histórico desde $FECHA_INICIO? (y/n)"
    read -r respuesta
    
    if [[ $respuesta == "y" || $respuesta == "Y" ]]; then
        execute_historical_data
    fi
    
    echo ""
    echo "🎉 ¡DESPLIEGUE COMPLETADO!"
    echo "========================="
    echo ""
    echo "📋 Comandos útiles:"
    echo "   ./faco_pipeline.sh execute_daily [fecha]"
    echo "   ./faco_pipeline.sh monitor [fecha]"
    echo "   ./faco_pipeline.sh show_summary [fecha]"
    echo ""
}

# ================================================================
# MENÚ PRINCIPAL
# ================================================================
case "${1:-help}" in
    "setup")
        setup_environment
        ;;
    "deploy")
        deploy_staging_layer
        ;;
    "execute_daily")
        execute_daily_pipeline "$2"
        ;;
    "execute_historical")
        execute_historical_data
        ;;
    "monitor")
        monitor_pipeline "$2"
        ;;
    "show_summary")
        show_summary "$2"
        ;;
    "full_deployment")
        full_deployment
        ;;
    "help"|*)
        echo "🔧 FACO Pipeline - Comandos:"
        echo ""
        echo "  setup              - Configurar ambiente"
        echo "  deploy             - Desplegar tablas y SPs"
        echo "  execute_daily      - Ejecutar para una fecha"
        echo "  execute_historical - Procesar desde 14/05/2025"
        echo "  monitor           - Monitorear fecha específica"
        echo "  show_summary      - Resumen de registros"
        echo "  full_deployment   - Despliegue completo"
        echo ""
        echo "📅 Ejemplos:"
        echo "  ./faco_pipeline.sh full_deployment"
        echo "  ./faco_pipeline.sh execute_daily 2025-05-14"
        echo "  ./faco_pipeline.sh monitor 2025-06-20"
        echo ""
        ;;
esac
