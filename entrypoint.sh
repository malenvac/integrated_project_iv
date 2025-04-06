#!/bin/bash

echo "✅ Inicializando la base de datos de Airflow..."
airflow db init || true

echo "✅ Creando usuario admin..."
airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com || true

echo "🚀 Ejecutando: $@"
exec airflow "$@"
