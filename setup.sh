#!/bin/bash

printf 'Activando docker compose'
# Run docker-compose
docker-compose up -d

# Wait for Neo4j to start up
printf 'Esperando al servicio de neo4j.'
until $(curl --output /dev/null --silent --head --fail http://localhost:7474); do
    printf '.'
    sleep 1
done

printf 'Instalando dependencias de python.'
# Install Python dependencies
pip install -r requirements.txt

printf 'Importando datos de csv a base de datos neo4j.'
#setup database
python neo4j/data_import.py


printf '¡Listo!'
