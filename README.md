# Requerimientos

* Docker Compose
* .venv como ambiente virtual de python.

# Manual de uso
Todo esto debería hacerse dentro de un ambiente virtual de python y dentro del directorio del proyecto. 

## Instalación Rápida
```
chmod +x setup.sh
```
```
./setup.sh
```

## Instalación Manual

```
docker compose up -d 
```

```
pip install -r requirements.txt
```
Correr la importación de los datos en [neo4j/data_import.py](data_import.py) (puede tardarse unos minutos).

```
python neo4j/data_import.py
```

## Puertos pertinentes

* El browser de neo4j estará disponible en http://localhost:7474/browser/
* La base de datos estará disponible en http://localhost:7687
* El servicio de dashboard neodash estará disponible en http://localhost:5005

## Neodash y documentación.

Dentro de neodash, importar los dashboards con el archivo ubicado en [dashboard_importaciones](/neo4j/dashboard_importaciones.json).json

Para revisar la documentación de los queries, revisar el archivo ubicado en [neo4j_queries.ipynb](neo4j/neo4j_queries.ipynb)
