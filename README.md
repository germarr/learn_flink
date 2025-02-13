

# 1. Por donde empezar?
## 1.1 Producers
Necesitamos nuestras fuentes de datos. Para este ejemplo tengo un folder que se llama `producers` y tengo 2 scripts que generan informacion y la envian a Kafka.
`weather.py` y `bitcoin.py`. 

## 1.2 Kafka Alone
Si quieres iniciar una instancia de Kafka rapido usa 
```bash
pip install quixstreams
```
Esto instala el CLI de quix. Despues corre
```bash
git init
quix init
quix pipeline up
```
Esto inicia un contenedor de Docker con Kafka lsito para usarse.


``
## 1.3 Docker. 
Todo empieza por Docker.
Primero necesitas crear una nueva imagen que esta en el archivo `Dockerfile`.
Este archivo va a generar una imagen que trae Flink sumado a la configuracion necesaria para correr PyFlink.

```bash
docker build -t custom-flink:latest -f DockerFile .
```
Ya que generas la imagen, usa el archivo `docker-compose` para generar los *containers* necesarios para correr el proyecto.
Especificamente vamos a usar Redpanda, que es una plataforma que creo una instancia de Kafka muy rapida y facil de configurar, sumado a que crearon una UI muy limpia para administrar Kafka desde ahi.

```bash
docker-compose up -d
```

Para entrar al container que tiene el sql-client
```bash
docker exec -it <container-name> /bin/bash
./bin/sql-client.sh
```

# 2. Consumir Kafka data
En el folder `consumers` hay un script de Pyflink que arranca un proceso para stremear datos.
Este proceso se debe arrancar en el jobamanger de flink para que comience a consumir data. 
Para arrancarl el task se usa este script
```bash
docker-compose exec jobmanager ./bin/flink run -py /opt/consumers/bitcoin_consumer.py -d
```

Y puedes monitorear cuantos trabajos hay activados usando esto
```bash
docker-compose exec jobmanager ./bin/flink list
```
O usando el UI de Flink.


