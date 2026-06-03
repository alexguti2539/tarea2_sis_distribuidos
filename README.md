# Tarea 2: Procesamiento Asíncrono y Tolerancia a Fallos

## Descripción del Proyecto
Esta arquitectura implementa un sistema distribuido asíncrono diseñado para procesar consultas espaciales masivas de la ciudad de Santiago. Para soportar picos de tráfico y garantizar la tolerancia a fallos, el sistema original síncrono (Pandas) fue desacoplado utilizando **Apache Kafka** como Message Broker y **Redis** como sistema de caché, todo orquestado mediante **Docker Compose**.

El sistema cuenta con:
- **1 Productor:** Genera ráfagas de 4000 consultas usando distribuciones Zipf y Uniforme.
- **2 Consumidores Paralelos:** Extraen consultas del tópico principal, validan resultados en caché y simulan fallos con una probabilidad del 15% para demostrar las políticas de reintentos y la derivación a una Dead Letter Queue (DLQ).
- **Caché Protectora:** Reduce drásticamente la carga de procesamiento, logrando un Cache Hit Rate cercano al 98%.

---

##  Requisitos Previos
Para ejecutar este proyecto, solo necesitas tener instalado:
- **Docker**
- **Docker Compose** (Viene incluido en Docker Desktop)
- *Git (para clonar el repositorio)*

---

##  Guía de Ejecución (Paso a Paso)

Sigue estos comandos en tu terminal para levantar la arquitectura y visualizar las métricas.

### 1. Clonar el repositorio
```bash
git clone [https://github.com/alexguti2539/tarea2_sis_distribuidos.git](https://github.com/alexguti2539/tarea2_sis_distribuidos.git)
cd tarea2_sis_distribuidos
```

### 2. Levantar la Arquitectura
Levantaremos los servicios de Zookeeper, Kafka, Redis, el Productor y los Consumidores en segundo plano (modo detached) para evitar problemas con la retención del *Backlog*.
```bash
docker compose up -d --build
```


### 3. Verificar la inyección de tráfico masivo (Spike)
Podemos monitorear al Productor para asegurarnos de que inyectó las 4000 peticiones correctamente:
```bash
docker compose logs -f producer
```


### 4. Observar el procesamiento y la Tolerancia a Fallos
Mientras el Productor encolaba mensajes, los dos Consumidores paralelos ya están trabajando para vaciar la cola. Podemos ver cómo atajan los errores y hacen los reintentos:
```bash
docker compose logs -f consumer_1 consumer_2
```

### 5. Extraer las Métricas Finales
Una vez procesadas todas las peticiones, ejecutamos el monitor de analíticas para obtener la tabla final con los tiempos de latencia (p50/p95), el Hit Rate de la caché y los porcentajes de reintentos:
```bash
docker compose exec consumer_1 python monitor.py
```

---

##  Estructura de Archivos Principales
- `docker-compose.yml`: Orquestador de la infraestructura (Kafka, Zookeeper, Redis, Productor, Consumidores).
- `producer.py`: Generador de tráfico asíncrono.
- `consumer.py`: Trabajadores que consultan Redis, calculan en Pandas, simulan fallos y envían a la DLQ.
- `monitor.py`: Script para el cálculo estadístico y extracción de métricas desde Redis.
- `datos/santiago_zonas_limpio.csv`: Dataset geoespacial utilizado por los consumidores.
