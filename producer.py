import json
import time
import random
import uuid
import numpy as np
from kafka import KafkaProducer
import redis

# Conexión a Redis (solo para limpiar métricas anteriores)
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def init_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Productor conectado a Kafka con éxito.")
            return producer
        except Exception as e:
            print("Esperando a que Kafka inicie...")
            time.sleep(3)

def generar_trafico(producer, num_consultas=5000, distribucion='uniforme'):
    print(f"Iniciando inyección de {num_consultas} consultas ({distribucion})...")
    zonas = ['Z1', 'Z2', 'Z3', 'Z4', 'Z5']
    tipos_consulta = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
    
    if distribucion == 'uniforme':
        probabilidades = [0.2, 0.2, 0.2, 0.2, 0.2]
    else: # Zipf
        s = 1.5
        probs = np.array([1/(i**s) for i in range(1, 6)])
        probabilidades = probs / probs.sum()

    for i in range(num_consultas):
        zona = np.random.choice(zonas, p=probabilidades)
        tipo = random.choice(tipos_consulta)
        confianza = random.choice([0.0, 0.5, 0.8])
        
        mensaje = {
            'id_consulta': str(uuid.uuid4()),
            'tipo': tipo,
            'zona': zona,
            'confianza': confianza,
            'timestamp_creacion': time.time(),
            'retry_count': 0
        }
        
        # Enviar al tópico principal
        producer.send('consultas_main', value=mensaje)
        r.incr('metricas:total_enviadas')
        
        # Simulamos un pequeño retraso natural de llegada de peticiones
        time.sleep(0.001) 
        
    producer.flush()
    print(f"Inyección de {distribucion} finalizada.")

if __name__ == "__main__":
    r.flushall() # Limpiamos caché y métricas al arrancar
    producer = init_kafka_producer()
    
    # Damos tiempo a los consumidores de levantar
    time.sleep(10) 
    
    # Lanzamos el tráfico
    generar_trafico(producer, 2000, 'uniforme')
    time.sleep(5) # Pausa entre experimentos
    generar_trafico(producer, 2000, 'zipf')
    
    print("El Productor ha terminado su trabajo. Revisa los logs de los consumidores.")