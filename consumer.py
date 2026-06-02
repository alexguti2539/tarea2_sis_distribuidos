import json
import time
import random
import os
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import redis

CONSUMER_ID = os.environ.get('CONSUMER_ID', '1')

# 1. Cargar Datos (Misma lógica ultra rápida de la Tarea 1)
df = pd.read_csv('datos/santiago_zonas_limpio.csv')
data = {
    zona: df[df['zona_id'] == zona]
    for zona in df['zona_id'].unique()
}
print(f"Consumidor {CONSUMER_ID}: Datos espaciales cargados en RAM.")

# Conexiones
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
TTL_CACHE = 60
MAX_RETRIES = 3

# Esperar a Kafka
time.sleep(8)
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    'consultas_main', 'consultas_retry',
    bootstrap_servers=['kafka:9092'],
    group_id='grupo-consultas',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

def procesar_consulta_pandas(msg):
    # Simula el cálculo (Simplificado para el ejemplo, pero usa tus datos)
    zona = msg['zona']
    conf = msg['confianza']
    registros = data.get(zona, pd.DataFrame())
    conteo = len(registros[registros['confidence'] >= conf])
    return {"resultado": conteo, "zona": zona}

print(f"Consumidor {CONSUMER_ID} listo y escuchando tópicos...")

for message in consumer:
    msg = message.value
    topic = message.topic
    
    start_time = time.time()
    cache_key = f"{msg['tipo']}:{msg['zona']}:{msg['confianza']}"
    
    # 1. Revisar Caché
    cached_response = r.get(cache_key)
    if cached_response:
        r.incr('metricas:cache_hits')
        # Registrar latencia (Hit)
        latencia = (time.time() - msg['timestamp_creacion']) * 1000
        r.lpush('metricas:latencias', latencia)
        continue
        
    r.incr('metricas:cache_misses')

    # 2. Simulación de Falla Temporal (15% de probabilidad de fallar)
    if random.random() < 0.15:
        retry_count = msg['retry_count']
        
        if retry_count >= MAX_RETRIES:
            # Mandar a DLQ
            producer.send('consultas_dlq', value=msg)
            r.incr('metricas:dlq_count')
            print(f"Consumidor {CONSUMER_ID}: Consulta {msg['id_consulta']} enviada a DLQ.")
        else:
            # Mandar a Reintento
            msg['retry_count'] += 1
            producer.send('consultas_retry', value=msg)
            r.incr('metricas:reintentos_count')
            print(f"Consumidor {CONSUMER_ID}: Falla simulada. Reintentando ({msg['retry_count']}/{MAX_RETRIES}).")
        continue

    # 3. Procesamiento exitoso (Generador de Respuestas)
    respuesta = procesar_consulta_pandas(msg)
    
    # Guardar en Caché
    r.setex(cache_key, TTL_CACHE, json.dumps(respuesta))
    
    # Si venía de un reintento y se recuperó, anotarlo en la métrica Recovery Rate
    if msg['retry_count'] > 0:
        r.incr('metricas:recovery_count')
        
    r.incr('metricas:consultas_exitosas')
    latencia = (time.time() - msg['timestamp_creacion']) * 1000
    r.lpush('metricas:latencias', latencia)