import pandas as pd
import redis
import os
import json
import time
import random
import numpy as np

# --- 1. GENERADOR DE RESPUESTAS ---
class GeneradorRespuestas:
    def __init__(self, csv_path):
        print("Cargando dataset en memoria...")
        self.data = {}
        df = pd.read_csv(csv_path)
        for zone_id, group in df.groupby('zona_id'):
            self.data[zone_id] = group
            
        self.zone_areas_km2 = {
            'Z1': 10.3, 'Z2': 15.5, 'Z3': 20.8, 'Z4': 12.4, 'Z5': 20.8
        }
        print("¡Dataset precargado!")

    def q1_count(self, zone_id, confidence_min=0.0):
        df_zone = self.data.get(zone_id)
        if df_zone is None: return 0
        return int((df_zone['confidence'] >= confidence_min).sum())

    def q2_area(self, zone_id, confidence_min=0.0):
        df_zone = self.data.get(zone_id)
        if df_zone is None or df_zone.empty: return {"avg_area": 0, "total_area": 0, "n": 0}
        filtered = df_zone[df_zone['confidence'] >= confidence_min]['area_in_meters']
        if filtered.empty: return {"avg_area": 0, "total_area": 0, "n": 0}
        return {"avg_area": float(filtered.mean()), "total_area": float(filtered.sum()), "n": len(filtered)}

    def q3_density(self, zone_id, confidence_min=0.0):
        count = self.q1_count(zone_id, confidence_min)
        area_km2 = self.zone_areas_km2.get(zone_id, 1.0)
        return float(count / area_km2)

    def q4_compare(self, zone_a, zone_b, confidence_min=0.0):
        da = self.q3_density(zone_a, confidence_min)
        db = self.q3_density(zone_b, confidence_min)
        return {"zone_a": da, "zone_b": db, "winner": zone_a if da > db else zone_b}

    def q5_confidence_dist(self, zone_id, bins=5):
        df_zone = self.data.get(zone_id)
        if df_zone is None or df_zone.empty: return []
        intervalos = pd.interval_range(start=0, end=1, periods=bins)
        cortes = pd.cut(df_zone['confidence'], bins=intervalos)
        conteos = cortes.value_counts().sort_index()
        return [{"bucket": i, "min": round(inv.left, 2), "max": round(inv.right, 2), "count": int(c)} for i, (inv, c) in enumerate(conteos.items())]


# --- 2. SISTEMA DE CACHÉ ---
class SistemaCache:
    def __init__(self, generador):
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        self.redis.flushall() 
        self.generador = generador
        self.ttl = 60

    def procesar_consulta(self, tipo_consulta, params):
        if tipo_consulta == 'q1': cache_key = f"count:{params['zone_id']}:conf={params.get('confidence_min', 0.0)}"
        elif tipo_consulta == 'q2': cache_key = f"area:{params['zone_id']}:conf={params.get('confidence_min', 0.0)}"
        elif tipo_consulta == 'q3': cache_key = f"density:{params['zone_id']}:conf={params.get('confidence_min', 0.0)}"
        elif tipo_consulta == 'q4': cache_key = f"compare:{params['zone_a']}:{params['zone_b']}:conf={params.get('confidence_min', 0.0)}"
        elif tipo_consulta == 'q5': cache_key = f"dist:{params['zone_id']}:bins={params.get('bins', 5)}"

        resultado_cacheado = self.redis.get(cache_key)

        if resultado_cacheado:
            # Es un HIT, retornamos True
            return json.loads(resultado_cacheado), True
        else:
            # Es un MISS, calculamos, guardamos y retornamos False
            if tipo_consulta == 'q1': res = self.generador.q1_count(**params)
            elif tipo_consulta == 'q2': res = self.generador.q2_area(**params)
            elif tipo_consulta == 'q3': res = self.generador.q3_density(**params)
            elif tipo_consulta == 'q4': res = self.generador.q4_compare(**params)
            elif tipo_consulta == 'q5': res = self.generador.q5_confidence_dist(**params)

            self.redis.setex(cache_key, self.ttl, json.dumps(res))
            return res, False


# --- 3. ALMACENAMIENTO DE MÉTRICAS ---
class AlmacenamientoMetricas:
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.latencias = []
        self.start_time = None

    def iniciar_reloj(self):
        self.start_time = time.time()

    def registrar(self, is_hit, latencia):
        if is_hit: self.hits += 1
        else: self.misses += 1
        self.latencias.append(latencia)

    def mostrar_reporte(self):
        total = self.hits + self.misses
        hit_rate = self.hits / total if total > 0 else 0
        tiempo_total = time.time() - self.start_time
        throughput = total / tiempo_total if tiempo_total > 0 else 0
        
        # Convertimos a milisegundos
        p50 = np.percentile(self.latencias, 50) * 1000 if self.latencias else 0
        p95 = np.percentile(self.latencias, 95) * 1000 if self.latencias else 0

        print("\n" + "="*30)
        print("REPORTE DE MÉTRICAS")
        print("="*30)
        print(f"Total Consultas : {total}")
        print(f"Aciertos (Hits) : {self.hits}")
        print(f"Fallos (Misses) : {self.misses}")
        print(f"Hit Rate        : {hit_rate:.2%}")
        print(f"Throughput      : {throughput:.2f} consultas/segundo")
        print(f"Latencia p50    : {p50:.2f} ms")
        print(f"Latencia p95    : {p95:.2f} ms")
        print("="*30 + "\n")


# --- 4. GENERADOR DE TRÁFICO ---
class GeneradorTrafico:
    def __init__(self, cache, metricas):
        self.cache = cache
        self.metricas = metricas
        self.zonas = ['Z1', 'Z2', 'Z3', 'Z4', 'Z5']
        self.tipos_consulta = ['q1', 'q2', 'q3', 'q4', 'q5']

    def simular(self, num_consultas=500, distribucion='uniforme'):
        print(f"\nIniciando simulación de tráfico ({num_consultas} consultas) - Distribución: {distribucion.upper()}")
        self.metricas.iniciar_reloj()

        for _ in range(num_consultas):
            tipo = random.choice(self.tipos_consulta)
            confianza = random.choice([0.0, 0.5, 0.8])

            # Selección de zona según distribución
            if distribucion == 'uniforme':
                zona = random.choice(self.zonas)
            else: # Distribución Zipf
                idx = min(np.random.zipf(2.0), 5) - 1
                zona = self.zonas[idx]

            if tipo == 'q4':
                params = {'zone_a': zona, 'zone_b': random.choice(self.zonas), 'confidence_min': confianza}
            elif tipo == 'q5':
                params = {'zone_id': zona, 'bins': 5} 
            else:
                params = {'zone_id': zona, 'confidence_min': confianza}

            # Medir y procesar
            t_inicio = time.time()
            _, is_hit = self.cache.procesar_consulta(tipo, params)
            t_fin = time.time()

            # Guardar en métricas
            self.metricas.registrar(is_hit, (t_fin - t_inicio))
        
        self.metricas.mostrar_reporte()


# --- FLUJO PRINCIPAL ---
def main():
    generador_resp = GeneradorRespuestas('datos/santiago_zonas_limpio.csv')
    cache = SistemaCache(generador_resp)
    
    # 1. Prueba con distribución Uniforme
    metricas_uni = AlmacenamientoMetricas()
    trafico_uni = GeneradorTrafico(cache, metricas_uni)
    trafico_uni.simular(num_consultas=10000, distribucion='uniforme')

    # 2. Prueba con distribución Zipf
    cache.redis.flushall()
    metricas_zipf = AlmacenamientoMetricas()
    trafico_zipf = GeneradorTrafico(cache, metricas_zipf)
    trafico_zipf.simular(num_consultas=10000, distribucion='zipf')

if __name__ == "__main__":
    main()