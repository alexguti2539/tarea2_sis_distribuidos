import redis
import numpy as np

# Conectamos al Redis
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

print("\n" + "="*45)
print("   📊 MÉTRICAS DEL SISTEMA DISTRIBUIDO")
print("="*45)

try:
    total_enviadas = int(r.get('metricas:total_enviadas') or 0)
    exitosas_pandas = int(r.get('metricas:consultas_exitosas') or 0)
    cache_hits = int(r.get('metricas:cache_hits') or 0)
    cache_misses = int(r.get('metricas:cache_misses') or 0)
    
    reintentos = int(r.get('metricas:reintentos_count') or 0)
    dlq = int(r.get('metricas:dlq_count') or 0)
    recuperadas = int(r.get('metricas:recovery_count') or 0)
    
    # El total real es lo procesado por Pandas + lo rescatado de la Caché
    total_procesadas = exitosas_pandas + cache_hits
    
    latencias_str = r.lrange('metricas:latencias', 0, -1)
    latencias = [float(l) for l in latencias_str]

    print(f"📦 Total Consultas Enviadas a Kafka: {total_enviadas}")
    print(f"✅ Total Procesadas Exitosamente: {total_procesadas}")
    print(f"   ├─ Calculadas por Pandas (Nuevas): {exitosas_pandas}")
    print(f"   └─ Resueltas por Caché Redis: {cache_hits}")
    print(f"⚠️  Consultas que Fallaron y se Reintentaron: {reintentos}")
    print(f"🔄 Consultas Recuperadas tras fallar: {recuperadas}")
    print(f"🗑️  Consultas Perdidas (DLQ): {dlq}\n")
    
    print("-" * 45)
    print("   ⏱️  ANÁLISIS DE RENDIMIENTO")
    print("-" * 45)

    if latencias:
        p50 = np.percentile(latencias, 50)
        p95 = np.percentile(latencias, 95)
        print(f"Latencia Promedio (p50): {p50:.2f} ms")
        print(f"Latencia Peor Caso (p95): {p95:.2f} ms")
    
    if total_procesadas > 0:
        hit_rate = (cache_hits / (cache_hits + cache_misses)) * 100
        print(f"Cache Hit Rate (Efectividad Caché): {hit_rate:.2f}%")
        
    if total_enviadas > 0:
        retry_rate = (reintentos / total_enviadas) * 100
        dlq_rate = (dlq / total_enviadas) * 100
        print(f"Retry Rate (Tasa de Reintentos): {retry_rate:.2f}%")
        print(f"DLQ Rate (Tasa de Pérdida): {dlq_rate:.2f}%")
        
    if reintentos > 0:
        recovery_rate = (recuperadas / reintentos) * 100
        print(f"Recovery Rate (Tasa de Recuperación): {recovery_rate:.2f}%")

    print("="*45 + "\n")

except Exception as e:
    print(f"Error al leer las métricas: {e}")