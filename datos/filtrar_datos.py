import pandas as pd

# 1. Cargar el dataset gigante (esto puede tomar un par de minutos dependiendo de tu RAM)
print("Cargando dataset original...")
df = pd.read_csv('datos/dataset_original.csv')

# 2. Definir las zonas de la rúbrica (lat_min, lat_max, lon_min, lon_max)
zonas = {
    'Z1': (-33.445, -33.420, -70.640, -70.600), # Providencia
    'Z2': (-33.420, -33.390, -70.600, -70.550), # Las Condes
    'Z3': (-33.530, -33.490, -70.790, -70.740), # Maipú
    'Z4': (-33.460, -33.430, -70.670, -70.630), # Santiago Centro
    'Z5': (-33.470, -33.430, -70.810, -70.760)  # Pudahuel
}

# 3. Filtrar y asignar el ID de la zona
dataframes_filtrados = []

for zona_id, (lat_min, lat_max, lon_min, lon_max) in zonas.items():
    # Filtramos las filas que caen dentro del bounding box
    df_zona = df[
        (df['latitude'] >= lat_min) & (df['latitude'] <= lat_max) &
        (df['longitude'] >= lon_min) & (df['longitude'] <= lon_max)
    ].copy()
    
    df_zona['zona_id'] = zona_id
    dataframes_filtrados.append(df_zona)

# 4. Unir todo en un dataframe limpio y guardarlo
df_final = pd.concat(dataframes_filtrados)
df_final.to_csv('datos/santiago_zonas_limpio.csv', index=False)
print("¡Listo! Archivo filtrado creado en datos/santiago_zonas_limpio.csv")