import pandas as pd
import os

# Nombre del archivo original
input_file = "Datasets/Precios steam.csv"
# Nombre del nuevo archivo CSV
output_file = "Outputs/steamconprecios.csv"

# Leer el archivo original "Precios steam.csv"
df = pd.read_csv(input_file, sep=";", encoding="utf-8-sig")

# Filtrar las filas con valores en la columna "Precios"
filtered_df = df.dropna(subset=["Precios"])

# Verificar si el archivo nuevo ya existe
if os.path.exists(output_file):
    # Si el archivo ya existe, cargar su contenido
    existing_df = pd.read_csv(output_file, sep=";", encoding="utf-8-sig")
    # Agregar las filas filtradas al archivo nuevo
    updated_df = pd.concat([existing_df, filtered_df], ignore_index=True)
    # Guardar el DataFrame actualizado en el archivo nuevo
    updated_df.to_csv(output_file, index=False, sep=";", encoding="utf-8-sig")
else:
    # Si el archivo no existe, copiar el encabezado de "Precios steam.csv" y guardar las filas filtradas
    filtered_df.to_csv(output_file, index=False, sep=";", encoding="utf-8-sig")

# Filtrar las filas con valores vacíos en la columna "Precios" (las filas que no se copiaron)
df_with_empty_prices = df[df["Precios"].isnull()]

# Sobreescribir el archivo original "Precios steam.csv" solo con las filas vacías
df_with_empty_prices.to_csv(input_file, index=False, sep=";", encoding="utf-8-sig")

print(f"Se han movido las filas con precios al archivo '{output_file}'.")
print(f"Las filas vacías se han mantenido en el archivo '{input_file}'.")
