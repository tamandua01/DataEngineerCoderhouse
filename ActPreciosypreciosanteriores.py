import pandas as pd
import requests
import time
from tqdm import tqdm  # Importar tqdm para mostrar la barra de progreso


def get_prices_in_batches(appids_to_check):
    batch_size = 50
    total_batches = len(appids_to_check) // batch_size
    for i in range(0, len(appids_to_check), batch_size):
        batch = appids_to_check[i:i+batch_size]
        appids_str = ','.join(map(str, batch))
        prices_data = get_price(appids_str)
        update_prices(prices_data)
        time.sleep(1)  # Esperar 1 segundo antes de la siguiente solicitud
        # Actualizar la barra de progreso en la misma línea
        progress = (i + batch_size) / len(appids_to_check) * 100
        tqdm.write(f"Progress: {progress:.2f}%", end="\r")  # Use end="\r" to stay on the same line



def get_price(appids_str):
    url_prices = f"https://store.steampowered.com/api/appdetails/?filters=price_overview&appids={appids_str}"
    response_prices = requests.get(url_prices)
    data_prices = response_prices.json()

    prices_data = {}
    for appid in appids_str.split(","):
        try:
            data = data_prices[appid]["data"]
            if isinstance(data, dict):
                price = data.get("price_overview", {}).get("final_formatted", "N/A")
            else:
                price = "N/A"
            prices_data[appid] = price
        except (KeyError, TypeError):
            print(f"Error fetching price for app ID {appid}")
            prices_data[appid] = None

    return prices_data
    
def update_prices(prices_data):
    df = pd.read_csv("Outputs/steamantesydespues.csv", sep=";", encoding="utf-8-sig")
    for appid, price in prices_data.items():
        df.loc[df["appid"] == int(appid), "Precios"] = price
    df.to_csv("Outputs/steamantesydespues.csv", index=False, sep=";", encoding="utf-8-sig")

# Leer el archivo "Precios steam.csv"
df = pd.read_csv("Outputs/steamantesydespues.csv", sep=";", encoding="utf-8-sig")

# Crear una copia y mover los valores a la nueva columna
df_copy = df.copy()
df_copy["Precio Anterior"] = df_copy["Precios"]
df_copy["Precios"] = None

# Guardar la copia en "steamantesydespues.csv"
df_copy.to_csv("Outputs/steamantesydespues.csv", index=False, sep=";", encoding="utf-8-sig")

# Leer el archivo "Precios steam.csv"
df = pd.read_csv("Outputs/steamantesydespues.csv", sep=";", encoding="utf-8-sig")

# Controlar las filas para obtener las que tienen la columna "Precios" vacía
reed_rows = df
rows_with_empty_prices = reed_rows[reed_rows["Precios"].isnull()]

# Obtener los precios y actualizar las filas correspondientes
if not rows_with_empty_prices.empty:
    appids_to_check = rows_with_empty_prices["appid"].tolist()
    print("App IDs to check:", appids_to_check)
    tqdm.write("Progress: 0.00%", end="\r")  # Mostrar 0% de progreso antes de iniciar el proceso
    get_prices_in_batches(appids_to_check)
    print("Prices updated.")
    
print("Proceso completado.")
