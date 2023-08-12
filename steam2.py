import pandas as pd
import requests
import time

def get_prices_in_batches(appids_to_check):
    batch_size = 50
    for i in range(0, len(appids_to_check), batch_size):
        batch = appids_to_check[i:i+batch_size]
        appids_str = ','.join(map(str, batch))  # Formar el appids_str correctamente
        prices_data = get_price(appids_str)
        update_prices(prices_data)
        time.sleep(15)  # Esperar 15 segundos antes de la siguiente solicitud

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
    df = pd.read_csv("Datasets/Precios steam.csv", sep=";", encoding="utf-8-sig")
    for appid, price in prices_data.items():
        df.loc[df["appid"] == int(appid), "Precios"] = price
    df.to_csv("Datasets/Precios steam.csv", index=False, sep=";", encoding="utf-8-sig")

# Leer el archivo "Precios steam.csv"
df = pd.read_csv("Datasets/Precios steam.csv", sep=";", encoding="utf-8-sig")

# Controlar las filas para obtener las que tienen la columna "Precios" vacía
reed_rows = df
rows_with_empty_prices = reed_rows[reed_rows["Precios"].isnull()]

# Obtener los precios y actualizar las filas correspondientes
if not rows_with_empty_prices.empty:
    appids_to_check = rows_with_empty_prices["appid"].tolist()
    print("App IDs to check:", appids_to_check)
    get_prices_in_batches(appids_to_check)
    print("Prices updated.") 

print("Proceso completado.")