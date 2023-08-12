import pandas as pd
import requests

# Hacemos la solicitud a la primera API y obtenemos los datos
url = "http://api.steampowered.com/ISteamApps/GetAppList/v0001/"
response = requests.get(url)
data = response.json()

# Extraemos los datos necesarios para la tabla
apps = data["applist"]["apps"]["app"]
appids = [app["appid"] for app in apps]
names = [app["name"] for app in apps]

# Creamos la tabla con pandas
table_data = {"appid": appids, "name": names}
df = pd.DataFrame(table_data)

# Eliminamos todas las filas con valores repetidos en el campo "name"
df = df.drop_duplicates(subset=["name"], keep='first')

# Eliminamos todas las filas que incluyan las siguientes palabras en el campo "name"
keywords_to_exclude = ["demo", "test", "dlc", "Soundtrack", "artbook", "art book",
                       "ep.", "ticket", "trailer", "wallpaper", "patch", "vol.",
                       "chapter", "pack", "pass", "add-on", "beta", " set ", " - ", "content"]

for keyword in keywords_to_exclude:
    df = df[~df["name"].str.contains(keyword, case=False)]

# Reiniciamos los índices para que queden consecutivos después de eliminar filas
df.reset_index(drop=True, inplace=True)

# Agregar una nueva columna "Precios" con valores vacíos
df["Precios"] = ""

print(df)

# Exportar el DataFrame como un archivo CSV con ";" como delimitador
nombre_archivo_csv = "Datasets/Precios steam.csv"
df.to_csv(nombre_archivo_csv, index=False, sep=";", encoding="utf-8-sig")