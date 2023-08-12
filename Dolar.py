import requests
import csv
import os
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import psycopg2

def fetch_exchange_data():
    url = "https://api.bluelytics.com.ar/v2/latest"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener los datos de la API")
        return None

def create_or_append_csv(data):
    file_exists = os.path.exists("Outputs/Dolar.csv")
    with open("Outputs/Dolar.csv", "a", newline="") as csvfile:
        fieldnames = ["D.Oficial", "D.blue", "D.Of+Imp", "Fecha"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        oficial_value = data["oficial"]["value_avg"]
        blue_value = data["blue"]["value_avg"]
        last_update = data["last_update"]

        # Calculate D.Of+Imp (D.Oficial multiplicado por 1.85)
        oficial_imp_value = oficial_value * 1.85

        writer.writerow({
            "D.Oficial": oficial_value,
            "D.blue": blue_value,
            "D.Of+Imp": oficial_imp_value,
            "Fecha": last_update})

if __name__ == "__main__":
    exchange_data = fetch_exchange_data()
    if exchange_data:
        create_or_append_csv(exchange_data)
        
# Cargar datos desde Dolar.csv a un DataFrame
dolar_df = pd.read_csv("Outputs/Dolar.csv")

# Eliminar las líneas duplicadas y mantener solo una copia de cada registro
dolar_df.drop_duplicates(keep='first', inplace=True)

# Guardar los datos actualizados en el archivo "Dolar.csv" sin las líneas duplicadas
dolar_df.to_csv("Outputs/Dolar.csv", index=False)


dbshema=f'mbellesi01_coderhouse'
conn= sa.create_engine('postgresql://mbellesi01_coderhouse:contraseña@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database')


# Drop the existing "Dolar" table if it exists
table_name_dolar = 'dolar'
conn.execute(f"DROP TABLE IF EXISTS  {table_name_dolar}")

# Create the "Dolar" table with the correct data types
dolar_df.to_sql('dolar', conn, index=False, method="multi", if_exists='replace')

# Cargar datos desde steamconprecios.csv a otro DataFrame
steam_df = pd.read_csv("Outputs/steamantesydespues.csv", delimiter=";")

# Cambiar el nombre de la columna "Precios" a "Precio"
steam_df.rename(columns={"Precios": "Precio"}, inplace=True)

# Reemplazar "Free" con un valor predeterminado (por ejemplo, 0) antes de la conversión
steam_df["Precio"] = steam_df["Precio"].replace({"Free": "0"}, regex=True)

# Eliminar "ARS$" del campo "Precio"
steam_df["Precio"] = steam_df["Precio"].replace({"ARS\$": ""}, regex=True)

# Eliminar puntos en el campo "Precio"
steam_df["Precio"] = steam_df["Precio"].replace("\.", "", regex=True)

# Reemplazar comas por puntos en el campo "Precio"
steam_df["Precio"] = steam_df["Precio"].replace(",", ".", regex=True)

# Convertir el campo "Precio" a números de punto flotante (float)
steam_df["Precio"] = steam_df["Precio"].astype(float)


# Reemplazar "Free" con un valor predeterminado (por ejemplo, 0) antes de la conversión
steam_df["Precio Anterior"] = steam_df["Precio Anterior"].replace({"Free": "0"}, regex=True)

# Eliminar "ARS$" del campo "Precio"
steam_df["Precio Anterior"] = steam_df["Precio Anterior"].replace({"ARS\$": ""}, regex=True)

# Eliminar puntos en el campo "Precio"
steam_df["Precio Anterior"] = steam_df["Precio Anterior"].replace("\.", "", regex=True)

# Reemplazar comas por puntos en el campo "Precio"
steam_df["Precio Anterior"] = steam_df["Precio Anterior"].replace(",", ".", regex=True)

# Convertir el campo "Precio" a números de punto flotante (float)
steam_df["Precio Anterior"] = steam_df["Precio Anterior"].astype(float)




# Nombre de la tabla para los datos de Steam
table_name_steam = 'steam'
conn.execute(f"DROP TABLE IF EXISTS  {table_name_steam}")
   
# Cargar los datos del DataFrame steam_df en la tabla
steam_df.to_sql(table_name_steam, conn, index=False, method="multi", if_exists='replace')




# Establecer la conexión a Redshift
conn = psycopg2.connect(
    host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    port="5439",
    dbname="data-engineer-database",
    user="mbellesi01_coderhouse",
    password="contraseña"
)

# Crear un cursor para ejecutar consultas
cur = conn.cursor()

# Elimina la tabla "new_steam" anterior
drop_table_query = "DROP TABLE IF EXISTS new_steam"
cur.execute(drop_table_query)
conn.commit()

# Crear una nueva tabla con la estructura deseada y "new_appid" como clave primaria
create_new_table_query = """
    CREATE TABLE new_steam (
        ID VARCHAR PRIMARY KEY,
        Nombre VARCHAR,
        Precio NUMERIC,
        "Precio Anterior" NUMERIC
    );
"""

cur.execute(create_new_table_query)
conn.commit()

# Insertar datos de la tabla "steam" en la nueva tabla "new_steam"
insert_data_query = """
    INSERT INTO new_steam (ID, Nombre, Precio, "Precio Anterior")
    SELECT appid, name, precio, "precio anterior"
    FROM steam
    WHERE appid IS NOT NULL;
"""

cur.execute(insert_data_query)
conn.commit()


# Elimina la tabla "new_dolar" anterior
drop_new_dolar = "DROP TABLE IF EXISTS new_dolar"
cur.execute(drop_new_dolar)
conn.commit()

# Crear una nueva tabla con la estructura deseada y "fecha" como clave primaria
create_new_dolar = """
    CREATE TABLE new_dolar (
        Dolar_Oficial NUMERIC,
        Dolar_Blue NUMERIC,
        "Dolar_Of+imp" NUMERIC,
        "Fecha" TIMESTAMP PRIMARY KEY
    );
"""

cur.execute(create_new_dolar)
conn.commit()

# Insertar datos de la tabla "steam" en la nueva tabla "new_steam"
insert_new_dolar = """
    INSERT INTO new_dolar (Dolar_Oficial, Dolar_Blue, "Dolar_Of+imp", "Fecha")
    SELECT "d.oficial", "d.blue", "d.of+imp", TO_TIMESTAMP("Fecha", 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "Fecha"
    FROM dolar
    WHERE fecha IS NOT NULL;
"""


cur.execute(insert_new_dolar)
conn.commit()

# Elimina las tablas anteriores
drop_dolar = "DROP TABLE IF EXISTS dolar"
cur.execute(drop_dolar)
conn.commit()

drop_steam = "DROP TABLE IF EXISTS steam"
cur.execute(drop_steam)
conn.commit()


# Cerrar el cursor y la conexión
cur.close()
conn.close()