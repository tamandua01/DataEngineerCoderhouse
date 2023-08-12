import os
import requests
import json

def get_app_list():
    url = "http://api.steampowered.com/ISteamApps/GetAppList/v0001/"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener la lista de aplicaciones:", response.status_code)
        return None

def save_to_json(data, folder, filename):
    os.makedirs(folder, exist_ok=True)  # Create the folder if it doesn't exist
    filepath = os.path.join(folder, filename)
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=4)

if __name__ == "__main__":
    app_list_data = get_app_list()

    if app_list_data:
        backup_folder = "Backups"
        filename = "app_list.json"
        save_to_json(app_list_data, backup_folder, filename)
        print(f"La lista de aplicaciones ha sido guardada en {backup_folder}/{filename}")
