import os
import requests
import csv
from influxdb_client import InfluxDBClient, Point

# --- ThingSpeak ---
CHANNEL_ID = "3216531"       # Remplace par ton Channel ID
READ_API_KEY = READ_API_KEY = os.environ.get("THING_SPEAK_API_KEY")       # Remplace par ta Read API Key securisé via github avec un secret

# --- InfluxDB Cloud ---
INFLUX_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com"  # ton Cluster URL
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN") # Custom API Token Read/Write securisé via github avec un secret
INFLUX_ORG = "SourisRose" # Nom de ton organisation
INFLUX_BUCKET = "ruche1" # Nom du bucket
   

# --- CSV archive ---
CSV_FILE = "archive_ruche.csv"

# --- Lire ThingSpeak ---
url = f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json?api_key={READ_API_KEY}&results=200"
r = requests.get(url)

print(f"ThingSpeak status code: {r.status_code}")
print(f"Preview response: {r.text[:200]}")  # affiche les 200 premiers caractères pour debug

try:
    data = r.json()
except ValueError:
    print("Erreur : le retour de ThingSpeak n'est pas un JSON")
    data = {"feeds": []}

if "feeds" not in data:
    print("Attention : aucune donnée reçue de ThingSpeak")
    data["feeds"] = []


# --- InfluxDB ---
with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
    write_api = client.write_api()

    # --- Ouvrir CSV en mode ajout ---
    with open(CSV_FILE, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for feed in data["feeds"]:
            if feed["field1"] is not None:
                timestamp = feed["created_at"]
                temp_int1 = feed["field1"]
                temp_int2 = feed["field2"]
                poids = feed["field3"]
                temp_ext = feed["field4"]
                humidite = feed["field5"]
                pression = feed["field6"]

                # --- Écrire dans InfluxDB ---
                point = (
                    Point("ruche")
                    .field("temp_int1", float(temp_int1))
                    .field("temp_int2", float(temp_int2))
                    .field("poids", float(poids))
                    .field("temp_ext", float(temp_ext))
                    .field("humidite", float(humidite))
                    .field("pression", float(pression))
                    .time(timestamp)
                )
                write_api.write(bucket=INFLUX_BUCKET, record=point)

                # --- Écrire dans CSV pour archive 4 ans ---
                writer.writerow([timestamp, temp_int1, temp_int2, poids, temp_ext, humidite, pression])


print("Sync ThingSpeak → InfluxDB + CSV terminé ✅")
