import os
import requests
import csv
from influxdb_client import InfluxDBClient, Point, WritePrecision

# --- ThingSpeak ---
READ_API_KEY = os.environ.get("READ_API_KEY")
CHANNEL_ID = os.environ.get("CHANNEL_ID")

# --- InfluxDB Cloud ---
INFLUX_URL = os.environ.get("INFLUX_URL") # ton Cluster URL
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET")  # Nom du bucket

CSV_FILE = os.environ.get("CSV_FILE", "archive_ruche.csv")

print(f"DEBUG → INFLUX_BUCKET = {INFLUX_BUCKET}")
print(f"DEBUG → CHANNEL_ID = {CHANNEL_ID}")   

# --- CSV archive ---
CSV_FILE = "archive_ruche.csv"

# --- Créer CSV avec header si inexistant ---
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp","temp_int1","temp_int2","poids","temp_ext","humidite","pression"])

# --- Fonction pour convertir float safely ---
def to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0

# --- Lire ThingSpeak ---
url = f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json?api_key={READ_API_KEY}&results=200"
r = requests.get(url)
print(f"ThingSpeak status code: {r.status_code}")
print(f"Preview response: {r.text[:200]}")  # debug

try:
    data = r.json()
except ValueError:
    print("Erreur : le retour de ThingSpeak n'est pas un JSON")
    data = {"feeds": []}

if "feeds" not in data:
    print("Attention : aucune donnée reçue de ThingSpeak")
    data["feeds"] = []

# --- InfluxDB + CSV ---
with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
    write_api = client.write_api()

    # Ouvrir CSV en mode ajout
    with open(CSV_FILE, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)

        for feed in data["feeds"]:
            timestamp = feed.get("created_at")
            temp_int1 = to_float(feed.get("field1"))
            temp_int2 = to_float(feed.get("field2"))
            poids = to_float(feed.get("field3"))
            temp_ext = to_float(feed.get("field4"))
            humidite = to_float(feed.get("field5"))
            pression = to_float(feed.get("field6"))

            # --- Écriture dans InfluxDB ---
            point = (
                Point("ruche")  # measurement
                .time(timestamp)
                .field("temp_int1", temp_int1)
                .field("temp_int2", temp_int2)
                .field("poids", poids)
                .field("temp_ext", temp_ext)
                .field("humidite", humidite)
                .field("pression", pression)
            )
            write_api.write(bucket=INFLUX_BUCKET, record=point, write_precision=WritePrecision.NS)
            print(f"DEBUG → point écrit {timestamp} | temp1={temp_int1} | temp2={temp_int2} | poids={poids}")

            # --- Écriture dans CSV ---
            writer.writerow([timestamp, temp_int1, temp_int2, poids, temp_ext, humidite, pression])

print("Sync ThingSpeak → InfluxDB + CSV terminé ✅")










