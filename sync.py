import os
import requests
import csv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# --- Variables ---
READ_API_KEY = os.environ.get("READ_API_KEY")
CHANNEL_ID = os.environ.get("CHANNEL_ID")


INFLUX_URL = os.environ.get("INFLUX_URL")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET")

CSV_FILE = "archive_ruche.csv"

# --- Conversion float ---
def to_float(val):
    try:
        return float(val)
    except:
        return None

# --- Création CSV si absent ---
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "temp_int1",
            "temp_int2",
            "poids",
            "temp_ext",
            "humidite",
            "pression"
        ])

# --- Trouver dernière date enregistrée ---
last_timestamp = None
with open(CSV_FILE, "r") as f:
    rows = list(csv.reader(f))
    if len(rows) > 1:
        last_timestamp = rows[-1][0]

print("Dernier timestamp connu :", last_timestamp)

# --- Construire URL ThingSpeak ---
if last_timestamp:
    url = f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json?api_key={READ_API_KEY}&start={last_timestamp}"
else:
    url = f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json?api_key={READ_API_KEY}&results=50"

r = requests.get(url)

if r.status_code != 200:
    print("Erreur ThingSpeak :", r.status_code)
    exit()

feeds = r.json().get("feeds", [])
print(f"{len(feeds)} nouvelles mesures reçues")

if not feeds:
    print("Aucune nouvelle donnée.")
    exit()

# --- Envoi Influx ---
with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:

    write_api = client.write_api(write_options=SYNCHRONOUS)

    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        for feed in feeds:

            timestamp = feed.get("created_at")

            temp_int1 = to_float(feed.get("field1"))
            temp_int2 = to_float(feed.get("field2"))
            poids     = to_float(feed.get("field3"))
            temp_ext  = to_float(feed.get("field4"))
            humidite  = to_float(feed.get("field5"))
            pression  = to_float(feed.get("field6"))

            point = (
                Point("ruche")
                .time(timestamp)
                .field("temp_int1", temp_int1)
                .field("temp_int2", temp_int2)
                .field("poids", poids)
                .field("temp_ext", temp_ext)
                .field("humidite", humidite)
                .field("pression", pression)
            )

            write_api.write(bucket=INFLUX_BUCKET, record=point)

            writer.writerow([
                timestamp,
                temp_int1,
                temp_int2,
                poids,
                temp_ext,
                humidite,
                pression
            ])

print("Sync ruche terminé proprement ✅")

