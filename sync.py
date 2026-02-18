import os
import requests
import csv

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


# --- ThingSpeak ---
READ_API_KEY = os.environ.get("READ_API_KEY")
CHANNEL_ID = os.environ.get("CHANNEL_ID")

# --- InfluxDB Cloud ---
INFLUX_URL = os.environ.get("INFLUX_URL")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET")

CSV_FILE = "archive_ruche.csv"

print(f"DEBUG → INFLUX_BUCKET = '{INFLUX_BUCKET}'")
print(f"DEBUG → CHANNEL_ID = '{CHANNEL_ID}'")
print(f"DEBUG → ORG = '{INFLUX_ORG}'")
print(f"DEBUG → TOKEN existe ? {INFLUX_TOKEN is not None}")


# --- Créer CSV si inexistant ---
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp","temp_int1","temp_int2","poids","temp_ext","humidite","pression"])


# --- TEST ECRITURE SIMPLE ---
with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:

    # Mode SYNCHRONE (important pour GitHub)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        client.ping()
        print("Connexion InfluxDB OK ✅")
    except Exception as e:
        print("Erreur ping InfluxDB :", e)

    # Point test
    point = Point("ruche").field("test", 1.0)

    print("Tentative écriture point test...")
    write_api.write(bucket=INFLUX_BUCKET, record=point)
    print("Point écrit ✅")


print("Fin du script.")
