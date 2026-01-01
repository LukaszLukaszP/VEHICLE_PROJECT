import requests
import json

url = "https://vpic.nhtsa.dot.gov/api//vehicles/GetManufacturerDetails/honda?format=json"
r = requests.get(url)
data = r.json()

# zapisz do pliku:
with open("sample_1.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)
