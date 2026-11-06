import json, os, random

random.seed(7)
HERE = os.path.dirname(os.path.abspath(__file__))

CITIES = [  # ville, groupe hospitalier, lat, lon
    ("Casablanca", "GHT-Atlantique", 33.5731, -7.5898),
    ("Rabat",      "GHT-Atlantique", 34.0209, -6.8416),
    ("Marrakech",  "GHT-Sud",        31.6295, -7.9811),
    ("Tanger",     "GHT-Nord",       35.7595, -5.8340),
]
PLAN = ([("CHU", "")]
        + [("HOPITAL_REGIONAL", s) for s in ("Nord", "Sud")]
        + [("CLINIQUE", s) for s in ("Al Amal", "Ennour", "Atlas", "Al Madina")]
        + [("CENTRE_SANTE", s) for s in ("Hay 1", "Hay 2", "Hay 3")])
BEDS = {"CHU": (600, 1200), "HOPITAL_REGIONAL": (250, 500),
        "CLINIQUE": (40, 120), "CENTRE_SANTE": (10, 30)}
LABEL = {"CHU": "CHU", "HOPITAL_REGIONAL": "Hôpital Régional",
         "CLINIQUE": "Clinique", "CENTRE_SANTE": "Centre de Santé"}

rows, n = [], 0
for city, group, lat, lon in CITIES:
    for typ, suffix in PLAN:
        n += 1
        name = " ".join(x for x in (LABEL[typ], suffix, city) if x)
        rows.append({
            "facility_id": f"F-{n:03d}",
            "name": name,
            "city": city,
            "hospital_group": group,
            "type": typ,
            "beds": random.randint(*BEDS[typ]),
            "latitude": round(lat + random.uniform(-0.05, 0.05), 5),
            "longitude": round(lon + random.uniform(-0.05, 0.05), 5),
        })

path = os.path.join(HERE, "facilities.json")
with open(path, "w", encoding="utf-8") as f:          # un objet JSON par ligne (40 lignes)
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print(len(rows), "établissements écrits dans", path)