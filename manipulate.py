# preprocess_bird_migration.py
import pandas as pd
import json

# 1. load
df = pd.read_csv("bird_migration_dataset.csv")

# 2. keep only rows with valid coords
df = df.dropna(subset=["Start_Latitude", "Start_Longitude", "End_Latitude", "End_Longitude"])

# ensure numeric
for c in ["Start_Latitude","Start_Longitude","End_Latitude","End_Longitude","Observation_Counts"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["Start_Latitude","Start_Longitude","End_Latitude","End_Longitude","Observation_Counts"])

# 3. optional: reduce dataset size if needed (uncomment if repo/files would be too big)
# df = df.sample(n=2000, random_state=1)

# 4. create aggregated start points (round coordinates to reduce jitter)
df["lat_round"] = df["Start_Latitude"].round(2)   # adjust rounding for coarse/fine aggregation
df["lon_round"] = df["Start_Longitude"].round(2)

start_grp = (df.groupby(["lat_round","lon_round","Species"], as_index=False)
               .agg(Observation_Counts=("Observation_Counts","sum"),
                    Sightings=("Bird_ID","count")))
# rename columns to easy-to-use names
start_grp = start_grp.rename(columns={"lat_round":"Latitude","lon_round":"Longitude"})

# 5. save aggregated CSV
start_grp[["Latitude","Longitude","Species","Observation_Counts","Sightings"]].to_csv("bird_migration_start_agg.csv", index=False)
print("Saved bird_migration_start_agg.csv:", start_grp.shape)

# 6. create route GeoJSON (one feature per original/aggregated route)
# Option A: create feature per record (may be large)
features = []
for _, r in df.iterrows():
    # build LineString coordinates as [lon, lat] pairs
    coords = [
        [float(r["Start_Longitude"]), float(r["Start_Latitude"])],
        [float(r["End_Longitude"]), float(r["End_Latitude"])]
    ]
    prop = {
        "Species": str(r["Species"]),
        "Observation_Counts": float(r["Observation_Counts"]),
        "Bird_ID": str(r["Bird_ID"])
    }
    features.append({"type":"Feature", "properties":prop, "geometry":{"type":"LineString","coordinates":coords}})

geo = {"type":"FeatureCollection", "features":features}
with open("routes.geojson","w") as f:
    json.dump(geo, f)
print("Saved routes.geojson with features:", len(features))

# 7. Optional: to reduce size, aggregate routes (start, end, species) and count
# (not shown here — but you can group by rounded start+end coords + species and create 1 feature per group)
