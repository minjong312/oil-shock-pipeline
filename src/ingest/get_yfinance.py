import requests
import pandas as pd
import os
import sys

api_key = "6c1575680264e52c01b11e9c2269f9ea"
start_date = "2015-01-01"

local_dir = "/tmp/fx_data"
hdfs_dir = "/user/maria_dev/fx_project/raw"

if not os.path.exists(local_dir):
    os.makedirs(local_dir)

print("Start downloading data using FRED API...")

def get_fred_data(series_id, key, start):
    print("Downloading " + series_id + "...")
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": key,
        "file_type": "json",
        "observation_start": start
    }
    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("Error: API request failed for " + series_id)
        sys.exit(1)

    data = response.json()
    obs = data.get("observations", [])

    dates = []
    values = []

    for row in obs:
        val = row.get("value")
        if val != ".":
            dates.append(row.get("date"))
            values.append(float(val))

    df = pd.DataFrame({"Date": dates, series_id: values})
    df.set_index("Date", inplace=True)
    return df

usdkrw = get_fred_data("DEXKOUS", api_key, start_date)
usdkrw.columns = ["USDKRW"]

wti = get_fred_data("DCOILWTICO", api_key, start_date)
wti.columns = ["WTI"]

dxy = get_fred_data("DTWEXBGS", api_key, start_date)
dxy.columns = ["DXY"]

print("Merging data...")

merged_data = pd.concat([usdkrw, wti, dxy], axis=1)
merged_data = merged_data.ffill()
merged_data = merged_data.dropna()

if merged_data.empty:
    print("Error: Merged data is empty.")
    sys.exit(1)

local_file = local_dir + "/yfinance_data.csv"
merged_data.to_csv(local_file, index_label="Date")
print("Saved to local: " + local_file)

print("Uploading to HDFS...")
os.system("hdfs dfs -mkdir -p " + hdfs_dir)
os.system("hdfs dfs -put -f " + local_file + " " + hdfs_dir + "/yfinance_data.csv")

print("Done!")
