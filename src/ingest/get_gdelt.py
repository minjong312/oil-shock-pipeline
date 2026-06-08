import pandas as pd
import os
import sys
import datetime

local_dir = "/tmp/fx_data"
hdfs_raw_dir = "/user/maria_dev/fx_project/raw"
hdfs_gdelt_dir = hdfs_raw_dir + "/gdelt"
yfinance_local = local_dir + "/yfinance_data.csv"

print("Step 1: Finding Top 15 USD/KRW spike days...")

if not os.path.exists(yfinance_local):
    print("Fetching yfinance data from HDFS...")
    os.system("hdfs dfs -get " + hdfs_raw_dir + "/yfinance_data.csv " + yfinance_local)

if not os.path.exists(yfinance_local):
    print("Error: Cannot find yfinance_data.csv")
    sys.exit(1)

df_fx = pd.read_csv(yfinance_local)
df_fx['Date'] = pd.to_datetime(df_fx['Date'])
df_fx = df_fx.sort_values('Date')

df_fx['Change'] = df_fx['USDKRW'].pct_change() * 100
top_15 = df_fx.nlargest(15, 'Change')
target_dates = top_15['Date'].dt.date.tolist()

print("Top 15 spike dates identified.")

print("Step 2: Calculating required GDELT dates (T-5 to T0)...")
dates_to_download = set()

for t_date in target_dates:
    for i in range(6):
        d = t_date - datetime.timedelta(days=i)
        dates_to_download.add(d)
today_date = datetime.date.today()

sorted_dates = sorted(list(dates_to_download))
print("Total unique dates to download: " + str(len(sorted_dates)))

print("Step 3: Downloading and uploading raw GDELT files...")
os.system("hdfs dfs -mkdir -p " + hdfs_gdelt_dir)

for d in sorted_dates:
    date_str = d.strftime("%Y%m%d")
    url = "http://data.gdeltproject.org/events/" + date_str + ".export.CSV.zip"
    zip_path = local_dir + "/" + date_str + ".zip"
    csv_name = date_str + ".export.CSV"
    csv_path = local_dir + "/" + csv_name

    print("Processing: " + date_str)
    cmd_dl = "wget -q -O " + zip_path + " " + url
    os.system(cmd_dl)

    if os.path.exists(zip_path) and os.path.getsize(zip_path) > 0:
        os.system("unzip -q -o " + zip_path + " -d " + local_dir)

        if os.path.exists(csv_path):
            os.system("hdfs dfs -put -f " + csv_path + " " + hdfs_gdelt_dir + "/" + csv_name)
            os.system("rm -f " + csv_path)
        
        os.system("rm -f " + zip_path)
    else:
        print("Failed to download: " + date_str)
        os.system("rm -f " + zip_path)

print("Done! Phase 2.2 Complete.")
