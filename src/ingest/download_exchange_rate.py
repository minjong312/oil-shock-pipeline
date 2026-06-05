import os
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta

end_date = datetime.now()
start_date = end_date - timedelta(days=14)

print(f"[*] Start collecting exchange rate data: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

df = fdr.DataReader('USD/KRW', start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))

if df.empty:
    print("[!] Warning: Failed to fetch data.")
    df = df[['Date', 'Close']]
    df.columns = ['Date', 'ExchangeRate']
    
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    partition_dir = f"./raw_data/year={year}/month={month}/day={day}"
    os.makedirs(partition_dir, exist_ok=True)

    file_path = f"{partition_dir}/exchange_rate.csv"
    df.to_csv(file_path, index=False)
    print(f"[*] Data saved successfully: {file_path}")
