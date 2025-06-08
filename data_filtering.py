import numpy as np 
import pandas as pd
import os

def combine_options_data(options_data, stock_data):
    options_data['mid_price'] = (options_data['bid'] + options_data['ask'])/2
    combined_df = options_data.merge(stock_data, 
                                     left_on = "underlying", right_on = "symbol",
                                     suffixes=('', '_stock'))
    combined_df = combined_df.drop(columns=['symbol'])
    return combined_df

def combine_new_data(date):
    options = pd.read_csv(f"/home/steve/Downloads/CFRM_521/ProjectData/2013-02/{date}options.csv")
    stocks = pd.read_csv(f"/home/steve/Downloads/CFRM_521/ProjectData/2013-02/{date}stocks.csv")
    return combine_options_data(options, stocks)
        
url = "https://web.archive.org/web/20130201003232/https://en.wikipedia.org/wiki/S%26P_100"
sp100_comp = pd.read_html(url)
sp100_comp = sp100_comp[2]["Symbol"]
sp100_comp = sp100_comp.unique()

dir = "/home/steve/Downloads/CFRM_521/ProjectData/2013-02/"
dates = []
for file in os.listdir(dir):
    #print(file)
    if file.endswith(".csv"):
        if "options" in file:
            dates.append(file.split("options")[0])
    
dates = sorted(set(dates))
for date in dates:
    print(f"Processing: {date}")
    combined_df = combine_new_data(date)
    combined_df['underlying'] = (
    combined_df['underlying']
    .str.replace('.', '-', regex=False)
    .str.upper()
    .replace({'GOOGL': 'GOOG'})
    )
    sp100_comp = [sym.replace('.', '-').upper() for sym in sp100_comp]
    filt_df = combined_df[combined_df['underlying'].isin(sp100_comp)]
    num_stocks = filt_df['underlying'].unique()
    if len(num_stocks) != len(sp100_comp):
        print(f"Error size mismatch, Filtered Df Size: {len(num_stocks)}, SP100 Size: {len(sp100_comp)}")
        missing = set(sp100_comp) - set(filt_df['underlying'].unique())
        print(f"Missing tickers: {sorted(missing)}")
    else:
        filt_df.to_csv(f"/home/steve/Downloads/CFRM_521/ProjectData/filtered/{date}.csv", index = False)

