# %% 
import numpy as np
import pandas as pd
import os
import sys
import glob
import re
from datetime import datetime
import matplotlib.pyplot as plt
from deal_with_zip_files import *
from tqdm import tqdm
# %%

DATA_PATH = "/Users/josem/mag_data/embrace/Karen"
AUX_PATH = "/Users/josem/python_projects/embrace_gic_nn_modeling/auxdata"
OUTPUT_PATH = "/Users/josem/mag_data/embrace_1sec/processed"
#%%
def createdir(out_subpath):
    if not os.path.exists(out_subpath):
        os.makedirs(out_subpath)
        print(f"Directory '{out_subpath}' created.")
    else:
        print(f"Directory '{out_subpath}' already exists.")

def convert_to_hdz(slope, scalling, raw):
    H = (((raw[0] * slope["LS"]) + slope["LO"] + slope["H"]) / scalling["Hs"] ) + scalling["Hb"]
    D = ((((raw[1] * slope["LS"]) + slope["LO"] + slope["D"]) / scalling["Ds"]) * 1/scalling["Hb"] * 3438/60 ) + scalling["Db"]
    Z = (((raw[2] * slope["LS"]) + slope["LO"] + slope["Z"]) / scalling["Zs"])+ scalling["Zb"]

    return H, D, Z

#%%

scaling_factors = pd.read_csv(f"{AUX_PATH}/embrace_scaling_factors.csv")
scaling_factors["valid_from_date"] = pd.to_datetime(scaling_factors["valid_from_date"])
slope_factors = pd.read_csv(f"{AUX_PATH}/embrace_solpe_offset_factors.csv")
slope_factors["Valid_from"] = pd.to_datetime(slope_factors["Valid_from"])

# %%
ff = open("log.txt", "a")
for year in tqdm(range(2022, 2024)):
    files = glob.glob(f"{DATA_PATH}/**/**/{year}/")
    if len(files) > 0:
        stations = []
        for file in sorted(files):
            stations.append([wrd for wrd in file.split("\\") if wrd][-2])
    else:
        stations = []
        print(f"No files found for {year}")
        
    for doy in tqdm(range(1, 366)):
        for station in stations: 
            inner_files = glob.glob(f"{DATA_PATH}/**/{station.upper()}/{year}/{station.lower()}{doy:03d}*.zip")
            if len(inner_files) > 0:
                day_files = []
                for file_h in sorted(inner_files):
                    try:
                        filename = get_zip_file_contents(file_h)
                        data = read_text_file_from_zip(file_h, filename)
                        day = pd.to_datetime(datetime.strptime(str(year) + "-" + data["day_of_the_year"], "%Y-%j").strftime("%Y-%m-%d"))

                        maskscaling = (scaling_factors["valid_from_date"]<=day) & (scaling_factors["Station_code"] == data["stationcode"])
                        filtered_scaling = scaling_factors[maskscaling]
                        scaling_for_the_date = filtered_scaling.iloc[-1]

                        mask_slope = (slope_factors["Valid_from"]<=day) & (slope_factors["station_code"] == data["stationcode"])
                        filtered_slope = slope_factors[mask_slope]
                        slope_for_the_date = filtered_slope.iloc[-1]

                        H, D, Z = convert_to_hdz(slope_for_the_date, 
                                                scaling_for_the_date, 
                                                [data["columns"]["H(Ch2)"], 
                                                data["columns"]["D(Ch4)"], 
                                                data["columns"]["Z(Ch6)"]])
                        
                        
                        datetimes = [datetime.combine(day, 
                                    datetime.strptime(f"{h}:{m}:{s}", '%H:%M:%S').time())
                                    for h, m, s in zip(data["columns"]["HH"].astype(int), 
                                                        data["columns"]["MM"].astype(int), 
                                                        data["columns"]["SS"].astype(int))]

                        hourDataframe = pd.DataFrame({"time": datetimes, "H": H, "D": D, "Z": Z})
                        day_files.append(hourDataframe)
                        ff.write(f"{file_h} -- ok\n")
                    except Exception as e:
                        print(f"ERROR: {file_h}")
                        ff.write(f"{file_h} -- ERROR\n")

                if len(day_files) > 0:
                    daydata = pd.concat(day_files)
                    fullday = pd.DataFrame({"time": pd.date_range(start=day, periods=86400, freq='s')})
                    merged = fullday.merge(daydata, how="left", on="time")

                    out_subpath = f"{OUTPUT_PATH}/{station}/{year}"
                    createdir(out_subpath)

                    print(f"Saving file at: {out_subpath}/{station}_{day.strftime('%Y%m%d')}_s.csv")
                    merged.to_csv(f"{out_subpath}/{station}_{day.strftime('%Y%m%d')}_s.csv", index=False)
                else:
                    day = pd.to_datetime(datetime.strptime(str(year) + f"-{doy:03d}", "%Y-%j").strftime("%Y-%m-%d"))
                    fullday = pd.DataFrame({"time": pd.date_range(start=day, periods=86400, freq='s'),
                                            "H": [np.nan]*86400, "D": [np.nan]*86400, "Z": [np.nan]*86400})
                    out_subpath = f"{OUTPUT_PATH}/{station}/{year}"
                    createdir(out_subpath)

                    print(f"Saving file at: {out_subpath}/{station}_{day.strftime('%Y%m%d')}_s.csv")
                    fullday.to_csv(f"{out_subpath}/{station}_{day.strftime('%Y%m%d')}_s.csv", index=False)
            
            else:
                day = pd.to_datetime(datetime.strptime(str(year) + f"-{doy:03d}", "%Y-%j").strftime("%Y-%m-%d"))
                print(f"No files found for {station} on {day.strftime('%Y%m%d')}")


#%%

# %%
