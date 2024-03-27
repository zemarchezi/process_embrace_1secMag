import numpy as np
import pandas as pd


def get_slope_offset_factors(aux_path, day, station_code):
    # The inputs are the path to the aux data, the day and the station code
    # The outputs are the slope and scalling factors for the date and station code

    scaling_factors = pd.read_csv(f"{aux_path}/embrace_scaling_factors.csv")
    scaling_factors["valid_from_date"] = pd.to_datetime(scaling_factors["valid_from_date"])
    slope_factors = pd.read_csv(f"{aux_path}/embrace_solpe_offset_factors.csv")
    slope_factors["Valid_from"] = pd.to_datetime(slope_factors["Valid_from"])

    maskscaling = (scaling_factors["valid_from_date"]<=day) & (scaling_factors["Station_code"] == station_code)
    filtered_scaling = scaling_factors[maskscaling]
    scaling_for_the_date = filtered_scaling.iloc[-1]

    mask_slope = (slope_factors["Valid_from"]<=day) & (slope_factors["station_code"] == station_code)
    filtered_slope = slope_factors[mask_slope]
    slope_for_the_date = filtered_slope.iloc[-1]

    return slope_for_the_date, scaling_for_the_date

def convert_to_hdz(slope, scalling, raw):
    # convert to H, D, Z
    # The inputs are the slope and scalling factors and the raw data
    # The raw data is a list with the following order: Hch, Dch, Zch
    # The output is a tuple with the following order: H, D, Z

    H = (((raw[0] * slope["LS"]) + slope["LO"] + slope["H"]) / scalling["Hs"] ) + scalling["Hb"]
    D = ((((raw[1] * slope["LS"]) + slope["LO"] + slope["D"]) / scalling["Ds"]) * 1/scalling["Hb"] * 3438/60 ) + scalling["Db"]
    Z = (((raw[2] * slope["LS"]) + slope["LO"] + slope["Z"]) / scalling["Zs"])+ scalling["Zb"]

    return H, D, Z