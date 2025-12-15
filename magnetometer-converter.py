#%% 131 day
import os
import glob
import re
import zipfile
import io
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import math

# Function to load station coordinates from CSV file
def load_station_coordinates(station_csv_path):
    """
    Load station coordinates from a CSV file.
    
    The CSV should have columns: station, active, geo_lon, geo_lat, mag_lon, mag_lat, l_shell
    
    Returns a dictionary with station code as key and coordinate information as value.
    """
    try:
        # Read the CSV file
        stations_df = pd.read_csv(station_csv_path)
        
        # Initialize the coordinates dictionary
        coordinates = {}
        
        # Get station mappings from the CSV
        for _, row in stations_df.iterrows():
            station_code = row['station'].lower()
            
            # Create a dictionary for each station
            coordinates[station_code] = {
                'longitude': row['geo_lon'],
                'latitude': row['geo_lat'],
                'mag_longitude': row['mag_lon'],
                'mag_latitude': row['mag_lat'],
                'l_shell': row['l_shell'],
                'name': station_code.upper(),  # Default name is just the station code
                'deactivated': not row['active']
            }
        
        print(f"Loaded coordinates for {len(coordinates)} stations")
        return coordinates
    except Exception as e:
        print(f"Error loading station coordinates: {str(e)}")
        # Provide a minimal fallback dictionary in case of error
        return {}

# Function to get contents of the zip file
def get_zip_file_contents(zip_path, year):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        if not file_list:
            raise ValueError(f"No files found in ZIP: {zip_path}")
        else:
            correct_year_file = [ff for ff in file_list if f'{str(year)[-2:]}S' in ff or f'{str(year)[-2:]}s' in ff]
        if len(correct_year_file) > 0:
            return correct_year_file[0]
        else:
            raise ValueError(f"No file for year {year} found in ZIP: {zip_path}")

# Function to extract and read the file content from a ZIP archive
def read_text_file_from_zip(zip_path, filename):
    station_code = os.path.basename(zip_path)[0:3].lower()
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Extract file content
        with zip_ref.open(filename) as file:
            content = file.read().decode('utf-8', errors='ignore')
            
            # Parse the header and data
            lines = content.strip().split('\n')
            
            # Skip header lines (usually 5 lines)
            header_lines = []
            data_start_line = 0
            for i, line in enumerate(lines):
                if 'HH MM SS' in line:
                    data_start_line = i + 1
                    break
                header_lines.append(line)
                
            # Extract data lines
            data_lines = lines[data_start_line:]
            
            # Prepare data columns
            hh, mm, ss, h_ch2, d_ch4, z_ch6 = [], [], [], [], [], []
            t1_ch7, t2_ch8 = [], []
            
            # Parse the data into columns
            for line in data_lines:
                if not line.strip():
                    continue
                    
                parts = line.strip().split()
                if len(parts) < 6:  # Need at least HH, MM, SS, H, D, Z
                    continue
                    
                try:
                    hh.append(int(parts[0]))
                    mm.append(int(parts[1]))
                    ss.append(int(parts[2]))
                    h_ch2.append(float(parts[3]))
                    d_ch4.append(float(parts[4]))
                    z_ch6.append(float(parts[5]))
                    
                    # Add temperature channels if available
                    if len(parts) > 6:
                        t1_ch7.append(float(parts[6]))
                    else:
                        t1_ch7.append(np.nan)
                        
                    if len(parts) > 7:
                        t2_ch8.append(float(parts[7]))
                    else:
                        t2_ch8.append(np.nan)
                        
                except (ValueError, IndexError) as e:
                    print(f"Error parsing line: {line} - {e}")
                    continue
            
            # Extract metadata from filename
            filename_pattern = fr"({station_code}\d{{3}}\d{{2}})\.\d{{2}}s"
            match = re.search(filename_pattern, filename, re.IGNORECASE)
            if not match:
                raise ValueError(f"Invalid filename format: {filename}")
                
            base_name = match.group(1)
            doy_str = base_name[3:6]
            hour_str = base_name[6:8]
            year_str = '20' + filename.split('.')[-1][:2]  # Assuming 21st century
            
            # Create columns dictionary like in the original code
            columns = {
                'HH': np.array(hh),
                'MM': np.array(mm),
                'SS': np.array(ss),
                'H(Ch2)': np.array(h_ch2),
                'D(Ch4)': np.array(d_ch4),
                'Z(Ch6)': np.array(z_ch6),
                'T1(Ch7)': np.array(t1_ch7),
                'T2(Ch8)': np.array(t2_ch8)
            }
            
            return {
                'stationcode': station_code,
                'year': int(year_str),
                'day_of_the_year': doy_str,
                'hour': int(hour_str),
                'columns': columns,
                'header': header_lines
            }

# Function to convert raw values to HDZ using your existing method
def convert_to_hdz(slope, scaling, raw):
    H = (((raw[0] * slope["LS"]) + slope["LO"] + slope["H"]) / scaling["Hs"]) + scaling["Hb"]
    D = ((((raw[1] * slope["LS"]) + slope["LO"] + slope["D"]) / scaling["Ds"]) * 1/scaling["Hb"] * 3438/60) + scaling["Db"]
    Z = (((raw[2] * slope["LS"]) + slope["LO"] + slope["Z"]) / scaling["Zs"]) + scaling["Zb"]
    
    return H, D, Z

# Function to create IAGA-2002 header
def create_iaga_header(station_code, date, station_info=None):
    if station_info is None:
        station_info = STATION_COORDINATES.get(station_code.lower(), {
            'longitude': 0.0,
            'latitude': 0.0,
            'name': f'{station_code.upper()} Station'
        })
    
    # Convert longitude to 0-360 format if needed
    lon360 = station_info['longitude'] + 360 if station_info['longitude'] < 0 else station_info['longitude']
    
    sloLat = 50 - len(f"{station_info['latitude']:.2f}")
    sloLon = 50 - len(f"{lon360:.2f}")
    slomLat = 50 - len(f"{station_info['mag_latitude']:.2f}")
    slomLon = 50 - len(f"{station_info['mag_longitude']:.2f}")
    slomLshel = 50 - len(f"{station_info['l_shell']:.2f}")

    # Format the station name with padding
    station_name = f"{station_info['name']}"
    
    header = f" Format                 IAGA-2002                                         |\n"
    header += f" Source of Data         EMBRACE/INPE                                      |\n"
    header += f" Station Name           {station_name:<50}|\n"
    header += f" IAGA CODE              {station_code.upper():<50}|\n"
    header += f" Geodetic Latitude      {station_info['latitude']:.2f}{' '*sloLat}|\n"
    header += f" Geodetic Longitude     {lon360:.2f}{' '*sloLon}|\n"
    header += f" Magnetic Latitude      {station_info['mag_latitude']:.2f}{' '*slomLat}|\n"
    header += f" Magnetic Longitude     {station_info['mag_longitude']:.2f}{' '*slomLon}|\n"
    header += f" L-shel                 {station_info['l_shell']:.2f}{' '*slomLshel}|\n"
    header += f" Elevation              0018                                              |\n"
    header += f" Reported               HDZF                                              |\n"
    header += f" Sensor Orientation     HDZ                                               |\n"
    header += f" Digital Sampling       1 second                                          |\n"
    header += f" Data Interval Type     1-Second (instantaneous)                          |\n"
    header += f" Data Type              provisional                                       |\n"
    header += f" # This data file was constructed by Embrace/INPE.                        |\n"
    header += f" # F is derived from the recorded spot values                             |\n"
    header += f" # and should be applied for quality check only.                          |\n"
    header += f" # DOI citation: https://doi.org/10.1002/2017RS006477                     |\n"
    header += f" # Final data will be available on the online.                            |\n"
    header += f" # Go to http://www.inpe.br/spaceweather for details on obtaining         |\n"
    header += f" # this product.                                                          |\n"
    header += f"DATE       TIME         DOY     {station_code.upper()}H           {station_code.upper()}D          {station_code.upper()}Z          {station_code.upper()}F     |"
    
    return header

# Main function to convert magnetometer data to IAGA-2002 format 
def magnetometer_to_iaga(data_path, aux_path, output_path, station_coordinates, year0, year1):
    # Load scaling and slope factors
    scaling_factors = pd.read_csv(f"{aux_path}/embrace_scaling_factors.csv")
    scaling_factors["valid_from_date"] = pd.to_datetime(scaling_factors["valid_from_date"])
    
    slope_factors = pd.read_csv(f"{aux_path}/embrace_solpe_offset_factors.csv")
    slope_factors["Valid_from"] = pd.to_datetime(slope_factors["Valid_from"])
    
    # Create log file
    log_file = open("iaga_conversion_logs.txt", "w")
    
    # Process years
    for year in range(year0, year1):
        # Find all station directories for this year
        station_dirs = []
        for root, dirs, files in os.walk(f"{data_path}/{year}"):
            for dir_name in dirs:
                year_path = os.path.join(root, dir_name)
                if os.path.exists(year_path):
                    station_dirs.append((dir_name.lower(), year_path))
        
        if not station_dirs:
            print(f"No stations found for year {year}")
            continue
            
        for station_code, station_year_path in station_dirs:
            # Process each day of year
            # for doy in range(1, 367):
            for doy in range(130, 138):
                # Find zip files for this day
                zip_files = glob.glob(f"{station_year_path}/{station_code}{doy:03d}*.zip")
                
                if not zip_files:
                    continue
                    
                # Create output directory if it doesn't exist
                station_output_dir = f"{output_path}/{station_code}/{year}"
                os.makedirs(station_output_dir, exist_ok=True)
                
                # Determine the date from the DOY
                day = datetime.strptime(f"{year}-{doy:03d}", "%Y-%j")
                date_str = day.strftime("%Y%m%d")
                
                # Process all hours for this day
                day_data = []
                
                for zip_file in sorted(zip_files):
                    try:
                        # Get filename inside zip
                        zip_contents = get_zip_file_contents(zip_file, year)
                        
                        # Read data from zip file
                        data = read_text_file_from_zip(zip_file, zip_contents)
                        
                        # Get scaling factors for this date
                        day_datetime = pd.to_datetime(datetime.strptime(f"{year}-{doy:03d}", "%Y-%j"))
                        
                        # Filter scaling factors for this station and date
                        mask_scaling = (scaling_factors["valid_from_date"] <= day_datetime) & \
                                       (scaling_factors["Station_code"] == data["stationcode"].upper())
                        filtered_scaling = scaling_factors[mask_scaling]
                        if filtered_scaling.empty:
                            print(f"No scaling factors found for {station_code} on {date_str}")
                            continue
                        scaling_for_date = filtered_scaling.iloc[-1]
                        
                        # Filter slope factors for this station and date
                        mask_slope = (slope_factors["Valid_from"] <= day_datetime) & \
                                    (slope_factors["station_code"] == data["stationcode"].upper())
                        filtered_slope = slope_factors[mask_slope]
                        if filtered_slope.empty:
                            print(f"No slope factors found for {station_code} on {date_str}")
                            continue
                        slope_for_date = filtered_slope.iloc[-1]
                        
                        # Convert raw values to HDZ
                        H, D, Z = convert_to_hdz(
                            slope_for_date, 
                            scaling_for_date, 
                            [data["columns"]["H(Ch2)"], 
                             data["columns"]["D(Ch4)"], 
                             data["columns"]["Z(Ch6)"]]
                        )
                        
                        # Calculate F (total field)
                        F = np.sqrt(H**2 + (H * np.tan(np.radians(D/60)))**2 + Z**2)
                        
                        # Create timestamps
                        datetimes = [datetime.combine(
                            day_datetime, 
                            datetime.strptime(f"{h}:{m}:{s}", '%H:%M:%S').time()
                        ) for h, m, s in zip(
                            data["columns"]["HH"].astype(int), 
                            data["columns"]["MM"].astype(int), 
                            data["columns"]["SS"].astype(int)
                        )]
                        
                        # Create hourly dataframe
                        hour_df = pd.DataFrame({
                            "time": datetimes,
                            "H": H,
                            "D": D,
                            "Z": Z,
                            "F": F,
                            "H(Ch)": data["columns"]["H(Ch2)"],
                            "D(Ch)": data["columns"]["D(Ch4)"],
                            "Z(Ch)": data["columns"]["Z(Ch6)"]
                        })
                        
                        day_data.append(hour_df)
                        log_file.write(f"{zip_file} -- OK\n")
                        
                    except Exception as e:
                        print(f"ERROR: {zip_file} - {str(e)}")
                        log_file.write(f"{zip_file} -- ERROR: {str(e)}\n")
                
                # Process all hours for this day
                if day_data:
                    # Concatenate all hourly data for this day
                    day_df = pd.concat(day_data).sort_values("time")
                    
                    # Create full day dataframe with all seconds
                    full_day_df = pd.DataFrame({
                        "time": pd.date_range(start=day_datetime, periods=86400, freq='s')
                    })
                    
                    # Merge the data (handles missing values)
                    merged_df = full_day_df.merge(day_df, how="left", on="time")
                    
                    # Create IAGA-2002 file
                    iaga_file_path = f"{station_output_dir}/{station_code.lower()}{date_str}psec.sec"
                    
                    with open(iaga_file_path, 'w') as iaga_file:
                        # Write IAGA-2002 header
                        station_info = STATION_COORDINATES.get(station_code)
                        header = create_iaga_header(station_code, day_datetime, station_info)
                        iaga_file.write(header + '\n')
                        
                        # Write data rows
                        for idx, row in merged_df.iterrows():
                            timestamp = row['time'].strftime('%Y-%m-%d %H:%M:%S.000')
                            doy_str = row['time'].strftime('%-j')  # Day of year without leading zeros
                            
                            # Format values with 6 decimal places
                            h_val = f"{row['H']:.6f}" if not pd.isna(row['H']) else "99999.999999"
                            d_val = f"{row['D']:.6f}" if not pd.isna(row['D']) else "99999.999999"
                            z_val = f"{row['Z']:.6f}" if not pd.isna(row['Z']) else "99999.999999"
                            f_val = f"{row['F']:.6f}" if not pd.isna(row['F']) else "99999.999999"
                            
                            # Ensure values are properly aligned (right-justified in their fields)
                            iaga_file.write(f"{timestamp} {doy_str:<6}{h_val:>12} {d_val:>14} {z_val:>14} {f_val:>14}\n")
                    
                    print(f"Created IAGA-2002 file: {iaga_file_path}")
                else:
                    print(f"No data for {station_code} on {date_str}")
    
    log_file.close()
    print("Conversion completed! Check 'iaga_conversion_log.txt' for details.")

#%%
# Define your paths here
DATA_PATH = "/data/mag_data/embrace/second_raw"  # Replace with your actual data path
AUX_PATH = "./aux_data"      # Replace with your actual auxiliary data path
OUTPUT_PATH = "/data/mag_data/embrace/second" # Replace with your desired output path
#%%
# Run the main function if script is executed directly
if __name__ == "__main__":
    print(f"Starting conversion using:")
    print(f"DATA_PATH: {DATA_PATH}")
    print(f"AUX_PATH: {AUX_PATH}")
    print(f"OUTPUT_PATH: {OUTPUT_PATH}")
    
    # Load station coordinates from CSV
    STATION_COORDINATES = load_station_coordinates(f"{AUX_PATH}/station_coordinates.csv")
    
    # Run the conversion
    magnetometer_to_iaga(DATA_PATH, AUX_PATH, OUTPUT_PATH, STATION_COORDINATES, 2024, 2025)
#%%
STATION_COORDINATES = load_station_coordinates(f"{AUX_PATH}/station_coordinates.csv")
data_path = DATA_PATH
aux_path = AUX_PATH
output_path = OUTPUT_PATH
station_coordinates = STATION_COORDINATES
year0 = 2022
year1 = 2025
# %%
