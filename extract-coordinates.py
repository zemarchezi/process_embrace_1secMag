#%%
def extract_station_coordinates(readme_path, calculate_magnetic=True, date_time=None):
    """
    Extracts station coordinates from the magnetometer readme file and calculates
    magnetic coordinates if requested.
    
    Parameters:
    ----------
    readme_path : str
        Path to the readme file containing station information.
    calculate_magnetic : bool, optional
        Whether to calculate magnetic coordinates and L-shell. Default is True.
    date_time : datetime.datetime, optional
        Date and time to use for magnetic field model calculations.
        If None, the current date and time will be used.
        
    Returns:
    -------
    dict
        Dictionary with station codes as keys and dictionaries containing:
        - 'geo_lon': Geographic longitude
        - 'geo_lat': Geographic latitude
        - 'mag_lon': Magnetic longitude (if calculate_magnetic=True)
        - 'mag_lat': Magnetic latitude (if calculate_magnetic=True)
        - 'l_shell': L-shell value (if calculate_magnetic=True)
    """
    coordinates = {}
    
    with open(readme_path, 'r') as file:
        content = file.read()
        
    # Find the section with station coordinates
    start_marker = "station coordinates:"
    if start_marker in content:
        # Extract the coordinates section
        section_start = content.index(start_marker) + len(start_marker)
        coordinates_section = content[section_start:].strip()
        
        # Skip the header line
        lines = coordinates_section.split('\n')[1:]
        
        # Process each station line
        for line in lines:
            # Split by whitespace and filter out empty strings
            parts = [part for part in line.split() if part]
            
            # Check if line has enough parts to extract coordinates and station code
            if len(parts) >= 3:
                longitude = float(parts[0])
                latitude = float(parts[1])
                station_code = parts[2]
                
                # Some stations have "deactivated" note - extract just the code
                if '(' in station_code:
                    station_code = station_code.split('(')[0].strip()
                
                # Store as a dictionary with geographic coordinates
                coordinates[station_code] = {
                    'geo_lon': longitude,
                    'geo_lat': latitude,
                    'active': '(deactivated)' not in line
                }
    
    # Calculate magnetic coordinates if requested
    if calculate_magnetic:
        try:
            import aacgmv2
            import numpy as np
            from datetime import datetime
            from apexpy import Apex
            
            # Use current date/time if not provided
            if date_time is None:
                date_time = datetime.now()
                
            for station, coords in coordinates.items():
                try:
                    # Calculate magnetic coordinates using AACGM-V2 model
                    mag_lat, mag_lon, _ = aacgmv2.convert_latlon(
                        coords['geo_lat'], 
                        coords['geo_lon'], 
                        0, 
                        date_time
                    )
                    if np.isnan(mag_lat) or np.isnan(mag_lon):
                        
                        apex24 = Apex(date=date_time) 
                        mag_lat, mag_lon = apex24.convert(coords['geo_lat'], 
                                                    coords['geo_lon'], 
                                                    'geo', 
                                                    'apex', 
                                                    height=0)
                    
                    # Calculate L-shell (dipole approximation)
                    # L ≈ 1 / cos²(λ) where λ is magnetic latitude in radians
                    mag_lat_rad = np.radians(abs(mag_lat))
                    l_shell = 1.0 / (np.cos(mag_lat_rad) ** 2)
                    
                    # Add magnetic coordinates to the station data
                    coords['mag_lat'] = float(mag_lat)
                    coords['mag_lon'] = float(mag_lon)
                    coords['l_shell'] = float(l_shell)
                    
                except Exception as e:
                    print(f"Warning: Failed to calculate magnetic coordinates for {station}: {str(e)}")
                    coords['mag_lat'] = None
                    coords['mag_lon'] = None
                    coords['l_shell'] = None
                    
        except ImportError:
            print("Warning: aacgmv2 package not found. Magnetic coordinates will not be calculated.")
            print("Install with: pip install aacgmv2")
    
    return coordinates
#%%
# Example usage
if __name__ == "__main__":
    # Replace with actual path to your readme file
    readme_path = "aux_data/readme_magnetometer_sec.txt"
    
    from datetime import datetime
    
    # With magnetic coordinates (requires aacgmv2 package)
    # Using a specific date for the calculation
    date_time = datetime(2024, 6, 21, 12, 0, 0)  # June 21, 2023 at 12:00 UTC
    station_coords = extract_station_coordinates(
        readme_path, 
        calculate_magnetic=True, 
        date_time=date_time
    )
    
    # Print the extracted coordinates
    print(f"Station Coordinates (Magnetic reference: {date_time}):")
    for station, coords in station_coords.items():
        status = "ACTIVE" if coords.get('active', True) else "DEACTIVATED"
        print(f"{station} ({status}):")
        print(f"  Geographic: Lon {coords['geo_lon']:.2f}, Lat {coords['geo_lat']:.2f}")
        
        if 'mag_lat' in coords and coords['mag_lat'] is not None:
            print(f"  Magnetic:  Lon {coords['mag_lon']:.2f}, Lat {coords['mag_lat']:.2f}")
            print(f"  L-shell:   {coords['l_shell']:.2f}")
        print()

    
    # Alternatively, without magnetic coordinates
    # station_coords = extract_station_coordinates(readme_path, calculate_magnetic=False)
    
    # Or with the current date and time
    # station_coords = extract_station_coordinates(readme_path, calculate_magnetic=True)  # uses datetime.now()
    
    # Export to CSV
    import csv
    with open('./aux_data/station_coordinates.csv', 'w', newline='') as csvfile:
        fieldnames = ['station', 'active', 'geo_lon', 'geo_lat', 'mag_lon', 'mag_lat', 'l_shell']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for station, coords in station_coords.items():
            row = {
                'station': station,
                'active': coords.get('active', True),
                'geo_lon': coords['geo_lon'], 
                'geo_lat': coords['geo_lat']
            }
            
            # Add magnetic coordinates if available
            if 'mag_lat' in coords:
                row['mag_lon'] = coords['mag_lon']
                row['mag_lat'] = coords['mag_lat']
                row['l_shell'] = coords['l_shell']
                
            writer.writerow(row)
# %%
