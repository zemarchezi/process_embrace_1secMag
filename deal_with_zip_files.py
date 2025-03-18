#%%
import numpy as np
import zipfile
import os
from io import BytesIO
#%%
def unzip_files(zip_path, extract_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
#%%
def get_zip_file_contents(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        if len(file_list) > 0:
            return file_list[0]
        elif len(file_list) > 1:
            print("Warning: more than one file in zip file, returning first one")
            return file_list[0]
        else:
            print("Error: no files in zip file")
            return ""
#%%
def read_text_file_from_zip(zip_path, file_name):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        with zip_ref.open(file_name) as file:
            content = file.read()
            # Assuming the text file is encoded in UTF-8
            content_str = content.decode('utf-8')

            equip_numb = content_str.split("\r\n")[0].split(" <")[0].split(" ")[-1]
            station_name = " ".join(content_str.split("\r\n")[0].split(" <")[0].split(" ")[:-1])
            stationcode = file_name[:3]
            day_of_the_year = content_str.split("\r\n")[0].split("<")[-1].split(">")[0]
            columns_header = [word for word in content_str.split("\r\n")[2].split(" ") if word]

            # Use numpy's loadtxt to load data from a string
            data = np.loadtxt(BytesIO(content), skiprows=5)

    out_dict = {"equip_numb" : equip_numb,
                "station_name" : station_name,
                "stationcode" : stationcode,
                "day_of_the_year" : day_of_the_year,
                "columns" : {
                            columns_header[0]: data[:,0],
                            columns_header[1]: data[:,1],
                            columns_header[2]: data[:,2],
                            columns_header[3]: data[:,3],
                            columns_header[4]: data[:,4],
                            columns_header[5]: data[:,5],
                            columns_header[6]: data[:,6],
                            columns_header[7]: data[:,7]             
                           }
                }   

    return out_dict


# %%
