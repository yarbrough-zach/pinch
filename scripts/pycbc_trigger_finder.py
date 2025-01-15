#! /urs/bin/env python3

import os
from gwpy.time import tconvert 

import pandas as pd
import h5py

def gps_to_yymmdd(gps_time):
        # Convert GPS time to datetime
        gps_datetime = tconvert(gps_time) 
                
        # Format the datetime object to 'yymmdd'
        return gps_datetime.strftime('%Y%m%d')

# Example usage
gps_time = 1420848018  # Replace with your GPS time
formatted_date = gps_to_yymmdd(gps_time)
print(formatted_date)
y = formatted_date[:4]
m = formatted_date[4:6]
d = formatted_date[6:]

pycbc_file_path = '/home/pycbc.live/analysis/prod/o4/full_bandwidth/cit/triggers/'

pycbc_file_path = f"{pycbc_file_path}{y}_{m}_{d}/"
print(pycbc_file_path)

pycbc_files = [file for file in os.listdir(pycbc_file_path) if file.endswith('.hdf') and file.startswith('H1L1V1')]

def extract_pycbc_data(file_path):

    def recursively_extract(group, prefix=""):
        for key, item in group.items():
            path = f"{prefix}/{key}" if prefix else key

            if isinstance(item, h5py.Dataset):
                data[path] = item[()]
            elif isinstance(item, h5py.Group):
                recursively_extract(item, path)

    with h5py.File(file_path, 'r') as h5_file:

        l1_group = h5_file['L1']

        data = {}

        recursively_extract(l1_group)
        #print(data.keys())
        
        for key in ['gates', 'loudest', 'psd']:
            if key in data.keys():
                del data[key]

        #for key in data.keys():
            #print(key, len(data[key]))
        
        #df =  pd.DataFrame(dict([(k, pd.Series(v)) for k, v in data.items()]))
   
    return data

def save_and_reset(df, file_count, save_path):
    save_file = f"{y}{m}{d}_pycbc_triggers_part_{file_count}.csv"
    df.to_csv(save_file, index=False)
    return pd.DataFrame(), file_count + 1

save_path = '/home/zach.yarbrough/TGST/observing/4/b/pycbc/triggers/'
max_rows = 1_000_000
file_count = 1

for i, file in enumerate(pycbc_files):

    file = f"{pycbc_file_path}{file}"
    print(file)

    if i == 0:
        df = pd.DataFrame(extract_pycbc_data(file))
        df = df[df['snr'] >= 4]
    else:
        try:
            current_df = pd.DataFrame(extract_pycbc_data(file))

            if current_df.empty:
                continue

            current_df = current_df[current_df['snr'] >= 4]
            df = pd.concat([df, current_df], ignore_index=True)

            if len(df) > max_rows:
                df, file_count = save_and_reset(df, file_count, save_path)

        except IsADirectoryError as e:
            print('dir error, moving on...')
            continue

    print(len(df))
    print(i/len(pycbc_files)*100, '% finished')

if not df.empty:
    save_file = f"{save_path}data_part_final.csv"
    df.to_csv(save_file, index=False)
    print(f"Saved remaining {len(df)} rows")
