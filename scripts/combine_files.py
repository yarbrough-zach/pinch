#!/usr/bin/env python3
import os
import argparse
import pandas as pd
parser = argparse.ArgumentParser()
parser.add_argument('--input-path', type=str)
parser.add_argument('--output-path', type=str)
args = parser.parse_args()

if not args.output_path:
    output_path = args.input_path
else:
    output_path = args.output_path

files = os.listdir(args.input_path)

if args.input_path.endswith('/'):
    chunk = str(args.input_path.split('/')[-2].split('k')[-1])
else:
    chunk = str(args.input_path.split('/')[-1].split('k')[-1])

combined_df = pd.DataFrame()

chunk_size = 20000

output_file = os.path.join(args.output_path, f'pycbc_chunk{chunk}.csv')

first_file = True

for file in files:
    print(file)
    file_path = os.path.join(args.input_path, file)

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        if first_file:
            chunk.to_csv(output_file, mode='w', header=True, index=False)
            first_file = False
        else:
            chunk.to_csv(output_file, mode='a', header=False, index=False)

    #file_df = pd.read_csv(os.path.join(args.input_path, file))

    #combined_df = pd.concat([combined_df, file_df], ignore_index=True)

#combined_df.to_csv(os.path.join(output_path, f'gstlal_chunk{chunk}.csv'))
print('combined df saved')
