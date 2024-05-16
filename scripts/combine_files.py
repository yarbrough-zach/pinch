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
for file in files:
    print(file)
    file_df = pd.read_csv(os.path.join(args.input_path, file))

    combined_df = pd.concat([combined_df, file_df], ignore_index=True)

combined_df.to_csv(os.path.join(output_path, f'gstlal_chunk{chunk}.csv'))
print('combined df saved')
