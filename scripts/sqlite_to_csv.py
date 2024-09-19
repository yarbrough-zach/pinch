#! /usr/bin/env python3

import sqlite3
import csv
import glob
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input-path', type=str)
parser.add_argument('--output-path', type=str)
args = parser.parse_args()

print(1)
def export_to_csv(db_file, table_name, csv_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)

        # header
        writer.writerow([description[0] for description in cursor.description])

        # data
        writer.writerows(rows)

    conn.close()
print(2)
#input_path = '/ligo/home/ligo.org/gstlalcbc/observing/3/final/b/runs/chunk33_1263751886_1264528208'
#output_path = '/ligo/home/ligo.org/zach.yarbrough/TGST/observing/3/b/gstlal_triggers/chunk34/'

sqlites = glob.glob(f"{args.input_path}/*LLOID_CHUNK*.sqlite")
print(sqlites)
print(3)
for file in sqlites:
    save_tag = file.split('/')[-1].split('.')[0]
    print(file)
    export_to_csv(file, 'sngl_inspiral', f"{args.output_path}{save_tag}.csv")
    print(f"Saved {save_tag}")
