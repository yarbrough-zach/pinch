import sqlite3
import numpy as np
import pandas as pd
import warnings
from os import listdir, system, environ
import os

# Ignoring some pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
environ["GSTLAL_FIR_WHITEN"] = '0'

# The current server I'm running this on
server = "ICS"

glitches_file = '/home/andre.guimaraes/public_html/gstlal/offline_analysis/background_investigation_gstlal_02/O3glitches.csv'

# File Locations depending on current server
filePathsServers = {
    "CIT":[
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk22_1253318003_1254009618/H1L1V1-ALL_LLOID-1253318003-691615.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk16_1249243172_1249938613/H1L1V1-ALL_LLOID-1249243172-695441.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk14_1247739088_1248529580/H1L1V1-ALL_LLOID-1247739088-790492.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk15_1248529580_1249243172/H1L1V1-ALL_LLOID-1248529580-713592.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk21_1252651569_1253318003/H1L1V1-ALL_LLOID-1252651569-666434.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk17_1249938613_1250674269/H1L1V1-ALL_LLOID-1249938613-735656.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk19_1251349203_1252015022/H1L1V1-ALL_LLOID-1251349203-665819.sqlite',
        '/home/gstlalcbc.offline/observing/3/b/C00/chunk24_1257388107_1258128955/H1L1V1-ALL_LLOID-1257388107-740848.sqlite',
        '/home/gstlalcbc.offline/observing/3/b/C00/chunk26_1258755063_1259423400/H1L1V1-ALL_LLOID-1258755063-668337.sqlite',
        '/home/gstlalcbc.offline/observing/3/b/C00/chunk34_1264528208_1265133147/H1L1V1-ALL_LLOID-1264528208-604939.sqlite'
    ],
    "LHO":[
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk13_1246824367_1247739088/H1L1V1-ALL_LLOID-1246824367-914721.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk18_1250674269_1251349203/H1L1V1-ALL_LLOID-1250674269-674934.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk20_1252015022_1252651569/H1L1V1-ALL_LLOID-1252015022-636547.sqlite',
        '/home/gstlalcbc.offline/observing/3/a/C00/chunk21_1252651569_1253318003/H1L1V1-ALL_LLOID-1252651569-666434.sqlite',
        '/home/gstlalcbc.offline/observing/3/b/C00/chunk25_1258128955_1258755063/H1L1V1-ALL_LLOID-1258128955-626108.sqlite',
        '/home/gstlalcbc.offline/observing/3/b/C00/chunk33_1263751886_1264528208/H1L1V1-ALL_LLOID-1263751886-776322.sqlite'
    ],
    "ICS":[
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk23_1256655642_1257388107/H1L1V1-ALL_LLOID-1256655642-732465.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk27_1259423400_1260081799/H1L1V1-ALL_LLOID-1259423400-658399.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk29_1260840862_1261582830/H1L1V1-ALL_LLOID-1260840862-741968.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk30_1261582830_1262192988/H1L1V1-ALL_LLOID-1261582830-610158.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk32_1262946475_1263751886/H1L1V1-ALL_LLOID-1262946475-805411.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk35_1265133147_1265747429/H1L1V1-ALL_LLOID-1265133147-614282.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk36_1265747429_1266463048/H1L1V1-ALL_LLOID-1265747429-715619.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk37_1266463048_1267145365/H1L1V1-ALL_LLOID-1266463048-682317.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk39_1267758251_1268420278/H1L1V1-ALL_LLOID-1267758251-662027.sqlite',
        '/ligo/home/ligo.org/gstlalcbc/observing/3/b/C00/chunk40_1268420278_1269561618/H1L1V1-ALL_LLOID-1268420278-1141340.sqlite'
    ]
}

files = sorted(filePathsServers[server])
for file in files:
    outFile = "TriggerFiles/" + file.split('/')[-2].split('_')[0] + "_triggers.csv"
    print("Reading from file: " + file)
    con = sqlite3.connect(file)
    cur = con.cursor()
    print("Reading Coinc Event Table")
    table = 'coinc_event'
    cur = con.execute('select * from ' + table)
    names = list(map(lambda x: x[0], cur.description))
    df_coinc = {name:[] for name in names}
    for row in cur.execute('SELECT * FROM '  + table + ";"):
        for i in range(len(names)):
            df_coinc[names[i]] += [row[i]]
    df_coinc = pd.DataFrame(df_coinc)


    print("Reading Coinc Event Map Table")
    table = 'coinc_event_map'
    cur = con.execute('select * from ' + table)
    names = list(map(lambda x: x[0], cur.description))
    df_coinc_map = {name:[] for name in names}

    for row in cur.execute('SELECT * FROM '  + table + ";"):
        for i in range(len(names)):
            df_coinc_map[names[i]] += [row[i]]
    df_coinc_map = pd.DataFrame(df_coinc_map)

    print("Reading Single Inspiral Table")
    table = 'sngl_inspiral'
    cur = con.execute('select * from ' + table)
    names = list(map(lambda x: x[0], cur.description))
    df_sngl_inspiral = {name:[] for name in names}

    for row in cur.execute('SELECT * FROM '  + table + ";"):
        for i in range(len(names)):
            df_sngl_inspiral[names[i]] += [row[i]]
    df_sngl_inspiral = pd.DataFrame(df_sngl_inspiral)

    print("Assigning Likelihood to Single Inspiral Events")
    df_sngl_inspiral = df_sngl_inspiral.merge(df_coinc_map[['event_id','coinc_event_id']], on='event_id', how='left').merge(df_coinc[['coinc_event_id','likelihood']],on='coinc_event_id', how='left')
    print("Saving to File")
    df_sngl_inspiral.to_csv(outFile)