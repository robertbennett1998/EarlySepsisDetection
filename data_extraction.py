# Following guidance on https://mimic.physionet.org/tutorials/intro-to-mimic-iii/
import pandas as pd
import psycopg2 as psql
import os
from pathlib import Path

chunk_size = 50000

def RunQuery(sql):
    db_user = 'rob'
    db_name = 'mimic'
    db_host = '/var/run/postgresql'
    db = psql.connect(dbname=db_name, host=db_host, user=db_user)
    cursor = db.cursor()

    query = None
    if (os.path.exists(sql) and Path(sql).is_file):
        query = open(sql, "r")
        query = query.read()
    else:
        query = sql
    print("Running query:\n", query, sep='')
    return pd.read_sql_query(query, db, chunksize=chunk_size)

def ExtractSepsisLabEvents(outpath):
    i = 0
    new_outpath = outpath
    while (os.path.exists(new_outpath)):
        i += 1
        new_outpath = outpath + ".bak." + str(i)

    if not outpath == new_outpath:
        os.rename(outpath, new_outpath)
        print("Output file already exsists (%s) renaming it to %s" % (outpath, new_outpath))
        outpath = new_outpath

    chunks = RunQuery(os.path.join(os.path.join(os.getcwd(), "queries"), "get_sepsis_patients_lab_events.pgsql"))
    write_header = True
    for chunk in chunks:
        chunk.to_csv(outpath, mode='a', header=write_header)
        write_header = False #only write header on first pass...

def ExtractSepsisChartEvents(outpath):
    i = 0
    new_outpath = outpath
    while (os.path.exists(new_outpath)):
        i += 1
        new_outpath = outpath + ".bak." + str(i)

    if not outpath == new_outpath:
        os.rename(outpath, new_outpath)
        print("Output file already exsists (%s) renaming it to %s" % (outpath, new_outpath))
        outpath = new_outpath

    chunks = RunQuery(os.path.join(os.path.join(os.getcwd(), "queries"), "get_sepsis_patients_chart_events.pgsql"))
    write_header = True
    for chunk in chunks:
        chunk.to_csv(outpath, mode='a', header=write_header)
        write_header = False #only write header on first pass...