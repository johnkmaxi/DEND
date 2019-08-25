"""
ETL Script
"""
import datetime
import glob
import re

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import psycopg2

from DataLogger import DataProcessingLogger
from helper import file_finder, region_state_map, csv_in_zip, calc_problem_duration
from helper import process_weather, bulk_df_insert, problem_map, process_nhis
from sql_queries import county_insert, state_insert, weather_insert, nhis_insert, problems_insert

def process_weather_files(cur, year, weather_keys, log):
    """
    """
    # open the files
    files = glob.glob(f'data/daily*{year}*')
    # get separate file types
    weather_files = file_finder(weather_keys, files, log)
    log.logger.info(f'weather files for {year}: {weather_files}')
    process_weather(weather_files, cur)

def process_nhis_files(cur, year, problem_keys, log):
    """
    """
    files = glob.glob(f'data/person*{year}*')
    problem_files = file_finder(problem_keys, files, log)
    assert len(problem_files) == 1, 'Found too many zip archives'
    log.logger.info(f'nhis files for {year}: {problem_files}')
    process_nhis(problem_files[0], cur)


def main():
    log = DataProcessingLogger(os.getcwd(), names=['etl'])
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()

    weather_keys = ['88101', 'PRESS', 'RH_DP', 'TEMP', 'WIND']
    log.logger.info(f'weather variables: {weather_keys}')
    problem_keys = ['person']
    log.logger.info(f'nhis variables: {problem_keys}')
    years = [2014, 2015, 2016, 2017, 2018]
    print(f'processing started: {datetime.datetime.now()}')
    log.logger.info(f'processing started: {datetime.datetime.now()}')
    for year in years:
        process_weather_files(cur, year, weather_keys, log)
        if year != 2014:
            process_nhis_files(cur, year, problem_keys, log)
        conn.commit()
    log.logger.info(f'processing finished: {datetime.datetime.now()}')
    print(f'processing finished: {datetime.datetime.now()}')
    conn.close()

if __name__ == '__main__':
    main()
