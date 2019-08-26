# helper.py
import re
from zipfile import ZipFile

import dask.dataframe as dd
import numpy as np
import pandas as pd
from io import StringIO

from sql_queries import county_insert, state_insert, weather_insert, nhis_insert, problems_insert

def file_finder(keys, search_list):
    """
    finds files in the search list associated with a list of keys to search for
    """
    pattern = "|".join(re.escape(s) for s in keys)
    crexp = re.compile(pattern)
    matches = [s for s in search_list if crexp.search(s) is not None]
    return matches

def csv_in_zip(archive_path, **kwargs):
    """
    get csv file from a zip archive holding data in multiple file types
    """
    with ZipFile(archive_path, 'r') as z:
        df = pd.read_csv(z.open('personsx.csv'), **kwargs)
    return df

def calc_problem_duration(df):
    """
    1 - days
    2 - weeks
    3 - months
    4 - years
    6 - since birth
    7 - refused
    8 - not ascertained
    9 - don't know
    """
    time_map = {1:'D',2:'W',3:'M',4:'Y'}
    try:
        if df.iloc[1:].isnull().any():
            return np.nan
        elif df.iloc[2] > 4:
            return np.nan
        else:
            return pd.to_timedelta(df.iloc[1], unit=time_map[df.iloc[2]])
    except:
        print(df)
        print(df.isnull().any())
        print(df.iloc[1], df.iloc[2])

def bulk_df_insert(df, cur, table, schema='postgres'):
    """
    Writes a df to a csv "buffer" and then does bulk upload of the CSV file
    :param: df, Pandas DataFrame
    :conn: psycopg2 database connection. Intened for use with postgresql
    """
    # Initialize a string buffer
    sio = StringIO()
    sio.write(df.to_csv(index=None, header=None))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream

    # Copy the string buffer to the database, as if it were an actual file
    # with conn.cursor() as c:
    cur.copy_from(sio, f"{table}", columns=df.columns, sep=',')
        #conn.commit()

def process_weather(files, cur, log):
    """
    process the AQI/WEATHER/WiND data
    process each year of data individually. I will need to first filter each file
    based on the subset of measurements that are valid. Second is to get the list
    of unique, cities, states, counties available for that year. These values get
    inserted into their respective tables, rows that already exist for that particular
    code are dropped from the insert. Then the monthly and yearly average reading
    values need to be calculated per (state, county, city) grouping. These values are
    then unpivoted and joined together to form the rows of the WEATHER and WIND tables.
    """
    # open the files as DataFrames
    # extract the key from the filenames so we know what type of data each df holds
    # read_csv if a zip file, read_json otherwise
    dfs = {re.search('_(\w+)_', f).group(1): pd.read_csv(f) if f[-3:]=='zip' else pd.read_json(f) for f in files}
    for k, v in dfs.items():
        log.logger.info(f'{k} shape: {v.shape}')
    # Parmeter Name = 'PM2.5 - Local Conditions'
    dfs['88101'] = dfs['88101'][dfs['88101']['Pollutant Standard'] == 'PM25 24-hour 2012']
    # concat dfs into a single DataFrame
    df = pd.concat(dfs.values(), axis=0)
    log.logger.info(f'df shape: {df.shape}')

    # create Year and Month columns
    df['ts'] = pd.to_datetime(df['Date Local'])

    # log.logger.info("extract State and County info")
    states = df[['State Code','State Name']].drop_duplicates()
    states['Region'] = states['State Name'].map(region_state_map).fillna(-1)
    counties = df[['County Code','County Name','State Code']].drop_duplicates().fillna(-1)

    # log.logger.info('created location tables')

    # pivot around rows location and time rows
    df_month = df.pivot_table(index=['State Code','County Code','ts'],
                              columns='Parameter Name',
                              values='Arithmetic Mean',
                              aggfunc='mean')
    # calculate the monthly averages
    months = df_month.groupby([pd.Grouper(level='State Code'),
                               pd.Grouper(level='County Code'),
                               pd.Grouper(freq='M', level='ts')]).mean()
    log.logger.info(f'df_month shape: {df_month.shape}')

    # add one month to the months index, this causes current months data to be
    # associated with the next month
    log.logger.info("making dates offset")
    months.index = months.index.set_levels(months.index.levels[-1] + pd.DateOffset(months=1), level=-1)
    months = months.reset_index()
    months['Year'] = months['ts'].dt.year
    months['Month'] = months['ts'].dt.month

    log.logger.info("order columns for as in data model")
    months = months[['State Code','County Code','Year','Month','PM2.5 - Local Conditions',
                     'Outdoor Temperature','Barometric pressure','Relative Humidity ',
                     'Dew Point','Wind Speed - Resultant','Wind Direction - Resultant']]

    log.logger.info('created month table')
    log.logger.info(f'states shape: {states.shape}')
    log.logger.info(f'counties shape: {counties.shape}')
    log.logger.info(f'months shape: {months.shape}')
    # insert data
    for i, row in states.iterrows():
        cur.execute(state_insert, list(row))
    for i, row in counties.iterrows():
        cur.execute(county_insert, list(row))
    for i, row in months.iterrows():
        cur.execute(weather_insert, list(row))

def process_nhis(file, cur, log):
    log.logger.info('read csv within zip archive')
    df = csv_in_zip(file)
    #df = dd.from_pandas(df, npartitions=500)
    log.logger.info('data loaded')
    log.logger.info(f'raw data shape: {df.shape}')
    # drop rows using the QC flag
    df = df[df['QCADULT'].isnull()]
    df = df[df['QCCHILD'].isnull()]
    log.logger.info('quality filter')
    log.logger.info('get the child and adult problems, times and durations')
    child_problems = [x for x in df.columns if 'LAHCC' in x]
    child_tuples = []
    for p in child_problems:
        n = re.search('\d+', p).group()
        t = [x for x in df.columns if 'LCTIME' in x and n in x][0]
        d = [x for x in df.columns if 'LCUNIT' in x and n in x][0]
        child_tuples.append((p, t, d))

    adult_problems = [x for x in df.columns if 'LAHCA' in x]
    adult_tuples = []
    for p in adult_problems:
        n = re.search('\d+', p).group()
        t = [x for x in df.columns if ('LATIME' in x or 'LTIME' in x) and n in x][0]
        d = [x for x in df.columns if ('LAUNIT' in x or 'LUNIT' in x) and n in x][0]
        adult_tuples.append((p,t,d))
    for t in child_tuples:
        log.logger.info(f'{t}')
    for t in adult_tuples:
        log.logger.info(f'{t}')
    log.logger.info('calculate how far back the problems started')
    for p in child_tuples:
        #df[f'start_{p[0]}'] = df[list(child_tuples[0])].apply(calc_problem_duration, axis=1, meta=(None, 'timedelta64[ns]'))
        idxs = df[list(child_tuples[0])].dropna().index
        to_insert = df[list(child_tuples[0])].dropna().apply(calc_problem_duration, axis=1)#, meta=(None, 'timedelta64[ns]')
        df.loc[idxs, f'start_{p[0]}'] = to_insert
    for p in adult_tuples:
        idxs = df[list(adult_tuples[0])].dropna().index
        to_insert = df[list(adult_tuples[0])].dropna().apply(calc_problem_duration, axis=1)#, meta=(None, 'timedelta64[ns]')
        df.loc[idxs, f'start_{p[0]}'] = to_insert
    log.logger.info('calculate the start year and start month')
    df['ts'] = pd.to_datetime(df['SRVY_YR'].astype(str)+df['INTV_MON'].astype(str), format='%Y%m')

    log.logger.info('stack problems into a column')
    melted1 = pd.melt(df,#.reshape
                      id_vars=['FPX','FMX','HHX','SRVY_YR','INTV_MON','AGE_P','WTFA','SEX','REGION','ts'],
                      value_vars=adult_problems+child_problems,
                      var_name='PROBLEM',
                      value_name='PROBLEM_CODE')
    log.logger.info('stack how long ago problem started into a column')
    melted2 = pd.melt(df,#.reshape
                      id_vars=['FPX','FMX','HHX','SRVY_YR','INTV_MON','AGE_P','WTFA','SEX','REGION','ts'],
                      value_vars=[x for x in df.columns if 'start' in x],
                      var_name='start',
                      value_name='start_ago')
    melted2['PROBLEM'] = melted2['start'].str[6:]

    log.logger.info('find out the year and month when a problem started')
    merged = pd.merge(melted1,
                      melted2,
                      left_on=['FPX','FMX','HHX','SRVY_YR','INTV_MON','AGE_P','WTFA','SEX','REGION','PROBLEM','ts'],
                      right_on=['FPX','FMX','HHX','SRVY_YR','INTV_MON','AGE_P','WTFA','SEX','REGION','PROBLEM','ts'],
                      how='inner')
    merged['PROBLEM_START'] = merged['ts'] - merged['start_ago']
    merged['LINE'] = merged.groupby(['FPX','FMX','HHX']).cumcount()
    log.logger.info('make PROBLEM_START_YR and Month')
    merged['PROBLEM_START_YR'] = merged['PROBLEM_START'].dt.year
    merged['PROBLEM_START_MONTH'] = merged['PROBLEM_START'].dt.month
    log.logger.info('recode identical child/adult problems:')
    # vision
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA1' if x == 'LAHCC1' else x)#, meta=('PROBLEM', 'object')
    # hearing
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA2' if x == 'LAHCC2' else x)#, meta=('PROBLEM', 'object')
    # birth defects
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA13' if x == 'LAHCC5' else x)#, meta=('PROBLEM', 'object')
    # injury
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA6' if x == 'LAHCC6' else x)#, meta=('PROBLEM', 'object')
    # intellectual disability, mental retardation
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA14A' if x == 'LAHCC7A' else x)#, meta=('PROBLEM', 'object')
    # other development problem, cerebral palsy
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA15' if x == 'LAHCC8' else x)#, meta=('PROBLEM', 'object')
    # mental/emotional/behavioral problem to adult depression; anxiety; emotional problem
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA17' if x == 'LAHCC9' else x)#, meta=('PROBLEM', 'object')
    # bone, joint, muscle problem to adult fracture; bone/joint injury
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA5' if x == 'LAHCC10' else x)#, meta=('PROBLEM', 'object')
    # other 1 and other 2
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA90' if x == 'LAHCC90' else x)#, meta=('PROBLEM', 'object')
    merged['PROBLEM'] = merged['PROBLEM'].apply(lambda x: 'LAHCA91' if x == 'LAHCC91' else x)#, meta=('PROBLEM', 'object')
    # clean problem codes
    merged['PROBLEM_CODE'] = merged['PROBLEM_CODE'].apply(lambda x: 1 if x == 1 else 0)#, meta=('PROBLEM_CODE', 'int64')
    # order columns for insert
    merged = merged[['FPX','FMX','HHX','LINE','SRVY_YR','INTV_MON','AGE_P','WTFA',
                     'SEX','REGION','PROBLEM','PROBLEM_CODE','PROBLEM_START_YR',
                     'PROBLEM_START_MONTH']]
    merged[['PROBLEM_START_YR','PROBLEM_START_MONTH']] =  merged[['PROBLEM_START_YR','PROBLEM_START_MONTH']].replace({np.nan:None})
    merged.columns = ['FPX','FMX','HHX','LINE','SRVY_YR','INTV_MON','AGE_P','WTFA',
                      'SEX','REGION_C','PROBLEM','PROBLEM_CODE','PROBLEM_START_YR',
                      'PROBLEM_START_MONTH']

    log.logger.info('insert data')
    log.logger.info(f'inserted data shape: {merged.shape}')
    # bulk_df_insert(merged, cur, 'NHIS', schema='postgres')
    for i, row in merged.iterrows():
        cur.execute(nhis_insert, list(row))

    log.logger.info('insert problems dim table')
    problems_table = pd.DataFrame(merged['PROBLEM'].drop_duplicates())
    problems_table['DESCRIPTION'] = problems_table['PROBLEM'].map(problem_map)
    log.logger.info(f'problems dim shape: {problems_table.shape}')
    for i, row in problems_table.iterrows():
        cur.execute(problems_insert, list(row))

problem_map = {
'LAHCA1':'vision problem',
'LAHCA2':'hearing problem',
'LAHCA3':'arthritis; rheumatism',
'LAHCA4':'back/neck problem',
'LAHCA5':'fracture; bone/joint injury',
'LAHCA6':'other injury',
'LAHCA7':'heart problem',
'LAHCA8':'stroke',
'LAHCA9':'hypertension; high blood pressure',
'LAHCA10':'diabetes',
'LAHCA11':'lung/breathing problem; asthma; emphysema',
'LAHCA12':'cancer',
'LAHCA13':'birth defect',
'LAHCA15':'other developmental problem; cerebral palsy',
'LAHCA16':'senility; dementia; Alzheimer\'s',
'LAHCA17':'depression; anxiety; emotional problem',
'LAHCA18':'weight problem; overweight; obesity',
'LAHCA90':'Other 1',
'LAHCA91':'Other 2',
'LAHCA19_':'missing limbs (fingers, toes, digits); amputee',
'LAHCA20_':'musculoskeletal system; connective tissue',
'LAHCA21_':'circulation problem; circulatory system; blood clots',
'LAHCA22_':'endocrine; nutritional; metabolic',
'LAHCA23_':'nervous system; sensory organ condition',
'LAHCA24_':'digestion',
'LAHCA25_':'genitourinary system',
'LAHCA26_':'skin; subcutaneous system',
'LAHCA27_':'blood; blood-forming organ',
'LAHCA28_':'benign tumor; cyst',
'LAHCA29_':'alcohol; drugs; substance abuse',
'LAHCA30_':'mental illness; ADD; bipolar; schizophrenia',
'LAHCA31_':'surgical after-effects; medical treatment; operation; surgery',
'LAHCA32_':'elderly; old age; aging',
'LAHCA33_':'fatigue; tiredness; weakness',
'LAHCA34_':'pregnancy',
'LAHCA14A':'intellectual disability; mental retardation',
'LAHCC3':'speech problem',
'LAHCC4':'asthma; breathing problem',
'LAHCC11':'epilepsy; seizure',
'LAHCC12':'learning disability',
'LAHCC13':'attention deficit/hyperactivity disorder; ADD/ADHD'
}

region_codes = {'Northeast':1, 'Midwest':2, 'South':3, 'West':4}

# https://www.businessinsider.com/regions-of-united-states-2018-5
region_state_map = {
'Maine':1,
'New Hampshire':1,
'Vermont':1,
'Massachusetts':1,
'Rhode Island':1,
'Connecticut':1,
'New York':1,
'New Jersey':1,
'Pennsylvania':1,
'Ohio':2,
'Michigan':2,
'Indiana':2,
'Wisconsin':2,
'Illinois':2,
'Minnesota':2,
'Iowa':2,
'Missouri':2,
'North Dakota':2,
'South Dakota':2,
'Nebraska':2,
'Kansas':2,
'Delaware':3,
'Maryland':3,
'District of Columbia':3,
'Virginia':3,
'West Virginia':3,
'Kentucky':3,
'North Carolina':3,
'South Carolina':3,
'Tennessee':3,
'Georgia':3,
'Florida':3,
'Alabama':3,
'Mississippi':3,
'Arkansas':3,
'Louisiana':3,
'Texas':3,
'Oklahoma':3,
'Montana':4,
'Idaho':4,
'Wyoming':4,
'Colorado':4,
'New Mexico':4,
'Arizona':4,
'Utah':4,
'Nevada':4,
'California':4,
'Oregon':4,
'Washington':4,
'Alaska':4,
'Hawaii':4
}
