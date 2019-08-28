
"""
AQI and NHIS Table Creation

Author: John Maxi
Date: 2019-08-12
"""

tables = ['PROBLEMS','NHIS','REGION','COUNTY','STATE','WEATHER']

columns = {
'PROBLEMS':"""(
    ID SERIAL NOT NULL,
    NAME varchar PRIMARY KEY NOT NULL,
    DESCRIPTION varchar
    )""",
'REGION':"""(
    REGION_C int PRIMARY KEY NOT NULL,
    REGION_N varchar NOT NULL
    )""",
'NHIS':"""(
    FPX int,
    FMX int,
    HHX int,
    LINE int,
    YEAR int,
    MONTH int NOT NULL,
    AGE_P int,
    WTFA float,
    SEX int,
    REGION_C int,
    PROBLEM_CODE varchar,
    PROBLEM int,
    PROBLEM_START_YR int,
    PROBLEM_START_MONTH int,
    PRIMARY KEY (FPX, FMX, HHX, YEAR, LINE)
    )""",
'COUNTY':"""(
    ID SERIAL NOT NULL,
    COUNTY_C int NOT NULL,
    COUNTY_N varchar NOT NULL,
    STATE_C int NOT NULL,
    PRIMARY KEY (COUNTY_C, STATE_C)
    );""",
'STATE':"""(
    STATE_C int PRIMARY KEY NOT NULL,
    STATE_N varchar NOT NULL,
    REGION_C int NOT NULL
    )""",
'WEATHER':"""(
    ID SERIAL NOT NULL,
    STATE_C int NOT NULL,
    COUNTY_C int NOT NULL,
    YEAR int NOT NULL,
    MONTH int NOT NULL,
    AQI_PREV_MONTH_AVG float,
    TEMP_PREV_MONTH_AVG float,
    PRESS_PREV_MONTH_AVG float,
    RH_PREV_MONTH_AVG float,
    DEW_POINT_MONTH_AVG float,
    WIND_SPEED_PREV_MONTH_AVG float,
    WIND_DIRECTION_PREV_MONTH_AVG float,
    PRIMARY KEY (STATE_C, COUNTY_C, YEAR, MONTH)
    )"""
}

county_insert = ("""INSERT INTO COUNTY(
                            COUNTY_C
                            ,COUNTY_N
                            ,STATE_C)
                            VALUES(%s, %s, %s)
                            ON CONFLICT DO NOTHING""")

state_insert = ("""INSERT INTO STATE(
                            STATE_C
                            ,STATE_N
                            ,REGION_C)
                            VALUES(%s, %s, %s)
                            ON CONFLICT DO NOTHING""")

weather_insert = ("""INSERT INTO WEATHER(
                            STATE_C
                            ,COUNTY_C
                            ,YEAR
                            ,MONTH
                            ,AQI_PREV_MONTH_AVG
                            ,TEMP_PREV_MONTH_AVG
                            ,PRESS_PREV_MONTH_AVG
                            ,RH_PREV_MONTH_AVG
                            ,DEW_POINT_MONTH_AVG
                            ,WIND_SPEED_PREV_MONTH_AVG
                            ,WIND_DIRECTION_PREV_MONTH_AVG)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (STATE_C, COUNTY_C, YEAR, MONTH)
DO UPDATE SET (AQI_PREV_MONTH_AVG, TEMP_PREV_MONTH_AVG ,PRESS_PREV_MONTH_AVG ,RH_PREV_MONTH_AVG
               ,DEW_POINT_MONTH_AVG ,WIND_SPEED_PREV_MONTH_AVG
              ,WIND_DIRECTION_PREV_MONTH_AVG) = (EXCLUDED.AQI_PREV_MONTH_AVG
            ,EXCLUDED.TEMP_PREV_MONTH_AVG, EXCLUDED.PRESS_PREV_MONTH_AVG
             ,EXCLUDED.RH_PREV_MONTH_AVG ,EXCLUDED.DEW_POINT_MONTH_AVG
             ,EXCLUDED.WIND_SPEED_PREV_MONTH_AVG ,EXCLUDED.WIND_DIRECTION_PREV_MONTH_AVG)""")

nhis_insert = ("""INSERT INTO NHIS(
                         FPX
                         ,FMX
                         ,HHX
                         ,LINE
                         ,YEAR
                         ,MONTH
                         ,AGE_P
                         ,WTFA
                         ,SEX
                         ,REGION_C
                         ,PROBLEM_CODE
                         ,PROBLEM
                         ,PROBLEM_START_YR
                         ,PROBLEM_START_MONTH)
                         VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  ON CONFLICT (FPX, FMX, HHX, YEAR, LINE)
                  DO UPDATE SET (MONTH
                                ,AGE_P
                                ,WTFA
                                ,SEX
                                ,REGION_C
                                ,PROBLEM_CODE
                                ,PROBLEM
                                ,PROBLEM_START_YR
                                ,PROBLEM_START_MONTH) =
                                (EXCLudED.MONTH
                                ,EXCLUDED.AGE_P
                                ,EXCLUDED.WTFA
                                ,EXCLUDED.SEX
                                ,EXCLUDED.REGION_C
                                ,EXCLUDED.PROBLEM_CODE
                                ,EXCLUDED.PROBLEM
                                ,EXCLUDED.PROBLEM_START_YR
                                ,EXCLUDED.PROBLEM_START_MONTH)""")

region_insert = ("""INSERT INTO REGION(
                        REGION_C
                        ,REGION_N)
                        VALUES(%s, %s)
                        ON CONFLICT DO NOTHING""")

problems_insert = ("""INSERT INTO PROBLEMS(
                        NAME,
                        DESCRIPTION)
                        VALUES(%s, %s)
                        ON CONFLICT DO NOTHING""")

count_problems_by_year = "select year, count(*) from nhis group by year order by year;"

count_weather_by_year = "select year, count(*) from weather group by year order by year;"

sum_problems_by_month_and_year = "select year, month, sum(problem) from nhis group by year, month order by year, month;"

sum_problems_by_start_date = """\
        select problem_start_yr,problem_start_month, sum(problem) 
        from nhis
        group by problem_start_yr, problem_start_month
        order by problem_start_yr, problem_start_month;"""
