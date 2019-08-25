
"""
AQI and NHIS Table Creation

Author: John Maxi
Date: 2019-08-12
"""

tables = ['PROBLEMS','NHIS','REGION','COUNTY','STATE','WEATHER']

columns = {
'PROBLEMS':"""(
    ID SERIAL PRIMARY KEY NOT NULL,
    NAME varchar NOT NULL,
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
    SRVY_YR int,
    INTV_MON int NOT NULL,
    AGE_P int,
    WTFA float,
    SEX int,
    REGION_C int,
    PROBLEM varchar,
    PROBLEM_CODE int,
    PROBLEM_START_YR int,
    PROBLEM_START_MONTH int,
    PRIMARY KEY (FPX, FMX, HHX, SRVY_YR, LINE)
    )""",
'COUNTY':"""(
    ID SERIAL PRIMARY KEY NOT NULL,
    COUNTY_C int NOT NULL,
    COUNTY_N varchar NOT NULL,
    STATE_C int NOT NULL
    );""",
'STATE':"""(
    STATE_C int PRIMARY KEY NOT NULL,
    STATE_N varchar NOT NULL,
    REGION_C int NOT NULL
    )""",
'WEATHER':"""(
    ID SERIAL PRIMARY KEY NOT NULL,
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
    WIND_DIRECTION_PREV_MONTH_AVG float
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
                            DO UPDATE SET (AQI_PREV_MONTH_AVG
                                          ,TEMP_PREV_MONTH_AVG
                                          ,PRESS_PREV_MONTH_AVG
                                          ,RH_PREV_MONTH_AVG
                                          ,DEW_POINT_MONTH_AVG
                                          ,WIND_SPEED_PREV_MONTH_AVG
                                          ,WIND_DIRECTION_PREV_MONTH_AVG) =
                                          (EXCLUDED.AQI_PREV_MONTH_AVG
                                          ,EXCLUDED.TEMP_PREV_MONTH_AVG
                                          ,EXCLUDED.PRESS_PREV_MONTH_AVG
                                          ,EXCLUDED.RH_PREV_MONTH_AVG
                                          ,EXCLUDED.DEW_POINT_MONTH_AVG
                                          ,EXCLUDED.WIND_SPEED_PREV_MONTH_AVG
                                          ,EXCLUDED.WIND_DIRECTION_PREV_MONTH_AVG)""")

nhis_insert = ("""INSERT INTO NHIS(
                         FPX
                         ,FMX
                         ,HHX
                         ,LINE
                         ,SRVY_YR
                         ,INTV_MON
                         ,AGE_P
                         ,WTFA
                         ,SEX
                         ,REGION_C
                         ,PROBLEM
                         ,PROBLEM_CODE
                         ,PROBLEM_START_YR
                         ,PROBLEM_START_MONTH)
                         VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  ON CONFLICT (FPX,FMX,HHX,LINE,SRVY_YR,INTV_MON)
                  DO UPDATE SET (AGE_P
                                ,WTFA
                                ,SEX
                                ,REGION_C
                                ,PROBLEM
                                ,PROBLEM_CODE
                                ,PROBLEM_START_YR
                                ,PROBLEM_START_MONTH) =
                                (EXCLUDED.AGE_P
                                ,EXCLUDED.WTFA
                                ,EXCLUDED.SEX
                                ,EXCLUDED.REGION_C
                                ,EXCLUDED.PROBLEM
                                ,EXCLUDED.PROBLEM_CODE
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
