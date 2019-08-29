Pipeline steps:
1.	Clone the git repo:
2.	Download data from sources:
a.	Weather source: https://aqs.epa.gov/aqsweb/airdata/download_files.html
i.	Use the daily zip files for 2014 - 2018
b.	NHIS source: https://www.cdc.gov/nchs/nhis/nhis_2015_data_release.htm
i.	Use the person data zip files for 2015 - 2018
3.	Setup project directory:
a.	Data should be in the data folder: 1 zip folder per year of data downloaded from above sources.
b.	Create a directory “logs” in the data folder
4.	Clone anaconda env using environment.yml file. In the anaconda terminal:
		conda env create -f environment.yml
5.	Run tests.py to test the helper functions and the correct directory structure
6.	Run python csv_to_json.py to convert some of the csv data to JSON (this is to meet project requirement of using different data formats, it is not necessary for recreating the project).
7.	Run create_tables.py
8.	Run etl.py
9.	Use the sample queries to get started with analysis!
