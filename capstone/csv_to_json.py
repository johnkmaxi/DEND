"""
Convert CSV files to JSON
"""
import glob
import pandas as pd

def main():
    # daily AQI readings
    files_to_convert = glob.glob('data/daily_88101*.zip')
    for f in files_to_convert:
        print(f)
        df = pd.read_csv(f)
        df = df.to_json(f[:-4]+'.json')

if __name__ == '__main__':
    main()
