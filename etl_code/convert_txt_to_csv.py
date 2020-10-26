
import pandas as pd
import csv
# convert txt file into csv file
read_file = pd.read_csv (r'combine16.txt')
read_file.to_csv (r'combine16.csv', index=None)
