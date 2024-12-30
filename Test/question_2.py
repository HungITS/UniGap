import pandas as pd

data = pd.read_json('data_test.json')


data['join_date'] = pd.to_datetime(data['join_date'])
data.info()

data.to_excel('data_test.xlsx')