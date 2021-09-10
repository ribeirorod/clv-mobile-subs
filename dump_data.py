
# from clickhouse_driver import Client
import pandas as pd
from io import StringIO
import sys
import requests
# import certifi

import urllib3
urllib3.disable_warnings()

pd.set_option('display.max_columns', 500, 'display.max_rows', 200)

url = 'https://biuser:mfduEwQ4sOd1@db-clickhouse-datastore-c1-01.tesseract.euwest1.mediaelements.io:443/tesseract'

train_file_name =sys.argv[1]
predict_file_name= sys.argv[2]
view_train = sys.argv[3]
view_predict = sys.argv[4]

# train data
dumped_data = requests.get(url, params={'query': view_train} , verify=False)
if "Code: " in dumped_data.text.splitlines()[0]:
    print(dumped_data.text.splitlines())
else:
    train_data = pd.read_csv(StringIO(dumped_data.text), dtype='str')
train_data.columns = [x.lower() for x in train_data.columns]

print(train_file_name, train_data.columns)

train_data.to_csv("/data/%s" %train_file_name, sep=";", index=False, compression='gzip')

print("saving training file on /data/%s" %train_file_name )

#  predict data
dumped_data = requests.get(url, params={'query': view_predict} , verify=False)
if "Code: " in dumped_data.text.splitlines()[0]:
    print(dumped_data.text.splitlines())
else:
    predict_data = pd.read_csv(StringIO(dumped_data.text), dtype='str')
predict_data.columns = [x.lower() for x in predict_data.columns]
predict_data.to_csv("/data/%s" %predict_file_name, sep=";", index=False, compression='gzip')