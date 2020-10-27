import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime
from espandas import Espandas
import json
import requests

df = pd.read_csv('gb_parliament.csv')#, skiprows=1)
nat = []
df = df.drop_duplicates(subset=['id','group_id'])
for i in range(df.shape[0]):
    nat.append('GB')
df = df[['id','name','sort_name','email','group_id','group']]
df.rename(columns={'group':'gname'}, inplace=True)
df['nationality'] = nat

INDEX="parliament"
TYPE= "doc"

def insertDataframeIntoElastic(dataFrame, index='index', type = 'test', server = 'http://localhost:9200',
                           chunk_size = 2000):
    headers = {'content-type': 'application/x-ndjson', 'Accept-Charset': 'UTF-8'}
    records = dataFrame.to_dict(orient='records')
    actions = ["""{ "index" : { "_index" : "%s", "_type" : "%s"} }\n""" % (index, type) +json.dumps(records[j])
                    for j in range(len(records))]
    i=0
    while i<len(actions):
        serverAPI = server + '/_bulk'
        data='\n'.join(actions[i:min([i+chunk_size,len(actions)])])
        data = data + '\n'
        r = requests.post(serverAPI, data = data, headers=headers)
        i = i+chunk_size

    return True

es = Elasticsearch()
doc = {
    'author': 'me',
    'text': 'Elasticsearch: cool.',
    'timestamp': datetime.now(),
}
if not es.indices.exists(INDEX):
    res = es.index(index = INDEX, id = 1, body = doc)
    # raise RuntimeError('index does not exist, use `curl -X PUT "localhost:9200/%s"` and try again'%INDEX)

print(insertDataframeIntoElastic(df))