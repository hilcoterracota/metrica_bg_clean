from datetime import timedelta
import pandas as pd
import requests as rqs
import os
import json
import pymongo

myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USERNAME"],password=os.environ["MONGO_PASSWORD"], unicode_decode_error_handler='ignore')
activitywatch_db = myclient["activitywatch"]
aw_watcher_window_db = myclient["activitywatch"]["aw-watcher-window"]

while True:
    for value in range(100,200):
        ip = f'192.168.3.{value}'
        try:  
            data = rqs.get(f'http://{ip}:5600/api/0/export').json()
            host = ''
            for key in data['buckets'].keys(): 
                if 'aw-watcher-window' in key:
                    host = key.replace('aw-watcher-window_','')
                    aw_window = data['buckets'][key]
                if 'aw-watcher-afk' in key:
                    aw_afk = data['buckets'][key]
            events = [{
                "timestamp": e["timestamp"],
                "duration": e["duration"],
                "host": host,
                **e["data"]
            } for e in aw_window['events']]
            print(str(datetime.datetime.today()),ip,"Actualizando db ...")
            aw_watcher_window_db.delete_many ({"host": host})
            print(str(datetime.datetime.today()),ip,"Agragando nueva data ...")
            aw_watcher_window_db.insert_many(events)
            print(str(datetime.datetime.today()),ip,host,"Actualizado!")
        except:
            print(str(datetime.datetime.today()),ip,"ERROR")
    