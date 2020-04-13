import pandas as pd
import requests as rqs
import os
import json
import pymongo
import datetime
import pytz

myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USERNAME"],password=os.environ["MONGO_PASSWORD"], unicode_decode_error_handler='ignore')
activitywatch_db = myclient["activitywatch"]
aw_watcher_window_db = myclient["activitywatch"]["aw-watcher-window"]

while True:
    for value in range(100,200):
        ip = f'{os.environ["MONGO_IP3"]}.{value}'
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
                "timestamp": e['timestamp'],
                "duration": e['duration'],
                "host": host.upper(),
                **e["data"]
            } for e in aw_window['events']]

            df = pd.DataFrame(events)
            df['timestamp'] = pd.to_datetime(df['timestamp'],infer_datetime_format=True)
            df['date'] = [x.astimezone(pytz.timezone('America/Mexico_City'))
                        for x in df['timestamp']]
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day'] = df['date'].dt.day
            df['hour'] = df['date'].dt.hour
            df['minute'] = df['date'].dt.minute
            df['time'] = df['duration'].apply(lambda x:
                '{:.0f} hr {:.0f} min {:.0f} secs'.format(
                    divmod(x,60*60)[0],
                    *divmod(divmod(x,60*60)[1],60))
                                            )

            df['app'] = df['app'].str.upper()
            df['app'] = df['app'].str.replace(".EXE","")

            df = df.loc[df['app']!= "LOCKAPP"]
            df = df.loc[df['app']!= "UNKNOWN"]
            df = df.loc[df['title']!= ""]
            df = df.loc[df['duration']!= 0]

            df = df.drop(['timestamp','date'], axis=1)
            print(str(datetime.datetime.today()),ip,"Actualizando db ...")
            aw_watcher_window_db.delete_many ({"host": host.upper()})
            print(str(datetime.datetime.today()),ip,"Agragando nueva data ...")
            aw_watcher_window_db.insert_many(df.to_dict(orient='records'))
            print(str(datetime.datetime.today()),ip,host,"Actualizado!")
        except:
            print(str(datetime.datetime.today()),ip,"ERROR")