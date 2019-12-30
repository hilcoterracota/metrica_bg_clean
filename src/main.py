import pymongo
import uuid
from bson.json_util import dumps
from datetime import datetime, timedelta, date
import time
import os
import calendar
import subprocess
import ipaddress
from subprocess import Popen, PIPE

def sum_time_array_clear(entry):
    totalSecs = 0
    for tm in entry:
        timeParts = [int(s) for s in tm.split(':')]
        totalSecs += (timeParts[0] * 60 + timeParts[1]) * 60 + timeParts[2]
    totalSecs, sec = divmod(totalSecs, 60)
    hr, min = divmod(totalSecs, 60)
    return "%d:%02d:%02d" % (hr, min, sec)


while True:
    
    ahora = datetime.now()
    hora_entrada = datetime(ahora.year, ahora.month, ahora.day, hour=9, minute=0)
    hora_salida = datetime(ahora.year, ahora.month, ahora.day, hour=18, minute=30)
    
    if ahora >= hora_entrada and ahora <= hora_salida and calendar.day_name[ahora.weekday()] not in ['Saturday','Sunday']  :
        myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore')    
        snapshot= []
        today = date.today()
        myclient["HTERRACOTA"]["info_pc"].delete_many({'fechaupdate':  {'$regex':f'^((?!{str(today)}).)*$'}})
        myclient["HTERRACOTA"]["info_pc"].update_many({}, {'$set':{'fechaupdate': f'{str(today)} no-update'}})
        for usuario in myclient["HTERRACOTA"]["info_pc"].find({ 'fechaupdate':  {'$regex':str(today)}}):
            userId = usuario['hostiduiid']
            listaprosesos = []
            listaprosesos_aux  = []
            listaprosesos_aux_fechas  = []
            nombre_usuario = ""

            for proseso in usuario['infoprosses']: 
                lista_pestanias = []
                kb_uso_memoria = 0
                tiempo_uso_app = []
                for pestania in usuario['infoprosses']:
                    
                    if proseso['nombredeimagen'] == pestania['nombredeimagen']:
                        if str(pestania["tiempodecpu"]) != "0:00:00" and "HILCOTERRACOTA" in str(pestania["nombredeusuario"]):
                            
                            lista_pestanias.append({
                                "tituloVentana": str(pestania["ttulodeventana"]),
                                "tiempoDeUso": str(pestania["tiempodecpu"])
                            })
                            tiempo_uso_app.append(pestania["tiempodecpu"])    
                            nombre_usuario =  str(pestania["nombredeusuario"])     
                        kb_uso_memoria = kb_uso_memoria + float(str(pestania["usodememoria"]).replace("N/D", "0").replace(",", "").replace(" ", "").replace("KB", "")) 

                if str(proseso['nombredeimagen']) not in listaprosesos:   
                    listaprosesos.append(proseso['nombredeimagen'])
                    listaprosesos_aux.append({
                        "nombre":proseso['nombredeimagen'].replace(".exe", "").replace(".EXE", "").upper(),
                        "usoMemoria": kb_uso_memoria * 0.001,
                        "tiempoTotal": sum_time_array_clear(tiempo_uso_app),
                        "estado":proseso['estado'],
                        "ventanas":lista_pestanias,
                        "fecha":str(today)
                    })
                    listaprosesos_aux_fechas.append(sum_time_array_clear(tiempo_uso_app))
        
            au = filter(lambda x: x["tiempoTotal"].split(":")[1] != "00", listaprosesos_aux)
            
            if  "fechaupdate" in usuario:
            
                if nombre_usuario != "":
                    nombre_usuario = nombre_usuario.split("\\")[1]
                    if str(ahora).split(" ")[0] == usuario["fechaupdate"].split(" ")[0]:
                        ip = "0.0.0.0"
                        for interface in usuario['interfaces']: 
                            if interface["interfacename"] == "Ethernet":
                                if len(interface["ips"])==1:
                                    ip = interface["ips"][0]
                                else:  
                                    ip = interface["ips"][1]
                    
                        snapshot.append({
                            "userId":userId,
                            "usuario":nombre_usuario,
                            "ip":ip,
                            "listaprosesos":sorted(list(au), key=lambda element: element['usoMemoria'],reverse=True),
                            "tiempoUsoGlobal": sum_time_array_clear(listaprosesos_aux_fechas)
                        }) 
            else:
                print(usuario['hostiduiid'])
                
        for element in snapshot:
            usr_htr = myclient["HTERRACOTA"]["info_pc_historico"].find_one({'usuario': element["usuario"]})
            if "None" == str(usr_htr):
                myclient["HTERRACOTA"]["info_pc_historico"].insert_one({
                    "userId":element["userId"],
                    "usuario":element["usuario"],
                    "historico": element["listaprosesos"]
                })
            else:
                for proseso in element["listaprosesos"]:
                    data_historica = myclient["HTERRACOTA"]["info_pc_historico"].find_one({'usuario': element["usuario"]})["historico"]
                    for idxh, elemento_historico in enumerate(usr_htr["historico"]):
                        if elemento_historico["fecha"] == proseso["fecha"] and elemento_historico["nombre"] == proseso["nombre"]:
                            tiempoTotalAcumulado = proseso["tiempoTotal"]
                            
                            if "tiempoAnterior" not in data_historica[idxh] :
                                data_historica[idxh]["tiempoAnterior"]=proseso["tiempoTotal"]
                            X1P = datetime.strptime(proseso["tiempoTotal"], '%H:%M:%S')
                            x2P = datetime.strptime(data_historica[idxh]["tiempoAnterior"], '%H:%M:%S')
                            if X1P >= x2P:
                                tiempoTotalAcumulado = proseso["tiempoTotal"]
                            else:
                                tiempoTotalAcumulado = sum_time_array_clear([proseso["tiempoTotal"],tiempoTotalAcumulado])  
                            
                            data_historica[idxh]["ventanas"] = proseso["ventanas"]
                            data_historica[idxh]["tiempoTotal"] = tiempoTotalAcumulado
                            data_historica[idxh]["tiempoAnterior"] = proseso["tiempoTotal"]
                            data_historica[idxh]["estado"] = proseso["estado"]
                            print(f'Actualizando {element["usuario"]} - {elemento_historico["nombre"]}: {tiempoTotalAcumulado}')

                    if not list(filter(lambda x: x["nombre"] == proseso["nombre"] and x["fecha"] == str(today), data_historica)):
                        data_historica.append(proseso)
                        print(f'Agragando {element["usuario"]} - {proseso["nombre"]}: {proseso["tiempoTotal"]}')
    
                    myclient["HTERRACOTA"]["info_pc_historico"].update_many({"usuario":element["usuario"]}, {"$set":{"historico":data_historica}}, upsert=True)
                    myclient["HTERRACOTA"]["info_pc_historico"].update_many({"usuario":element["usuario"]}, {"$set":{"ip":element["ip"]}}, upsert=True)

        today1    = date.today()
        today2    = date.today() + timedelta(days=1) 
        today1    = today1.strftime("%Y/%m/%d").replace("/","-")
        today2    = today2.strftime("%Y/%m/%d").replace("/","-")
        ip_net    = ipaddress.ip_network(u'192.168.1.0/24', strict=False)
        ip_online = []
        print("Analizando red...")
        for ip in ip_net.hosts():
            ip = str(ip)
            print(ip)
            toping = Popen(['ping', '-c', '1', '-W', '50', ip], stdout=PIPE)
            output = toping.communicate()[0]
            hostalive = toping.returncode
            if hostalive ==0:
                ip_online.append(ip)

        print(len(ip_online),"host encontrados")

        data = []
        for ip in ip_online:
            myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore')
            
            try:  
                r = rqst.get(f'http://{ip}:5600/',verify=False, timeout=3)
                if r.status_code == 200:
                    r = rqst.get(f'http://{ip}:5600/api/0/buckets/',verify=False, timeout=5)
                    keys = list(r.json().keys())
                    host = ""
                    for key in keys:
                        if "aw-watcher-afk_" in key:
                            host = key.replace("aw-watcher-afk_","")
                
                            
                    payload = "{\n    \"query\": [\n        \"events  = flood(query_bucket('aw-watcher-window_"+host+"'));\",\n        \"not_afk = flood(query_bucket('aw-watcher-afk_"+host+"'));\",\n        \"not_afk = filter_keyvals(not_afk, 'status', ['not-afk']);\",\n        \"events  = filter_period_intersect(events, not_afk);\",\n        \"title_events = sort_by_duration(merge_events_by_keys(events, ['app', 'title']));\",\n        \"app_events   = sort_by_duration(merge_events_by_keys(title_events, ['app']));\",\n        \"cat_events   = sort_by_duration(merge_events_by_keys(events, ['$category']));\",\n        \"events = sort_by_timestamp(events);\",\n        \"app_events  = limit_events(app_events, 100);\",\n        \"title_events  = limit_events(title_events, 100);\",\n        \"duration = sum_durations(events);\",\n        \"RETURN  = {'app_events': app_events, 'title_events': title_events, 'cat_events': cat_events, 'duration': duration, 'active_events': not_afk};\"\n    ],\n    \"timeperiods\": [\n        \""+today1+"/"+today2+"\"\n    ]\n}"
                    headers = {
                        'Content-Type': "application/json"
                    }
                    r = rqst.request("POST", f'http://{ip}:5600/api/0/query/', data = payload, headers=headers)
                    
                    if r.status_code == 200:   
                        response = r.json()[0]
                        usr_htr = myclient["HTERRACOTA"]["ipcht"].find_one({"host":host})
                        
                        if "None" == str(usr_htr):
                            myclient["HTERRACOTA"]["ipcht"].insert_one({
                                "host"         : host,
                                "app_events"   : response["app_events"],
                                "title_events" : response["title_events"],
                                "cat_events"   : response["cat_events"],
                                "active_events": response["active_events"],
                                "duration"     : response["duration"],
                                "ip": ip
                                })
                        else:
                            myclient["HTERRACOTA"]["ipcht"].update_many(
                                {"host":host}, 
                                {"$set":{
                                    "app_events"   : response["app_events"],
                                    "title_events" : response["title_events"],
                                    "cat_events"   : response["cat_events"],
                                    "active_events": response["active_events"],
                                    "duration"     : response["duration"],
                                    "ip": ip
                                }}, upsert=True)
                        print(ip,host,"actualizado!")
            except:
                print(ip,"ERROR: ")


                        