import pymongo
import uuid
from bson.json_util import dumps
from datetime import datetime, timedelta, date
import time
import os
import calendar


def sum_time_array_clear(entry):
    totalSecs = 0
    for tm in entry:
        timeParts = [int(s) for s in tm.split(':')]
        totalSecs += (timeParts[0] * 60 + timeParts[1]) * 60 + timeParts[2]
    totalSecs, sec = divmod(totalSecs, 60)
    hr, min = divmod(totalSecs, 60)
    return "%d:%02d:%02d" % (hr, min, sec)


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


    myclient.close()



                    