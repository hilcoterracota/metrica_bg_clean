import pymongo
import uuid
from bson.json_util import dumps
from datetime import datetime, timedelta, date
import time
import os
import calendar

def sum_time_array_object(entry,promedio):
    if len(entry) == 0:
        return 0
    else:
        t = datetime.strptime('00:00:00', '%H:%M:%S')
        for item in entry:
            if promedio:
                p = len(entry)
            else:
                p = 1
        h, m, s = item["tiempoTotal"].split(':')
        t = t + timedelta(hours=int(h)/p, minutes=int(m)/p, seconds=int(s)/p)
        a,b,c = str(t.strftime("%H:%M:%S")).split(':')
    return round(((int(a)*6300)+(int(b)*60)+int(c))/3600,2)

def sum_time_array(entry,promedio):
    t = datetime.strptime('00:00:00', '%H:%M:%S')
    for item in entry:
        if promedio:
            p = len(entry)
        else:
            p = 1
        h, m, s = item.split(':')
        t = t + timedelta(hours=int(h)/p, minutes=int(m)/p, seconds=int(s)/p)
    return t.strftime("%H:%M:%S")


while True:
    
    ahora = datetime.now()
    hora_entrada = datetime(ahora.year, ahora.month, ahora.day, hour=9, minute=0)
    hora_salida = datetime(ahora.year, ahora.month, ahora.day, hour=18, minute=30)

    if ahora >= hora_entrada and ahora <= hora_salida and calendar.day_name[ahora.weekday()] not in ['Saturday','Sunday']  :
        myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore')
        try:
            snapshot= []
            today = date.today()
            for usuario in myclient["HTERRACOTA"]["info_pc"].find():
                userId = usuario['hostiduiid']
                listaprosesos = []
                listaprosesos_aux  = []
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
                            "tiempoTotal": sum_time_array(tiempo_uso_app,False),
                            "estado":proseso['estado'],
                            "ventanas":lista_pestanias,
                            "fecha":str(today)
                        })

                au = filter(lambda x: x["tiempoTotal"].split(":")[1] != "00", listaprosesos_aux)
                
                if  "fechaupdate" in usuario:
                    if(nombre_usuario != "" and str(ahora).split(" ")[0]) == usuario["fechaupdate"].split(" ")[0]:
                        nombre_usuario = nombre_usuario.split("\\")[1]
                        snapshot.append({
                            "userId":userId,
                            "usuario":nombre_usuario,
                            "listaprosesos":sorted(list(au), key=lambda element: element['usoMemoria'],reverse=True),
                            "tiempoUsoGlobal": sum_time_array_object(listaprosesos_aux,False)
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
                                if proseso["tiempoTotal"] >= data_historica[idxh]["tiempoTotal"]:
                                    tiempoTotalAcumulado = proseso["tiempoTotal"]
                                else:
                                    tiempoTotalAcumulado = sum_time_array([proseso["tiempoTotal"],tiempoTotalAcumulado],False)

                                data_historica[idxh]["ventanas"] = proseso["ventanas"]
                                data_historica[idxh]["tiempoTotal"] = tiempoTotalAcumulado
                                data_historica[idxh]["tiempoAnterior"] = proseso["tiempoTotal"]
                                data_historica[idxh]["estado"] = proseso["estado"]
                                print(f'Actualizando {element["usuario"]} - {elemento_historico["nombre"]}: {tiempoTotalAcumulado}')

                        if not list(filter(lambda x: x["nombre"] == proseso["nombre"] and x["fecha"] == str(today), data_historica)):
                            data_historica.append(proseso)
                            print(f'Agragando {element["usuario"]} - {proseso["nombre"]}: {proseso["tiempoTotal"]}')

                        myclient["HTERRACOTA"]["info_pc_historico"].update_many({"usuario":element["usuario"]}, {"$set":{"historico":data_historica}}, upsert=True)
        except: 
            print("An exception occurred")
    
        myclient.close()
    
    time.sleep(5)

                    