import pymongo
import uuid
from bson.json_util import dumps
from datetime import datetime, timedelta, date
import time

while True:
    myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore')
    mydb = myclient["HTERRACOTA"]
    mycol = mydb["info_pc"]
    info_pc_historico = mydb['info_pc_historico']

    snapshot= []
    today = date.today()

    for usuario in mycol.find():
        userId = str(usuario['hostiduiid'])
        listaprosesos = []
        listaprosesos_aux  = []
        nombre_usuario = ""
        tiempo_uso_global = datetime.strptime('00:00:00', '%H:%M:%S')
        for proseso in usuario['infoprosses']: 
            lista_pestanias = []
            kb_uso_memoria = 0
            tiempo_uso_app = datetime.strptime('00:00:00', '%H:%M:%S')
            for pestania in usuario['infoprosses']:
                if str(proseso['nombredeimagen']) == str(pestania['nombredeimagen']):
                    if str(pestania["tiempodecpu"]) != "0:00:00" and "HILCOTERRACOTA" in str(pestania["nombredeusuario"]):
                        lista_pestanias.append({
                            "tituloVentana": str(pestania["ttulodeventana"]),
                            "tiempoDeUso": str(pestania["tiempodecpu"])
                        })
                        time_aux = str(pestania["tiempodecpu"]).split(":")
                        minutos = (int(time_aux[0])*60)+int(time_aux[1])
                        seconds_aux = (minutos*60)+int(time_aux[2])
                        ##FACTOR TIMEMPO USO/CPU
                        tiempo_uso_app = tiempo_uso_app + timedelta(seconds=int(seconds_aux)*5)
                        nombre_usuario =  str(pestania["nombredeusuario"])     
                    kb_uso_memoria = kb_uso_memoria + float(str(pestania["usodememoria"]).replace("N/D", "0").replace(",", "").replace(" ", "").replace("KB", "")) 

            if str(proseso['nombredeimagen']) not in listaprosesos:   
                listaprosesos.append(proseso['nombredeimagen'])
                listaprosesos_aux.append({
                    "nombre":proseso['nombredeimagen'].replace(".exe", "").replace(".EXE", "").upper(),
                    "usoMemoria": kb_uso_memoria * 0.001,
                    "tiempoTotal": str(tiempo_uso_app.strftime("%H:%M:%S")),
                    "estado":proseso['estado'],
                    "ventanas":lista_pestanias,
                    "fecha":str(str(today.year)+"-"+str(today.month)+"-"+str(today.day))
                })
                
                
                time_sub_aux = str(tiempo_uso_app.strftime("%H:%M:%S")).split(":")
                sub_minutos = (int(time_sub_aux[0])*60)+int(time_sub_aux[1])
                sub_seconds_aux = (sub_minutos*60)+int(time_sub_aux[2])
                tiempo_uso_global = tiempo_uso_global + timedelta(seconds=int(sub_seconds_aux))


        au = filter(lambda x: x["tiempoTotal"].split(":")[1] != "00", listaprosesos_aux)
        
        au_au = list(au)

        if(nombre_usuario != ""):
            nombre_usuario = nombre_usuario.split("\\")[1]
        snapshot.append({
            "userId":userId,
            "usuario":nombre_usuario,
            "listaprosesos":sorted(au_au, key=lambda element: element['usoMemoria'],reverse=True),
            "tiempoUsoGlobal": str(tiempo_uso_global)
        })  

    for element in snapshot:
        usr_htr = info_pc_historico.find_one({'userId': element["userId"]})
        if "None" == str(usr_htr):
            info_pc_historico.insert_one({
                "userId":element["userId"],
                "usuario":element["usuario"],
                "historico": element["listaprosesos"]
            })
        else:
            for proseso in element["listaprosesos"]:
                data_historica = usr_htr["historico"]
                for idxh, elemento_historico in enumerate(usr_htr["historico"]):
                    if elemento_historico["fecha"] == proseso["fecha"] and elemento_historico["nombre"] == proseso["nombre"]:
                        print("se actualiza el inxex del historico: "+ str(idxh))
                        data_historica[idxh]["tiempoTotal"] = proseso["tiempoTotal"]
                        data_historica[idxh]["ventanas"] = proseso["ventanas"]
                        data_historica[idxh]["tiempoTotal"] = proseso["tiempoTotal"]
                        data_historica[idxh]["estado"] = proseso["estado"]
                if not list(filter(lambda x: x["nombre"] == proseso["nombre"] and x["fecha"] == str(today), data_historica)):
                    print("Se agrega un nuevo historico")
                    data_historica.append(proseso)
                info_pc_historico.update_many({"userId":element["userId"]}, {"$set":{"historico":data_historica}}, upsert=True)
    
    time.sleep(5)
        
                    