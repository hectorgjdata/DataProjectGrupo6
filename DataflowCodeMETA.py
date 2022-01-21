####################################################################
####################################################################

#NOTA IMPORTANTE

#    - El programa genera un diccionario "Yo" en el que están los atributos de nuestro usuario (aleatorio) con coordenadas en el centro del mapa.

#    - Genera una lista de 100 diccionarios "users_generated", uno para cada usuario, y va actualizando sus coordenadas en cada step.

#    - En cada usuario hay un atributo "friends" que puede ser 1 o 0 si es amigo nuestro o no. Sólo sabemos qué usuario es amigo nuestro, no si lo son entre ellos.

#    - El código genera una lista de diccionarios "amigoscerca" con el esquema {"@usuario" : "@PedroNieto"} en la que SOLO estarán los usuarios que sean amigos nuestros Y estén cerca en ese momento.

#    - Para salir del bucle hay que cancelar desde la interfaz.

#LO QUE FALTA

# 1. Crear tabla "Yo" en BigQuery e insertar los datos. Esa tabla no se actualizará.

# 2. Crear y actualizar en bucle las tablas "users_generated" y "amigoscerca"

# En el código está escrito dónde hay que insertar el código


####################################################################
####################################################################

#pip install Faker
#pip install keyboard
#pip install geopy

#LIBRARIES
from geopy import distance
from faker import Faker
import time
import random


#########
#AJUSTES#
#########

#Seleccionar tiempo de refresh (s)
refresh=2

#Ajustar velocidad
multiplo=1

#Radio (km)
radius=2

#Usuarios totales
USERS_TOTAL=100

#Definición cuadrante del mapa
lat_min=39.4505101
lat_max=39.4939737
lon_min=-0.3952789
lon_max=-0.3449821
vehicles=["Bike","Train","Car", "Walking"]
####################################################################
####################################################################



faker = Faker('es_ES')

#Usuario Yo

yo={}
yo["id"]=faker.ssn()
yo["name"]=faker.first_name()
yo["last_name"]=faker.last_name()
#añadir nombre usuario @usuario
string="@"+yo["name"]+yo["last_name"]
string=string.replace(" ","")
yo["@usuario"]=string
yo["age"]=random.randint(18, 45) #EDAD
yo["lat"]=(lat_min+lat_max)/2
yo["lon"]=(lon_min+lon_max)/2

position_yo = (yo["lat"],yo["lon"])

######
# CÓDIGO para crear tabla "Yo" en BigQuery e insertar los datos. Esa tabla no se actualizará.
#####  


# Función que genera los usuarios y define quién es amigo
users={}

def initiate_data():
    global users
    #Genera usuarios
    for i in range(0,USERS_TOTAL):
        user={}
        user["id"]=faker.ssn()
        user["name"]=faker.first_name()
        user["last_name"]=faker.last_name()
        #añadir nombre usuario @usuario
        string="@"+user["name"]+user["last_name"]
        string=string.replace(" ","")
        user["@usuario"]=string
        user["transport"]=random.choice(vehicles)
        user["age"]=random.randint(18, 45) #EDAD
        #¿Qué usuarios serán amigos? El 10% aleatorio. Amigo=1 o Amigo=0
        if random.randint(0, 10)<=9:
            user["friends"]=0
        else:
            user["friends"]=1
        user["lat"]=random.uniform(lat_min, lat_max)
        user["lon"]=random.uniform(lon_min, lon_max)
        users[user["id"]]=user   
   

    print("DATA GENERATED")        

# Calcula las coordenadas del movimiento de las personas

def generate_step():
    global users
    if len(users)>0: #Si los usuarios ya están generados
        print("STEP")
        for element in users.items():
            lat=users[element[0]]["lat"]
            lon=users[element[0]]["lon"]
            # Modifica las coordenadas en cada iteración para simular movimiento
            users[element[0]]["lon"]=lon+random.uniform(0.0001, 0.0005)*multiplo
            users[element[0]]["lat"]=lat+random.uniform(0.0001, 0.0005)*multiplo
            # Si se salen del cuadrante, respawn aleatorio
            if lat>lat_max or lat<lat_min:
                users[element[0]]["lat"]=random.uniform(lat_min, lat_max)
                users[element[0]]["transport"]=random.choice(vehicles)
            if lon>lon_max or lon<lon_min:
                users[element[0]]["lon"]=random.uniform(lon_min, lon_max)
    else: #Si los usuarios no están generados, los genera
        initiate_data()
    return users

# Bucle que modifica las coordenadas
amigoscerca=[]

while True:
    users_generated=generate_step()
    
    for user in users_generated:
        
        if users_generated[user]["friends"] == 1:
            position_user = (users_generated[user]["lat"],users_generated[user]["lon"])
            
            if distance.distance(position_yo, position_user,).km <= radius and users_generated[user]["transport"] == "Walking":
                amigoscerca.append({"@usuario" : users_generated[user]["@usuario"]})
    ######
    # CÓDIGO PARA ACTUALIZAR LOS DICTS EN BIGQUERY EN CADA STEP
    # Tiene que estar identado al mismo nivel que el for: tiene que ejecutarse cuando acaba el for
    # Hay que actualizar "amigoscerca"; y en "users_generated" sólo las coordenadas
    #####    
    time.sleep(refresh) #Tiempo de descanso después de insertarlo todo
    amigoscerca=[] # Renueva para que los que ya no estén cerca no salgan



