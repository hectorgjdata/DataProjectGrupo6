#LIBRARIES
#Data Generator
from bdb import GENERATOR_AND_COROUTINE_FLAGS
from faker import Faker
import keyboard
import time
import random
#Dataflow
import argparse
import json
import logging
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import (PipelineOptions, StandardOptions)
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools
import datetime

####################################################################
####################################################################

#NOTA IMPORTANTE

#    - El programa genera un diccionario "Yo" en el que están los atributos de nuestro usuario
#    - Genera un diccionario con 100 lineas de otros usuarios, el cuarto elemento incluye otro diccionario con todos los atributos.
#    - Sólo sabemos qué usuario es amigo nuestro, entre ellos no lo sabemos y nos da igual
#    - El bucle while actualiza en el diccionario SOLO el valor de las coordenadas de cada usuario (el nuestro no)

#LO QUE FALTA: 

#   - Hacer lo de la distancia. Mi idea es poner un atributo booleano para cada persona que esté cerca y sea tu amigo 
#    -   Ver cómo conectarse con bigquery y en qué formato deben estar las tablas (dataframe,dict,json...), es posible que no haga falta Dataflow


####################################################################
####################################################################




####################################################################
# DATA GENERATOR
####################################################################

faker = Faker('es_ES')

#Usuarios totales
USERS_TOTAL=100
users={}

#Definición cuadrante del mapa
lat_min=39.4
lat_max=39.5
lon_min=-0.3
lon_max=-0.4

#Medio de transporte
vehicles=["Bike","Train","Car","Walking"]

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


# Función que genera los usuarios y define quién es amigo

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
        user["age"]=random.randint(18, 45) #EDAD
        #¿Qué usuarios serán amigos? El 10% aleatorio. Amigo=1 o Amigo=0
        if random.randint(0, 10)<=9:
            user["friends"]=0
        else:
            user["friends"]=1

        user["position"]={"lat":random.uniform(lat_min, lat_max),"lon":random.uniform(lon_min, lon_max)}
        user["transport"]=random.choice(vehicles)
        users[user["id"]]=user   
   

    print("DATA GENERATED")        

# Calcula las coordenadas del movimiento de las personas

def generate_step():
    global users
    if len(users)>0: #Si los usuarios ya están generados
        print("STEP")
        for element in users.items():
            lat=users[element[0]]["position"]["lat"]
            lon=users[element[0]]["position"]["lon"]
            # Modifica las coordenadas en cada iteración para simular movimiento
            # PODEMOS MODIFICAR LA VELOCIDAD SEGÚN EL MEDIO DE TRANSPORTE CON BUCLE IF, sino lo QUITARÍA DIRECTAMENTE
            users[element[0]]["position"]["lon"]=lon+random.uniform(0.001, 0.005)
            users[element[0]]["position"]["lat"]=lat+random.uniform(0.001, 0.005)
            # Si se salen del cuadrante, respawn aleatorio con medio de transporte distinto
            if lat>lat_max or lat<lat_min:
                users[element[0]]["position"]["lat"]=random.uniform(lat_min, lat_max)
                users[element[0]]["transport"]=random.choice(vehicles)
            if lon>lon_max or lon<lon_min:
                users[element[0]]["position"]["lon"]=random.uniform(lon_min, lon_max)
                users[element[0]]["transport"]=random.choice(vehicles)
    else: #Si los usuarios no están generados, los genera
        initiate_data()
    return users



# Bucle constante que modifica las coordenadas

#Seleccionar tiempo de refresh (s)
refresh=2

while True:
    try:  
        #Para salir del bucle
        if keyboard.is_pressed('q'):  # if key 'q' is pressed 
            print('You Exited the data generator')
            break  
        else:
            users_generated=generate_step()
            # Place your code here  
            #Probablemente aquí hará falta poner la orden de que escriba en bigquery
            #Aquí se calculará la distancia entre Yo y amigos para generar un atributo booleano que diga si estás en el circulo o no
            print("code")
            # End Place for code
            time.sleep(refresh)
    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")
        break  









####################################################################
# DATAFLOW CODE
####################################################################


#ParseJson Function
#Get data from PubSub and parse them
def parse_json_message(message):
    #Mapping message from PubSub
    #DecodePubSub message in order to deal with
    pubsubmessage = message.data.decode('utf-8')
    #Get messages attributes
    attributes = message.attributes
    #Print through console and check that everything is fine.
    logging.info("Receiving message from PubSub:%s", message)
    logging.info("with attributes: %s", attributes)
    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)
    #Add Processing Time (new column)
    row["processingTime"] = str(datetime.datetime.now())
    #Return function
    return row
class add_processing_time(beam.DoFn):
    def process(self, element):
        window_start = str(datetime.datetime.now())
        output_data = {'aggTemperature': element, 'processingTime': window_start}
        output_json = json.dumps(output_data)
        yield output_json.encode('utf-8')
class agg_temperature(beam.DoFn):
    def process(self, element):
        temp = element['temperature']
        yield temp
#Create Beam pipeline
def edemData(output_table):
    #Load schema from BigQuery/schemas folder
    with open(f"schemas/{output_table}.json") as file:
        input_schema = json.load(file)
    #Declare bigquery schema
    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))
    #Create pipeline
    #First of all, we set the pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True)
    with beam.Pipeline(options=options) as p:
        #Part01: we create pipeline from PubSub to BigQuery
        data = (
            #Read messages from PubSub
            p | "Read messages from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/clase-edem/topics/iottobigquery/{output_table}-sub", with_attributes=True)
            #Parse JSON messages with Map Function and adding Processing timestamp
              | "Parse JSON messages" >> beam.Map(parse_json_message)
        )
        #Part02: Write proccessing message to their appropiate sink
        #Data to Bigquery
        (data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table = f"clase-EDEM:datasetID.{output_table}",
            schema = schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, #por eso no hace falta crear la tabla en bigquery
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))
        #Part03: Count temperature per minute and put that data into PubSub
        #Create a fixed window (1 min duration)
        (data 
            | "Get temperature value" >> beam.ParDo(agg_temperature())
            | "WindowByMinute" >> beam.WindowInto(window.FixedWindows(60))
            | "MeanByWindow" >> beam.CombineGlobally(MeanCombineFn()).without_defaults()
            | "Add Window ProcessingTime" >> beam.ParDo(add_processing_time())
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic="projects/clase-edem/topics/iottobigquery", with_attributes=False)
        )
if _name_ == '_main_':
    logging.getLogger().setLevel(logging.INFO)
    edemData("iotToBigQuery")