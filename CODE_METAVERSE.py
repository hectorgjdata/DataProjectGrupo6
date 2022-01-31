####################################################################
####################################################################

#NOTA IMPORTANTE

# Genera dataframes y los exporta a localhost:8084

# Para volver a runear hay que desactivar el puerto desde la consola (linea 21)


#LO QUE FALTA

# Falta incluir el dataframe de userscerca y dibujar una tabla o similar que se actualice

# Falta incluir el dataframe de locales y dibujar una tabla o similar que se actualice

# Los dataframes están en codeMETAv3

# Por último mejorar el formato en lo que se pueda



####################################################################
####################################################################

# kill port
# netstat -ano | findstr :8084
# taskkill /PID 12384 /F

#pip install Faker
#pip install keyboard
#pip install geopy

#LIBRARIES
from geopy import distance
from faker import Faker
import time
import random
import pandas as pd

import requests
import json
import pandas as pd
from bokeh.models import HoverTool,LabelSet,ColumnDataSource
from bokeh.tile_providers import get_provider, STAMEN_TERRAIN
import numpy as np
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, TableColumn, DataTable
from bokeh.io import show
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.layouts import gridplot

from bokeh.models import HTMLTemplateFormatter

#########
#AJUSTES#
#########

#Seleccionar tiempo de refresh (s)
refresh=2

#Ajustar velocidad
multiplo=0.2

#Radio (m)
radius=500

#Usuarios totales
USERS_TOTAL=100

#Usuarios totales
LOCALES_TOTAL=10

# Porcentaje de amigos (%)
por=10

#Definición cuadrante del mapa
lat_min=39.4505101
lat_max=39.4939737
lon_min=-0.4
lon_max=-0.341


vehicles=["Bike","Train","Car", "Walking"]

faker = Faker('es_ES')

####################################################################
####################################################################

#FUNCTION TO CONVERT GCS WGS84 TO WEB MERCATOR
#DATAFRAME
def wgs84_to_web_mercator(df, lon="lon", lat="lat"):
    k = 6378137
    df["x"] = df[lon] * (k * np.pi/180.0)
    df["y"] = np.log(np.tan((90 + df[lat]) * np.pi/360.0)) * k
    return df

#POINT
def wgs84_web_mercator_point(lon,lat):
    k = 6378137
    x= lon * (k * np.pi/180.0)
    y= np.log(np.tan((90 + lat) * np.pi/360.0)) * k
    return x,y

#COORDINATE CONVERSION
xy_min=wgs84_web_mercator_point(lon_min,lat_min)
xy_max=wgs84_web_mercator_point(lon_max,lat_max)

#COORDINATE RANGE IN WEB MERCATOR
x_range,y_range=([xy_min[0],xy_max[0]], [xy_min[1],xy_max[1]])

#REST API QUERY
user_name=''
password=''
url_data='https://'+user_name+':'+password+'@opensky-network.org/api/states/all?'+'lamin='+str(lat_min)+'&lomin='+str(lon_min)+'&lamax='+str(lat_max)+'&lomax='+str(lon_max)



#USER TRACKING FUNCTION
def user_tracking(doc):
        
    # YO Position
    yo_source = ColumnDataSource({'lat':[],'lon':[],'x':[],'y':[],'usuario':[],'match':[],'distance':[],'url':[]})
    
    yo = pd.DataFrame([{'lon': (lon_min+lon_max)/2, 'lat': (lat_min+lat_max)/2,'usuario': 'Yo','match': 0,'distance':0,'url':'https:...'}])  
    yo=wgs84_to_web_mercator(yo)
    
    n_rollyo=len(yo.index)
    yo_source.stream(yo.to_dict(orient='list'),n_rollyo)
    
    # LOCALES
    # El nombre del local es 'usuario' para que funcione el hover del ratón en el mapa. Y el 'match' es la valoración del local
    locales_source = ColumnDataSource({
        'usuario':[],'lat':[],'lon':[],'distance':[],'match':[],'x':[],'y':[],'url':[]})
    
    locales = pd.DataFrame(columns=('usuario','lat','lon','distance','match','url'))

    for i in range(LOCALES_TOTAL):
        data = [faker.company(),random.uniform(lat_min, lat_max),random.uniform(lon_min, lon_max),0,str(random.randint(0, 5))+'/5','https:...']
        position_local=(data[1],data[2])
        distance1=int(distance.distance((yo.iat[0,1],yo.iat[0,0]), position_local,).m)
        data[3]=distance1
        locales.loc[i] = [item for item in data]
        
    locales=wgs84_to_web_mercator(locales)
    
    n_rolllocales=len(locales.index)
    locales_source.stream(locales.to_dict(orient='list'),n_rolllocales)
    
    # Usuarios y amigos
    
    users_source = ColumnDataSource({'usuario':[],'lat':[],'lon':[],'distance':[],'friends':[],'transport':[],'match':[],'x':[],'y':[],'url':[]})

    
    users = pd.DataFrame(columns=('usuario','lat','lon','distance','friends','transport','match','x','y','url'))
    
    for i in range(USERS_TOTAL):
        name=faker.first_name()
        last_name=faker.last_name()
        username="@"+name.replace(" ","")+last_name.replace(" ","")
        data = [username,random.uniform(lat_min, lat_max),random.uniform(lon_min, lon_max),0,0,random.choice(vehicles),str(random.randint(0, 100))+'%',0,0,'https:...']

        if random.randint(0, 10)<=(10-por/10):
            data[4]=0
        else:
            data[4]=1
        users.loc[i] = [item for item in data]
        
    # AMIGOSCERCA
    
    amigoscerca_source = ColumnDataSource({'usuario':[]})
    
    # UPDATING USERS DATA
    def update():
        
        # AMIGOSCERCA
    
        amigoscerca = pd.DataFrame(columns = ['usuario'])
        
        for i in range(USERS_TOTAL):
            lat=users.iat[i,1]
            lon=users.iat[i,2]
            # Modifica las coordenadas en cada iteración para simular movimiento según el tipo de transporte
            if users.iat[i,5] == "Walking":
                users.iat[i,2]=lon+random.uniform(-0.0005, 0.0005)*multiplo
                users.iat[i,1]=lat+random.uniform(-0.0005, 0.0005)*multiplo
            if users.iat[i,5] == "Bike":
                users.iat[i,2]=lon+random.uniform(-0.0005, 0.0005)*multiplo*2
                users.iat[i,1]=lat+random.uniform(-0.0005, 0.0005)*multiplo*2
            if users.iat[i,5] == "Car":
                users.iat[i,2]=lon+random.uniform(-0.0005, 0.0005)*multiplo*3
                users.iat[i,1]=lat+random.uniform(-0.0005, 0.0005)*multiplo*3
            if users.iat[i,5] == "Train":
                users.iat[i,2]=lon+random.uniform(-0.0005, 0.0005)*multiplo*4
                users.iat[i,1]=lat+random.uniform(-0.0005, 0.0005)*multiplo*4
            # Si se salen del cuadrante, respawn aleatorio
            if lat>lat_max or lat<lat_min:
                users.iat[i,2]=random.uniform(lat_min, lat_max)
                users.iat[i,5]=random.choice(vehicles)
            if lon>lon_max or lon<lon_min:
                users.iat[i,2]=random.uniform(lon_min, lon_max)
                users.iat[i,5]=random.choice(vehicles)
            #DISTANCIA QUE SE ACTUALIZA
            position_user=(lat,lon)
            users.iat[i,3]=int(distance.distance(((lat_min+lat_max)/2,(lon_min+lon_max)/2), position_user,).m)
            
            if users.iat[i,3] <= radius:  #if users.iat[i,4] == 1 and users.iat[i,5] == 'Walking' and users.iat[1,3] <= radius:
                add=pd.DataFrame([users.iat[i,0]], columns = ['usuario'])
                amigoscerca = pd.concat([amigoscerca, add], ignore_index=True)
        
        wgs84_to_web_mercator(users)
        
        # CONVERT TO BOKEH DATASOURCE AND STREAMING
        n_roll=len(users.index)
        users_source.stream(users.to_dict(orient='list'),n_roll)
        
        n_rollamigoscerca=len(amigoscerca.index)
        amigoscerca_source.stream(amigoscerca.to_dict(orient='list'),n_rollamigoscerca)
        
        
    #CALLBACK UPATE IN AN INTERVAL
    doc.add_periodic_callback(update, refresh*1000) #5000 ms/10000 ms for registered user
    
    #PLOT USERS POSITION
    p=figure(x_range=x_range,y_range=y_range,x_axis_type='mercator',y_axis_type='mercator',sizing_mode='fixed',plot_height=510,plot_width=600)
    tile_prov=get_provider(STAMEN_TERRAIN)
    p.add_tile(tile_prov,level='image')
    p.image_url(url='url', x='x', y='y',source=users_source,anchor='center',h_units='screen',w_units='screen',w=40,h=40)
    p.circle('x','y',source=users_source,fill_color='#0057E9',hover_color='white',size=10,fill_alpha=1,line_width=0)
    p.circle('x','y',source=yo_source,fill_color='#FF00BD',hover_color='white',size=15,fill_alpha=1,line_width=0)
    p.square_dot('x','y',source=locales_source,fill_color='#F2CA19',hover_color='white',size=12,fill_alpha=1,line_width=0)

    #ADD HOVER TOOL AND LABEL
    my_hover=HoverTool()
    my_hover.tooltips=[('Name','@usuario'),('Match','@match'),('Distance (m)','@distance')]
    labels = LabelSet(x='x', y='y', text='', level='glyph', x_offset=0, y_offset=0, source=users_source, render_mode='canvas',background_fill_color='#0057E9',text_font_size="1pt")
    p.add_tools(my_hover)
    p.add_layout(labels)
    
    #LOCALES TABLE
    
    columnslocales = [
            TableColumn(field='usuario', title='Local'),
            TableColumn(field='distance', title='Distance (m)'),
            TableColumn(field='match', title='Rate')
            ]

    q = DataTable(source=locales_source, columns=columnslocales, width=350, height=280)

    # AMIGOSCERCA TABLE
    columnsamigoscerca = [TableColumn(field='usuario', title='USER')]

    r = DataTable(source=amigoscerca_source, columns=columnsamigoscerca, width=350, height=280)
    
    s = gridplot([[q],[r]])
    
    # TOTAL DASHBOARD
    
    doc.title='MEETAVERSE'
    t = gridplot([[p, s]], toolbar_location='right')
    doc.add_root(t)
# SERVER CODE
apps = {'/': Application(FunctionHandler(user_tracking))}
server = Server(apps, port=8084) #define an unused port
server.start()
