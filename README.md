# ELT_sqlite_python
#Esta es una ETL de sqlite y python

#Importamos los paquetes a usar
import pandas as pd
import json
import requests
from pandas import json_normalize

#Creación de conexión inicial con la API

res = requests.get('http://api.tvmaze.com/schedule/web?date=2020-12-01')
serie = json.loads(res._content)
dx = json_normalize(serie)

#Creación de bucle para absorber los días restantes del mes de la API

dia = range(2,32) #toma todos los días del mes
for n in dia:
    

    #res = requests.get('http://api.tvmaze.com/schedule/web?date=2020-12-30')# Aquí tegno que hacer un cilco for del 1 al 30
    if len(str(n))==1:
        res = requests.get('http://api.tvmaze.com/schedule/web?date=2020-12-'+'0'+str(n))
        print('http://api.tvmaze.com/schedule/web?date=2020-12-'+'0'+str(n))
    else:
        res = requests.get('http://api.tvmaze.com/schedule/web?date=2020-12-'+str(n))
        print('http://api.tvmaze.com/schedule/web?date=2020-12-'+str(n))

    serie = json.loads(res._content) 
    df = json_normalize(serie)
    dx = dx.append(df,ignore_index=False)
    
#Se renombran las columnas del dataframe resultante para eviar problemas a la hora del cargue en la db

dx.rename(columns={
'id':'id',
'url':'url',
'name':'name',
'season':'season',
'number':'number',
'type':'type',
'airdate':'airdate',
'airtime':'airtime',
'airstamp':'airstamp',
'runtime':'runtime',
'image':'image',
'summary':'summary',
'rating.average':'rating_average',
'_links.self.href':'_links_self_href',
'_embedded.show.id':'_embedded_show_id',
'_embedded.show.url':'_embedded_show_url',
'_embedded.show.name':'_embedded_show_name',
'_embedded.show.type':'_embedded_show_type',
'_embedded.show.language':'_embedded_show_language',
'_embedded.show.genres':'_embedded_show_genres',
'_embedded.show.status':'_embedded_show_status',
'_embedded.show.runtime':'_embedded_show_runtime',
'_embedded.show.averageRuntime':'_embedded_show_averageRuntime',
'_embedded.show.premiered':'_embedded_show_premiered',
'_embedded.show.ended':'_embedded_show_ended',
'_embedded.show.officialSite':'_embedded_show_officialSite',
'_embedded.show.schedule.time':'_embedded_show_schedule_time',
'_embedded.show.schedule.days':'_embedded_show_schedule_days',
'_embedded.show.rating.average':'_embedded_show_rating_average',
'_embedded.show.weight':'_embedded_show_weight',
'_embedded.show.network':'_embedded_show_network',
'_embedded.show.webChannel.id':'_embedded_show_webChannel_id',
'_embedded.show.webChannel.name':'_embedded_show_webChannel_name',
'_embedded.show.webChannel.country.name':'_embedded_show_webChannel_country_name',
'_embedded.show.webChannel.country.code':'_embedded_show_webChannel_country_code',
'_embedded.show.webChannel.country.timezone':'_embedded_show_webChannel_country_timezone',
'_embedded.show.webChannel.officialSite':'_embedded_show_webChannel_officialSite',
'_embedded.show.dvdCountry':'_embedded_show_dvdCountry',
'_embedded.show.externals.tvrage':'_embedded_show_externals_tvrage',
'_embedded.show.externals.thetvdb':'_embedded_show_externals_thetvdb',
'_embedded.show.externals.imdb':'_embedded_show_externals_imdb',
'_embedded.show.image.medium':'_embedded_show_image_medium',
'_embedded.show.image.original':'_embedded_show_image_original',
'_embedded.show.summary':'_embedded_show_summary',
'_embedded.show.updated':'_embedded_show_updated',
'_embedded.show._links.self.href':'_embedded_show__links_self_href',
'_embedded.show._links.previousepisode.href':'_embedded_show__links_previousepisode_href',
'image.medium':'image_medium',
'image.original':'image_original',
'_embedded.show.network.id':'_embedded_show_network_id',
'_embedded.show.network.name':'_embedded_show_network_name',
'_embedded.show.network.country.name':'_embedded_show_network_country_name',
'_embedded.show.network.country.code':'_embedded_show_network_country_code',
'_embedded.show.network.country.timezone':'_embedded_show_network_country_timezone',
'_embedded.show.network.officialSite':'_embedded_show_network_officialSite',
'_embedded.show._links.nextepisode.href':'_embedded_show__links_nextepisode_href',
'_embedded.show.webChannel':'_embedded_show_webChannel',
'_embedded.show.image':'_embedded_show_image',
'_embedded.show.webChannel.country':'_embedded_show_webChannel_country',
'_embedded.show.dvdCountry.name':'_embedded_show_dvdCountry_name',
'_embedded.show.dvdCountry.code':'_embedded_show_dvdCountry_code',
'_embedded.show.dvdCountry.timezone':'_embedded_show_dvdCountry_timezone'},inplace = True)
dx = dx.drop(['_embedded_show_genres', '_embedded_show_schedule_days'], axis=1)

#Se genera una conexión son sqlite3

import sqlite3
conn=sqlite3.connect('mydb.db')

Se crea una tabla y se depositan el dataframe resultante.
dx.to_sql("Serie_dataset",conn,if_exists="replace")
