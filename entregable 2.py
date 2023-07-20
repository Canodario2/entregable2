import pandas as pd
import psycopg2 as ps #para conectar con la base de datos
import requests #para hacer solicitud de la api
import json #para reconocer un formato json

#hacemos una funcion para guardar en una lista el json extraido
def obtener_datos(url):
    respuesta = requests.get(url)
    if respuesta.status_code == 200:
        datos = json.loads(respuesta.text)
        return datos
    else:
        print("Error al obtener los datos de la API")
        return None

#guardamos el url para usarlo en la funcion    
url_de_api = "https://infra.datos.gob.ar/catalog/modernizacion/dataset/7/distribution/7.27/download/localidades-censales.json"

#Llamar a la funcion y colocarlo en una variable
datos_api = obtener_datos(url_de_api)

#Me traigo los censados y los guardo en una variable
censados = datos_api["localidades-censales"]

#Filtro los censados quitando los valores null del municipio
censados_filt = []
for i in censados:
    if i["municipio"]["id"] is not None:
        censados_filt.append(i)

#Me traigo las provincias y las guardo en variable
p = []
for i in censados:
    p.append(i["provincia"])

#Le quito los duplicados
provincias = []
for i in p:
    if i not in provincias:
        provincias.append(i)

#Me traigo los municipios y las guardo en variable
m = []
for i in censados:
    m.append(i["municipio"])
    
#Le quito los duplicados
municipios = []
for i in m:
    if i not in municipios:
        municipios.append(i)

#Remuevo este elemento por que hay un error y no figuran datos        
municipios.remove(municipios[2])

#Me traigo los departamentos y las guardo en variable
d = []
for i in censados:
    d.append(i["departamento"])

#Le quito los duplicados
departamentos = []
for i in d:
    if i not in departamentos:
        departamentos.append(i)

#Remuevo este elemento por que hay un error y no figuran datos        
departamentos.remove(departamentos[18])

#Las variables (Tablas) son censados , provincias, municipios y departamentos
#print(censados_filtr)
#print(provincias)
#print(municipios)
#print(departamentos)


#conectamos con la base de datos
conn = ps.connect(host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
                  port=5439,
                  dbname='data-engineer-database',
                  user='canodario1991_coderhouse',
                  password='svoU6a0313')

#creamos el cursor para poder ejecutar cualquier tipo de queries
cursor = conn.cursor()

#insertamos los valores a la tabla creada en redshift
for i in provincias:
    insertsql1 = "INSERT INTO provincia (nombre, id) VALUES (%s, %s)"
    values1 = (i["nombre"],i["id"])
    cursor.execute(insertsql1,values1)

for i in municipios:
    insertsql2 = "INSERT INTO municipio (nombre, id) VALUES (%s, %s)"
    values2 = (i["nombre"],i["id"])
    cursor.execute(insertsql2,values2)

for i in departamentos:
    insertsql3 = "INSERT INTO departamento (nombre, id) VALUES (%s, %s)"
    values3 = (i["nombre"],i["id"])
    cursor.execute(insertsql3,values3)

for i in censados_filt:
    insertsql4 = "INSERT INTO censados (provincia_id, funcion,nombre,categoria,coord_lat,coord_long,municipio_id,departamento_id,id,fuente) VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"
    values4 = (i["provincia"]["id"],i["funcion"],i["nombre"],i["categoria"],i["centroide"]["lat"],i["centroide"]["lon"],i["municipio"]["id"],i["departamento"]["id"],i["id"],i["fuente"])
    cursor.execute(insertsql4,values4)  

#Guardar consulta en variable
queryc = 'SELECT * FROM censados'
queryp = 'SELECT * FROM provincia'
querym = 'SELECT * FROM municipio'
queryd = 'SELECT * FROM departamento'

## Leer la querry y guardarla como dataframe en una variable
df_c= pd.read_sql_query(queryc,conn)
df_p = pd.read_sql_query(queryp,conn)
df_m = pd.read_sql_query(querym,conn)
df_d = pd.read_sql_query(queryd,conn)

#Muestro los dataframe
print(df_c)
print(df_p)
print(df_m)
print(df_d)

#Cierro cursor y conector
conn.commit
cursor.close
conn.close