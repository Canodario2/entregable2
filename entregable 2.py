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

#usamos la funcion y guardamos el json en datos_api
datos_api = obtener_datos(url_de_api)

loc_cen = datos_api["localidades-censales"]

print(loc_cen)


#conectamos con la base de datos
conn = ps.connect(host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
                  port=5439,
                  dbname='data-engineer-database',
                  user='canodario1991_coderhouse',
                  password='svoU6a0313')

#creamos el cursor para poder ejecutar cualquier tipo de queries
cursor = conn.cursor()

#insertamos los valores a la tabla creada en redshift
for i in loc_cen:
    insertsql = "INSERT INTO censados (provincia_id, funcion,nombre,categoria,coor_lat,coor_long,municipio_id,departamento_id,id,fuente) VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"
    values = (loc_cen["provinca_id"],loc_cen["funcion"],loc_cen["nombre"],loc_cen["categoria"],
              loc_cen["coor_lat"],loc_cen["coor_long"],loc_cen["municipio_id"],loc_cen["departamento_id"],
              loc_cen["id"],loc_cen["fuente"])
    cursor.execute(insertsql,values)

#Guardar consulta en variable
query = 'SELECT * FROM censados'

## Leer la querry y guardarla como dataframe en una variable
df = pd.read_sql_query(query,conn)

print(df)

conn.commit
cursor.close
conn.close
