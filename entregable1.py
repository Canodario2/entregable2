import pandas as pd
import requests
import json

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

#Remuevo este elemento que figura como null          
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
#Remuevo este elemento que figura como null        
departamentos.remove(departamentos[18])

# Para ver que no hayan nulos
'''indices_nulos = [indice for indice, elemento in enumerate(censados_filt) if elemento["municipio"]["id"] is None]
print(indices_nulos)'''

print(censados_filt)

