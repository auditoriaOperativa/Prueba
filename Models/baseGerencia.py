from datetime import datetime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import MetaData, create_engine
from sqlalchemy import Column, DateTime, Float, Integer, String

from Util import Config
#Cargo desde  configuration.ini los parametros para conectarme a la base de datos 
config=Config.readIni()   
direccion_servidor =config['GESTIONOPERATIVA']['direccion_servidor']
nombre_bd=config['GESTIONOPERATIVA']['nombre_bd']
nombre_usuario =config['GESTIONOPERATIVA']['nombre_usuario']
password =config['GESTIONOPERATIVA']['password']
driver=config['GESTIONOPERATIVA']['driver']

# creo el engine el cual me permitira acceder a la base de datos y realizar la comunicacion que necesita el ORM con la base de datos
engine=create_engine('mssql+pyodbc://{0}:{1}@{2}/{3}?driver={4}'.format(nombre_usuario,password,direccion_servidor,nombre_bd,driver))
# engine=create_engine('mssql+pyodbc://{0}/{1}?driver={2}'.format(direccion_servidor,nombre_bd,driver))
# la metadata son datos utilizados para decirle a la base que caracteristicas necesito que utilice de la base de datos
metadata = MetaData()
#En este caso estamos indicandole que solo queremos que la base nos mapee las siguientes tablas de la base de datos.
#metadata.reflect(engine, only=['Hoja1'])
# Se utiliza la funcion automap_base para hacer un mapeo de la base de datos trayendo todos los campos de las tablas que se le indicaron
#anteriormente en la metadata
Base = automap_base(metadata=metadata)




#Preparo la conexion y el mapeado
Base.prepare()

#Todas las tablas mapeadas se pueden acceder utilizando su nombre segun este en la base de datos 
#siguiendo el patron Base.classes.tableName como por ejemplo
# DimPuntosDeVenta = Base.classes.DimPuntosDeVenta

#La session es la herramienta con la cual vamos a realizar las inserciones y la ejecucion en base de datos asi que se define desde aca es importada
#donde sea necesaria.
session = Session(engine)





