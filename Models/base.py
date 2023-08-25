from datetime import datetime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import MetaData, create_engine
from sqlalchemy import Column, DateTime, Float, Integer, String
import pyodbc



from Util import Config
#Cargo desde  configuration.ini los parametros para conectarme a la base de datos 
config=Config.readIni()   

'''
direccion_servidor =config['PRODUCTION_FROM_PERSONAL']['direccion_servidor']
nombre_bd=config['PRODUCTION_FROM_PERSONAL']['nombre_bd']
nombre_usuario =config['PRODUCTION_FROM_PERSONAL']['nombre_usuario']
password =config['PRODUCTION_FROM_PERSONAL']['password']
driver=config['PRODUCTION_FROM_PERSONAL']['driver']
'''

direccion_servidor =config['DEVELOPMENT_WITH_USERTEC']['direccion_servidor']
nombre_bd=config['DEVELOPMENT_WITH_USERTEC']['nombre_bd']
nombre_usuario =config['DEVELOPMENT_WITH_USERTEC']['nombre_usuario']
password =config['DEVELOPMENT_WITH_USERTEC']['password']
driver=config['DEVELOPMENT_WITH_USERTEC']['driver']

'''
direccion_servidor =config['PRODUCTION_FROM_HOME']['direccion_servidor']
nombre_bd=config['PRODUCTION_FROM_HOME']['nombre_bd']
nombre_usuario =config['PRODUCTION_FROM_HOME']['nombre_usuario']
password =config['PRODUCTION_FROM_HOME']['password']
driver=config['PRODUCTION_FROM_HOME']['driver']
'''

'''
#La configuración de este engine solo funciona para la conexion con autenticación de windows con la BD de Desarrollo, NO con la de producción
conn_str = 'DRIVER={2};SERVER={0};DATABASE={1};Trusted_Connection=yes;'.format(direccion_servidor,nombre_bd,driver)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")
'''




# creo el engine el cual me permitira acceder a la base de datos y realizar la comunicacion que necesita el ORM con la base de datos
#Solo funciona con Sql server authentication (pasando usuario y pass)
engine=create_engine('mssql+pyodbc://{0}:{1}@{2}/{3}?driver={4}'.format(nombre_usuario,password,direccion_servidor,nombre_bd,driver))


#La configuración de ste engine no funciona sin pasar usuario y pass
#engine=create_engine('mssql+pyodbc://{0}/{1}?driver={2}'.format(direccion_servidor,nombre_bd,driver))

# la metadata son datos utilizados para decirle a la base que caracteristicas necesito que utilice de la base de datos
metadata = MetaData()

#Para imprimir los drivers que tenemos en el equipo donde se ejecute el código
#print(pyodbc.drivers())
#En este caso estamos indicandole que solo queremos que la base nos mapee las siguientes tablas de la base de datos.
metadata.reflect(engine, only=['LOGINFO','TmpPremisasOff_HG_Aprov','TmpParamAuditPremOff_HG_Aprov','DimParametros2','FacPremisasOff_HG_Aprov','ParamAuditPremOff_HG_Aprov','DimCampos'])
# Se utiliza la funcion automap_base para hacer un mapeo de la base de datos trayendo todos los campos de las tablas que se le indicaron
#anteriormente en la metadata
Base = automap_base(metadata=metadata)


#Preparo la conexion y el mapeado
Base.prepare()


#instancio la clase de los objetos de la BD en un nombre generico para poder parametrizar los paquete con los cambios 
#centralizados para hacerlos en el minimo de módulos posible
TmpTable = Base.classes.TmpPremisasOff_HG_Aprov
strTmpTable = 'TmpPremisasOff_HG_Aprov'
TmpTableParam = Base.classes.TmpParamAuditPremOff_HG_Aprov
strTmpTableParam = 'TmpParamAuditPremOff_HG_Aprov'

FactTable = Base.classes.FacPremisasOff_HG_Aprov
strFactTable = 'facPremisasOff_HG_Aprov'
TableParam = Base.classes.ParamAuditPremOff_HG_Aprov
strTableParam = 'ParamAuditPremOff_HG_Aprov'

strDimParametros2 = 'DimParametros2'

#Todas las tablas mapeadas se pueden acceder utilizando su nombre segun este en la base de datos 
#siguiendo el patron Base.classes.tableName como por ejemplo
# DimPuntosDeVenta = Base.classes.DimPuntosDeVenta

#La session es la herramienta con la cual vamos a realizar las inserciones y la ejecucion en base de datos asi que se define desde aca es importada
#donde sea necesaria.
session = Session(engine)






