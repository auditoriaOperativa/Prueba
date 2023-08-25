# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from datetime import datetime
import os
import shutil
from Modules import log

from distutils.dir_util import copy_tree
from shutil import copyfile


def mesAnterior():
    fecha= datetime.today()
    primerDiaMes=fecha.replace(day=1)
    mesAnt= primerDiaMes - timedelta(days=1)
    return mesAnt
    
def today():
    return datetime.today()

def yesterday():
        return datetime.today()- timedelta(days = 1)

def monthName(month):
    diccionario = {1:'Enero', 2:'Febrero', 3:'Marzo', 4:'Abril', 5:'Mayo', 6:'Junio', 7:'Julio', 8:'Agosto', 9:'Septiembre', 10:'Octubre', 11:'Noviembre', 12:'Diciembre'}
    return diccionario.get(month)


def dateFromString(date:str)->datetime:
     return datetime.strptime(date,"%Y-%m-%d")

def datetime2ole(date:datetime):
    OLE_TIME_ZERO = datetime(1899, 12, 30)
    delta = date - OLE_TIME_ZERO
    return float(delta.days) + (float(delta.seconds) / 86400)


def CopyFolderFromTo(fromPath:str,toPath:str,Source:str):
    try:
        
        ###### Si no existen los Paths los crea 
        if not os.path.exists(fromPath):
            #os.makedirs(fromPath)
            raise Exception('Source not found')
        if not os.path.exists(toPath):
            os.makedirs(toPath)
        
        
        ###### Copia la carpeta en la ruta especificada
        copy_tree(fromPath,toPath)
                
        log.insertLogToDB('Success',Source,'Success execution in CopyFolderFromTo')    
    except Exception as e:
        log.insertLogToDB(str(e),'Source','Failed execution in CopyFolderFromTo')    
        raise
###### Metodo encargado de mover archivos de un lugar a otro
def CopyFileFromTo(fromPath:str,toPath:str,Source:str):
    try:
        
        ###### Si no existen los Paths los crea 
        if not os.path.exists(fromPath):
            os.makedirs(fromPath)
        if not os.path.exists(toPath):
            os.makedirs(toPath)
        
        
        ###### Copia la carpeta en la ruta especificada
        shutil.copy(fromPath,toPath)
                
        
        log.insertLogToDB('Success','Delete Folder','Success execution in CopyFileFromTo')    
    except Exception as e:
        log.insertLogToDB(str(e),'Delete Folder','Failed execution in CopyFileFromTo')    
        raise





def deleteFolder(path:str):
    try:
        
       ###### Elimina la carpeta en la ruta especifica   
        if os.path.exists(path):
            shutil.rmtree(path)

        log.insertLogToDB('Success','Delete Folder','Success execution in deleteFolder')    
    except Exception as e:
        log.insertLogToDB(str(e),'Delete Folder','Failed execution in deleteFolder')    
        raise
        

    