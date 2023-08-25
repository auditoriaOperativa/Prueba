#from ast import Param
from datetime import datetime
#from logging import exception
import os
from sqlalchemy import and_, case, exists, select
from Models.base import Base,session
from Models import base
import pandas as pd
#from Repo.FacParametrosRepo import ParamAuditPremOff_HG_Aprov
from Util.Paths import version,imagesFolderName, UEN, PROCESO, SUBPROCESO,TIPO_AUDITORIA
from Util.Paths import AU_Identifier
from Util import Functions
#from  Repo import DimParametros2Repo
import math
from Modules import log



def truncateTmpTableParam(strTmpTableParam):
    try:

        sql = 'TRUNCATE TABLE {0}'.format(strTmpTableParam)
        
        with session.begin():
            session.execute(sql) 
            session.commit()
    except Exception as e:
        log.insertLogToDB(str(e),'truncateTmpTableParam','Failed execution in Mod Audit_Test')
        raise

def readParamsFromTmpTableParam(TmpTable,fecha:datetime):
    try:
        ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
        ###### algo falla
        with session.begin():
            params=session.query(TmpTable).filter(
                        TmpTable.ESTADO_GENERAL != 'SIN GESTION',TmpTable.FECHA == fecha.strftime('%Y-%m-%d') ).all()
            session.commit()
            return params
    except Exception as e:
        log.insertLogToDB(str(e),'readParamsFromTmpTableParam','Failed execution in Mod Audit_Test')
        raise


def insert_AT_ToTmpTableParam(AuditData:pd.DataFrame,date:datetime,TmpTableParam,TmpTable,DimParam):
    fecha_carga = datetime.today()
    try:
        listaAInsertar=[]
        
        with session.begin():
            parametros= findAllForVs(DimParam)
            for _AuditData in AuditData:
                ###### Agrega a la lista los objetos creados en el metodo con los parametros entregados
                for parametro in parametros:
                    listaAInsertar.append(createObjectTmpTableParam(_AuditData,parametro.ID_PARAMETRO,date,TmpTableParam,TmpTable,DimParam,fecha_carga))

            
                
            
            session.bulk_save_objects(listaAInsertar)

            session.commit()
    except Exception as e:
        log.insertLogToDB(str(e),'insert_AT_ToTmpTableParam','Failed execution in Mod Audit_Test')
        raise

##### Toma cada premisa y extrae el parametro individual de manera dinamica.
def createObjectTmpTableParam(AuditData,Id_Parametro:str,date:datetime,TmpTableParam,TmpTable,DimParam,fecha_carga):
 
    try:
        #AuditData = list[TmpTable]
        
        dict= {column: str(getattr(AuditData, column)) for column in AuditData.__table__.c.keys()}
        
        
        ins_TmpTableParam=TmpTableParam()
        ins_TmpTableParam.ID_FAC = AuditData.ID
        ins_TmpTableParam.PEDIDO_ID = AuditData.PEDIDO_ID
        ins_TmpTableParam.FECHA_AUDITORIA = AuditData.FECHA_AUDITORIA
        ins_TmpTableParam.HALLAZGO = None if dict[Id_Parametro if Id_Parametro != 'NOV' else 'NOVEDAD'] == 'None' else (dict[Id_Parametro if Id_Parametro != 'NOV' else 'NOVEDAD'])
        
        ins_TmpTableParam.ID_PARAMETRO = Id_Parametro
        ins_TmpTableParam.FOTO1 =None if dict[Id_Parametro+'_Foto1'] == 'None' else dict[Id_Parametro+'_Foto1'].replace(imagesFolderName,'')
        ins_TmpTableParam.FOTO2 = None if dict[Id_Parametro+'_Foto2'] == 'None' else dict[Id_Parametro+'_Foto2'].replace(imagesFolderName,'')
        ins_TmpTableParam.FOTO3 = None if dict[Id_Parametro+'_Foto3'] == 'None' else dict[Id_Parametro+'_Foto3'].replace(imagesFolderName,'')
        ins_TmpTableParam.DATALLE_HALLAZGO = None if dict['DETALLES_'+Id_Parametro] == 'None' else dict['DETALLES_'+Id_Parametro]
        CANT_FOTOS=0
        
        if(ins_TmpTableParam.FOTO1 != None and  ins_TmpTableParam.FOTO1 != 'Unable to load image data. Image may be missing or upload size may be too large for this device.'):
            CANT_FOTOS+=1
            
        if(ins_TmpTableParam.FOTO2 != None and  ins_TmpTableParam.FOTO2 != 'Unable to load image data. Image may be missing or upload size may be too large for this device.'):
            CANT_FOTOS+=1

        if(ins_TmpTableParam.FOTO3 != None and  ins_TmpTableParam.FOTO3 != 'Unable to load image data. Image may be missing or upload size may be too large for this device.'):
            CANT_FOTOS+=1

        ins_TmpTableParam.CANT_FOTOS= CANT_FOTOS
        ins_TmpTableParam.CANT_FOTOS_VAL=CANT_FOTOS
        

        #Se trae el respectivo ID_PARAMETRO_TIPI de cada item desde la DimParametros2
        ins_TmpTableParam.ID_PARAMETRO_TIPO = campoID_PARAMETRO_TIPO(Id_Parametro,DimParam)
        ins_TmpTableParam.FECHA_CARGA=fecha_carga
        
        
        
        ins_TmpTableParam.VS=version
        ins_TmpTableParam.FECHA=date
        ###### RECIBE EL PARAMETRO Y REALIZA UPDATE SOBRE CAMPOS IMPORTANTES
        ins_TmpTableParam= campos_CAL_DESCRIPCION(ins_TmpTableParam,DimParam)
        if(ins_TmpTableParam.HALLAZGO =='1' or ins_TmpTableParam.HALLAZGO ==1 ):
            ins_TmpTableParam.HALLAZGO= 'True'
        elif(ins_TmpTableParam.HALLAZGO =='0' or ins_TmpTableParam.HALLAZGO ==0):
            ins_TmpTableParam.HALLAZGO= 'False'
        return ins_TmpTableParam
    except Exception as e:
        log.insertLogToDB(str(e),'createObjectTmpTableParam','Failed execution in Mod Audit_Test')
        raise


def findAllForVs (DimParam):
    try:
        query=session.query(DimParam).filter(
                DimParam.VS ==version,
                DimParam.UEN ==UEN,
                DimParam.SUBPROCESO ==SUBPROCESO,
                DimParam.TIPO_AUDITORIA ==TIPO_AUDITORIA,
                DimParam.PROCESO ==PROCESO
                        )
        
        params=query.all()
        return params
    except Exception as e:
        log.insertLogToDB(str(e),'findAllForVs','Failed execution in Mod Audit_Test')
        raise
    
    

def campoID_PARAMETRO_TIPO(Id_Parametro:str,DimParam):
    try:
        ID_PARAMETRO_TIPO=findByIdParametro(Id_Parametro,DimParam).ID_PARAMETRO_TIPO
        if( ID_PARAMETRO_TIPO == [] or ID_PARAMETRO_TIPO ==None):
            raise Exception("ID_TIPO_PARAMETRO "+str(Id_Parametro) +" NO EXISTE")
        return ID_PARAMETRO_TIPO    
    except Exception as e:
        log.insertLogToDB(str(e),'campoID_PARAMETRO_TIPO','Failed execution in Mod Audit_Test')
        raise

def findByIdParametro (ID_PARAMETRO:str,DimParam):
    try:
        query=session.query(DimParam).filter(
                DimParam.VS ==version,
                DimParam.UEN ==UEN,
                DimParam.SUBPROCESO ==SUBPROCESO,
                DimParam.TIPO_AUDITORIA ==TIPO_AUDITORIA,
                DimParam.PROCESO ==PROCESO,
                DimParam.ID_PARAMETRO == ID_PARAMETRO
                        )
        
        params=query.one_or_none()
        
        return params    
    except Exception as e:
        log.insertLogToDB(str(e),'findByIdParametro','Failed execution in Mod Audit_Test')
        raise


def campos_CAL_DESCRIPCION(Param,DimParam):
    try:
        #def campos_CAL_DESCRIPCION(Param:TmpTableParam):    
        DimParametros=findByIdParametro(Param.ID_PARAMETRO,DimParam)
        if(Param.FOTO1 == None and 
                                Param.FOTO2 == None and 
                                Param.FOTO3== None and 
                                Param.HALLAZGO == 'NO CUMPLE' and 
                                DimParametros.REQ_EVIDEN == 'SI'): 
            Param.CAL_DESCRIPCION= 'SIN FOTO'
            if(Param.CANT_ERROR  == None):
                        Param.CANT_ERROR  = 1
            else:
                Param.CANT_ERROR  += 1
        if(Param.FOTO1 == 'Unable to load image data. Image may be missing or upload size may be too large for this device.' and 
                                Param.HALLAZGO == 'NO CUMPLE'):


            Param.CAL_DESCRIPCION= 'ERROR FOTO'
            Param.CAL_FOTO1=0
            Param.FOTO1=None
            Param.CANT_FOTOS_VAL-=1
            if(Param.CANT_ERROR  == None):
                        Param.CANT_ERROR  = 1
            else:
                Param.CANT_ERROR  += 1
        if(Param.FOTO2 == 'Unable to load image data. Image may be missing or upload size may be too large for this device.' and 
                                Param.HALLAZGO == 'NO CUMPLE'): 

            Param.CAL_DESCRIPCION= 'ERROR FOTO'
            Param.CAL_FOTO2=0
            Param.FOTO2=None
            Param.CANT_FOTOS_VAL-=1
            if(Param.CANT_ERROR  == None):
                        Param.CANT_ERROR  = 1
            else:
                Param.CANT_ERROR  += 1
        if(Param.FOTO3 == 'Unable to load image data. Image may be missing or upload size may be too large for this device.' 
            and 
            Param.HALLAZGO == 'NO CUMPLE'): 
            Param.CAL_DESCRIPCION= 'ERROR FOTO'
            Param.CAL_FOTO3=0
            Param.FOTO3=None
            Param.CANT_FOTOS_VAL-=1
            if(Param.CANT_ERROR  == None):
                        Param.CANT_ERROR  = 1
            else:
                Param.CANT_ERROR  += 1
        return Param
    except Exception as e:
        log.insertLogToDB(str(e),'campos_CAL_DESCRIPCION','Failed execution in Mod Audit_Test')
        raise
def main(readingDate,strTmpTableParam,TmpTable,TmpTableParam,DimParametros2):
    ##lee los parametros de la tabla de Unidades Auditadas y guarda en la varible result_df_params
    result_df_params = readParamsFromTmpTableParam(TmpTable,readingDate)
    #truncateFactTableParam es la funcion que trunca la tabla temporal para poder cargar los parametros nuevos a procesar
    truncateTmpTableParam(strTmpTableParam)
    ##inserta los parametros a la tabla temporal y ademas popula algunos campos de negocio a partir de la evaluación de otros campos
    insert_AT_ToTmpTableParam(result_df_params,readingDate,TmpTableParam,TmpTable,DimParametros2)