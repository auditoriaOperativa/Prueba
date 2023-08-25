from datetime import datetime
import os
import string
from tkinter import N
from sqlalchemy import and_, case, exists, not_, or_, select
from Models.base import Base,session
import pandas as pd
from openpyxl import load_workbook

#from Repo import TmpParametrosRepo,GestionOperativaRepo
#from Repo import DimParamRepo
#from Repo import FacPedidoRepo
from Util.Paths import pathFromCloudAS,appSheetFolderName,excelName,pedidoExcelSheet,UEN,imagesFolderName
from Util.Paths import version,UEN,PROCESO,SUBPROCESO,TIPO_AUDITORIA
from Util.Paths import AU_Identifier

import math
#from Repo import DimCamposRepo
from Models import base
from sqlalchemy import func
from Modules import log

###### Modifica campos ERROR_DESCRIPCION, CUENTA_ERROR , CALIDAD_DATA, TIEMPO_VAL a partir de ciertas condiciones
def ERROR_DESC_CUENTA_ERROR_CALIDAD_DATA (TmpTable):
    ''' Modifica campos [ERROR_DESCRIPCION], [CUENTA_ERROR] , [CALIDAD_DATA], [TIEMPO_VAL] a partir de ciertas condiciones
    '''
    with session.begin():
        ###### trae los registros de la base de datos a ser modificados
        data=selectByEstadoGeneralIn(TmpTable,('AUDITADO','NO AUDITADO'))
        lista=[]
        for pedido in data:
            
            
            ####Fecha vacia
            if(pedido.FECHA_AUDITORIA == None):
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'FechaAudi'
                else:
                    pedido.ERROR_DESCRIPCION += ' /FechaAudi'
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                pedido.CALIDAD_DATA   = 'NO'

            ####Calidad consistencia fechas          
            if(pedido.FECHA_AUDITORIA != pedido.FECHA and pedido.TIMESTAMP.date() == pedido.FECHA):
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'FechaAudi'
                else:
                    pedido.ERROR_DESCRIPCION += ' /FechaAudi'
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                pedido.CALIDAD_DATA   = 'NO'
                
            if(pedido.FECHA_AUDITORIA != pedido.FECHA and pedido.TIMESTAMP.date() != pedido.FECHA):
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'Sinc'
                else:
                    pedido.ERROR_DESCRIPCION += ' /Sinc'
                '''
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                '''
            ####CALIDAD HORA DE INICIO
            if(pedido.HORA_INICIO == None or pedido.HORA_INICIO < 0.288194444 or pedido.HORA_INICIO > 0.8125):
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'HrIni'
                else:
                    pedido.ERROR_DESCRIPCION += ' /HrIni'
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                pedido.TIEMPO_VAL= 0
                pedido.CALIDAD_DATA   = 'NO'
            ####CALIDAD LATITUD Y LONGITUD
            if(pedido.LATITUD_LONGITUD  == None ):
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'longLat'
                else:
                    pedido.ERROR_DESCRIPCION += ' /longLat'
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                pedido.CALIDAD_DATA   = 'NO'
            ####CALIDAD HORA FINALIZACION
            if(pedido.HORA_FINALIZACION  == None or pedido.HORA_FINALIZACION  < 0.288194444 or pedido.HORA_FINALIZACION  > 0.8125):
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'HrFin'
                else:
                    pedido.ERROR_DESCRIPCION += ' /HrFin'
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                pedido.TIEMPO_VAL= 0
                pedido.CALIDAD_DATA   = 'NO'
            ####CALIDAD CAMPO ENVIAR
            if(pedido.ENVIAR   == None or pedido.ENVIAR == 0):
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'Enviar'
                else:
                    pedido.ERROR_DESCRIPCION += ' /Enviar'
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                pedido.CALIDAD_DATA   = 'NO'
            ####CALIDAD DIFERENCIA HORAS     
            if(pedido.HORA_FINALIZACION!=None and pedido.HORA_INICIO != None):   
                if(pedido.HORA_FINALIZACION-pedido.HORA_INICIO<0):
                    if(pedido.ERROR_DESCRIPCION == None):
                        pedido.ERROR_DESCRIPCION = 'Hra_dif'
                    else:
                        pedido.ERROR_DESCRIPCION += ' /Hra_dif'
                    if(pedido.CUENTA_ERROR  == None):
                        pedido.CUENTA_ERROR  = 1
                    else:
                        pedido.CUENTA_ERROR  += 1
                    pedido.TIEMPO_VAL= 0
                    pedido.CALIDAD_DATA   = 'NO'
            ####CALIDAD TECNOLOGIA_AUDIT     
            if(pedido.TECNOLOGIA_AUDIT == None and pedido.ESTADO_AUDITORIA != 'NO AUDITABLE'):   
                if(pedido.ERROR_DESCRIPCION == None):
                    pedido.ERROR_DESCRIPCION = 'Tec'
                else:
                    pedido.ERROR_DESCRIPCION += ' /Tec'
                if(pedido.CUENTA_ERROR  == None):
                    pedido.CUENTA_ERROR  = 1
                else:
                    pedido.CUENTA_ERROR  += 1
                pedido.CALIDAD_DATA   = 'NO'
            
            lista.append(pedido)
        ######  Agrego la lista a la session para guardarlos en la base de datos y con el commit hago persistencia en la base de datos
        session.bulk_save_objects(lista)
        session.commit()

def selectByEstadoGeneralIn (TmpTable,ESTADO_GENERAL):
    
    params=session.query(TmpTable).filter(
                TmpTable.ESTADO_GENERAL.in_(ESTADO_GENERAL) ).all()
    
    return params      

def campoCALIDAD_DATA(TmpTable):

    ###### Etiqueta  [CALIDAD_DATA] = 'NO' LOS CAMPOS PROBADOS Y EL RESTO LOS DEJA COMO [CALIDAD_DATA] = 'SI'
    ###### algo falla
    with session.begin():
        session.query(TmpTable).filter(
                     TmpTable.ESTADO_GENERAL == 'SIN GESTION').\
                    update({TmpTable.CALIDAD_DATA :'NO APLICA'},
                    synchronize_session=False)
        session.query(TmpTable).filter(or_(and_(
                     TmpTable.ESTADO_GENERAL.in_(('NO AUDITADO','AUDITADO')),
                                    or_(TmpTable.ENVIAR  != 1,
                                        TmpTable.FECHA_AUDITORIA  == None,
                                        TmpTable.HORA_INICIO   == None,
                                        TmpTable.LATITUD_LONGITUD   == None,
                                        TmpTable.HORA_FINALIZACION   == None)),
                                        and_(
                     TmpTable.ESTADO_GENERAL == 'AUDITADO',TmpTable.TECNOLOGIA_AUDIT   == None))
                                        ).\
                    update({TmpTable.CALIDAD_DATA :'NO'},
                    synchronize_session=False)

        session.query(TmpTable).filter(
                     TmpTable.CALIDAD_DATA == None).\
                    update({TmpTable.CALIDAD_DATA :'SI'},
                    synchronize_session=False)
        session.commit()

def Duplicados_e_IMPUT_MESA_Dia (strFactTable,strTmpTable):
    '''
        EXECUTE A SQL STATEMENT
    '''
    query = '''update A 
                    set A.CALIDAD_DATA = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {1}
                                        WHERE PEDIDO_ID = A.PEDIDO_ID  ) >1 then 'NO' else A.CALIDAD_DATA end,
                        A.ERROR_DESCRIPCION = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {1}
                                        WHERE PEDIDO_ID = A.PEDIDO_ID ) >1 then 
                                        iif(A.ERROR_DESCRIPCION is null,'Dup',A.ERROR_DESCRIPCION+' / Dup') else A.ERROR_DESCRIPCION end,
                        A.CUENTA_ERROR = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {1}
                                        WHERE PEDIDO_ID = A.PEDIDO_ID ) >1 then 
                                        IIF(A.CUENTA_ERROR IS NULL,1, A.CUENTA_ERROR+1) else A.CUENTA_ERROR end,
                        A.IMPUT_MESA = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {1}
                                        WHERE [PEDIDO_ID] = A.[PEDIDO_ID] ) >1 then 
                                        1 else A.IMPUT_MESA end
                        FROM {1} as A'''.format(strFactTable,strTmpTable)

    
    with session.begin():
        session.execute(query)
        session.commit()

def Duplicados_e_IMPUT_MESA_Historia (strFactTable,strTmpTable):
    '''
        EXECUTE A SQL STATEMENT
    '''
    query = '''update A 
                    set A.CALIDAD_DATA = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {0}
                                        WHERE PEDIDO_ID = A.PEDIDO_ID  ) >=1 then 'NO' else A.CALIDAD_DATA end,
                        A.ERROR_DESCRIPCION = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {0}
                                        WHERE PEDIDO_ID = A.PEDIDO_ID ) >=1 then 
                                        iif(A.ERROR_DESCRIPCION is null,'Dup',A.ERROR_DESCRIPCION+' / Dup') else A.ERROR_DESCRIPCION end,
                        A.CUENTA_ERROR = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {0}
                                        WHERE PEDIDO_ID = A.PEDIDO_ID ) >=1 then 
                                        IIF(A.CUENTA_ERROR IS NULL,1, A.CUENTA_ERROR+1) else A.CUENTA_ERROR end,
                        A.IMPUT_MESA = CASE 
                                    when 
                                        (select  count(*)
                                        FROM {0}
                                        WHERE [PEDIDO_ID] = A.[PEDIDO_ID] ) >1 then 
                                        1 else A.IMPUT_MESA end
                        FROM {1} as A '''.format(strFactTable,strTmpTable)

    
    with session.begin():
        session.execute(query)
        session.commit()

def HALLAZGO_GENERAL_FAC (TmpTable, TmpTableParam, DimParam):
    ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
    ###### algo falla
    with session.begin():

        ######## UPDATE HALLAZGO_GENERAL
        cases=case( 
                    (session.query(TmpTableParam).filter(TmpTableParam.HALLAZGO == 'NO CUMPLE',
                    TmpTableParam.ID_PARAMETRO.not_in(['A10','NOV']),
                    TmpTableParam.ID_FAC == TmpTable.ID).exists() , 0),
                    else_=1

                    
                    )
        session.query(TmpTable).\
                    update({TmpTable.HALLAZGO_GENERAL:
                    cases},
                    synchronize_session=False)
        
        ###### CRITICIDAD_GENERAL FAC
        cases=case( 
                    (session.query(TmpTableParam).outerjoin(DimParam,and_(TmpTableParam.VS ==DimParam.VS 
                                                                    ,TmpTableParam.ID_PARAMETRO_TIPO == DimParam.ID_PARAMETRO_TIPO)).filter(DimParam.CRITICIDAD == 'GRAVE',
                                                                TmpTableParam.HALLAZGO=='NO CUMPLE',
                                                                TmpTableParam.ID_FAC ==TmpTable.ID,
                                                                TmpTableParam.ID_PARAMETRO.not_in(['A10'])
                                                                ).exists() , 'GRAVE'),
                    (session.query(TmpTableParam).outerjoin(DimParam,and_(TmpTableParam.VS ==DimParam.VS 
                                                                    ,TmpTableParam.ID_PARAMETRO_TIPO == DimParam.ID_PARAMETRO_TIPO)).filter(DimParam.CRITICIDAD == 'MEDIO',
                                                                TmpTableParam.HALLAZGO=='NO CUMPLE',
                                                                TmpTableParam.ID_FAC ==TmpTable.ID,
                                                                TmpTableParam.ID_PARAMETRO.not_in(['A10'])
                                                                ).exists() , 'MEDIO'),
                    (session.query(TmpTableParam).outerjoin(DimParam,and_(TmpTableParam.VS ==DimParam.VS 
                                                                    ,TmpTableParam.ID_PARAMETRO_TIPO == DimParam.ID_PARAMETRO_TIPO)).filter(DimParam.CRITICIDAD == 'LEVE',
                                                                TmpTableParam.HALLAZGO=='NO CUMPLE',
                                                                TmpTableParam.ID_FAC ==TmpTable.ID,
                                                                TmpTableParam.ID_PARAMETRO.not_in(['A10'])
                                                                ).exists() , 'LEVE'),
                    else_=None

                    
                    )
        session.query(TmpTable).\
                    update({TmpTable.CRITICIDAD_GENERAL:
                    cases},
                    synchronize_session=False)
        
        ###### update InputAuditor
        session.query(TmpTable).filter(TmpTable.CALIDAD_DATA=='NO',
                                                    TmpTable.ERROR_DESCRIPCION!= 'Dup').\
                    update({TmpTable.IMPUT_AUDITOR:1},
                    synchronize_session=False)

        ########### COMENTADA se encuentra una forma de realizar los 3 updates anteriores utilizando select y modificando los objetos,
        ############ pero se opto por la opcion de los updates con el orm para evitar demoras ya que el proceso iterativo era bastante demorado
        # data=findAll()
        # lista=[]
        # for pedido in data:
            
        #     ####Calidad consistencia fechas          
        #     if(TmpParametrosRepo.countByIdFac(pedido.ID)>0):
        #         pedido.HALLAZGO_GENERAL =0
        #     else:
        #         pedido.HALLAZGO_GENERAL =1
            
        #     ###### CRITICIDAD_GENERAL FAC
            # if(TmpParametrosRepo.countByIdFacAndCriticidad(pedido.ID,'GRAVE')>0):
            #     pedido.CRITICIDAD_GENERAL='GRAVE'
        #     elif(TmpParametrosRepo.countByIdFacAndCriticidad(pedido.ID,'MEDIO')>0):
        #         pedido.CRITICIDAD_GENERAL='MEDIO'
        #     elif(TmpParametrosRepo.countByIdFacAndCriticidad(pedido.ID,'LEVE')>0):
        #         pedido.CRITICIDAD_GENERAL='LEVE'
        #     else:
        #         pedido.CRITICIDAD_GENERAL=None
        #     ###### CAMPO IMPUT_AUDITOR
        #     if (pedido.CALIDAD_DATA =='NO' and pedido.ERROR_DESCRIPCION != 'Dup'):
        #         pedido.IMPUT_AUDITOR=1
        #     ###### CAMPO IMPUT_MESA
        #     if (pedido.CALIDAD_DATA =='NO' and  'Dup'  in pedido.ERROR_DESCRIPCION):
        #         pedido.IMPUT_MESA=1
                
        #     lista.append(pedido)
        # session.bulk_save_objects(lista)
        session.commit()

def Etiquetar_Duplicados (TmpTable):
    
    with session.begin():
        
        lista=[]
        '''
        dataparametros=findByLikeErrorDescripcion()
        for parametro in dataparametros:

            parametro.PEDIDO_ID = parametro.PEDIDO_ID+'-Dup'
            lista.append(parametro)
        '''
        datapedidos=findByLikeErrorDescripcion(TmpTable)
        
        for pedido in datapedidos:
            
            pedido.PEDIDO_ID = pedido.PEDIDO_ID+'-Dup'
            lista.append(pedido)
        ######  Agrego la lista a la session para guardarlos en la base de datos y con el commit hago persistencia en la base de datos
        session.bulk_save_objects(lista)
        session.commit()


def findByLikeErrorDescripcion(TmpTable):
    
    
    params=session.query(TmpTable).filter( TmpTable.ERROR_DESCRIPCION.ilike('%Dup%')).all()
    
    return params

def updateValuesBoolean(TmpTable):
    '''
    convert the boolean values from 1 and 0 to True or False
    '''
    with session.begin():
        session.query(TmpTable).filter(
                     TmpTable.QUIEN_ATIENDE_ES_QUIEN_RECIBE_EL_SERVICIO == '1').\
                    update({TmpTable.QUIEN_ATIENDE_ES_QUIEN_RECIBE_EL_SERVICIO :'True'},
                    synchronize_session=False)
        session.query(TmpTable).filter(
                     TmpTable.QUIEN_ATIENDE_ES_QUIEN_RECIBE_EL_SERVICIO == '0').\
                    update({TmpTable.QUIEN_ATIENDE_ES_QUIEN_RECIBE_EL_SERVICIO :'False'},
                    synchronize_session=False)
        session.query(TmpTable).filter(
                     TmpTable.NOVEDAD == '1').\
                    update({TmpTable.NOVEDAD :'True'},
                    synchronize_session=False)
        session.query(TmpTable).filter(
                     TmpTable.NOVEDAD == '0').\
                    update({TmpTable.NOVEDAD :'False'},
                    synchronize_session=False)
        session.commit()




def main(TmpTable,FactTable,strTmpTable,strFactTable,readingDate):
    campoCALIDAD_DATA(TmpTable)
    ERROR_DESC_CUENTA_ERROR_CALIDAD_DATA(TmpTable)
    Duplicados_e_IMPUT_MESA_Dia(strFactTable,strTmpTable)
    Duplicados_e_IMPUT_MESA_Historia(strFactTable,strTmpTable)
    #the Audits units are tagged, is add to the end de sufix "-Dup".
    #Etiquetar_Duplicados(TmpTable)
    updateValuesBoolean(TmpTable)