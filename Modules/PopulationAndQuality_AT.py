from datetime import datetime
import os
import string
from tkinter import N
from sqlalchemy import and_, case, exists, not_, or_, select
from Models.base import Base,session
import pandas as pd

#from Repo import TmpParametrosRepo,GestionOperativaRepo
#from Repo import DimParametros2Repo
#from Repo import FacPedidoRepo
from Util.Paths import pathFromCloudAS,appSheetFolderName,excelName,pedidoExcelSheet,UEN,imagesFolderName
from Util.Paths import version,UEN,PROCESO,SUBPROCESO,TIPO_AUDITORIA
from Util.Paths import AU_Identifier
from Modules import PopulationAndQuality_AT as PopAndQua_AT 

import math
#from Repo import DimCamposRepo
from Models import base
from sqlalchemy import func
from Modules import log



def ERROR_DESC_CUENTA_ERROR_CALIDAD_DATA_PARAMETROS (TmpTableParam,TmpTable,DimParametros2):
    '''
    POPULA ERROR_DESCRIPCION, CUENTA_ERROR, CALIDAD_DATA , TIEMPO_VAL dependiendo de ciertos parametros
    '''

    with session.begin():
        lista=[]
        ###### carga datos de la base de datos
        data=PopAndQua_AT.selectByEstadoGeneralIn(TmpTable,['AUDITADO'])
        params= findBy_Version_NotIn_IdParam_And_NotIn_IdClassParametro(DimParametros2)
        
        for pedido in data:
            
            ####### CALIDAD TECNOLOGIA_AUDIT Y REGISTRO DE PARAMETROS
            for param in params:
               
                if(pedido.TECNOLOGIA_AUDIT in param.TECNOLOGIA and selectByIdFacAndIdParametro(pedido.ID,param.ID_PARAMETRO,TmpTableParam).HALLAZGO == None):                    
                    if(pedido.ERROR_DESCRIPCION == None):
                        pedido.ERROR_DESCRIPCION = str(param.ID_PARAMETRO)
                    else:
                        pedido.ERROR_DESCRIPCION += '/'+str(param.ID_PARAMETRO)
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

        ##Se filtran por tecnologia diferente a vacia para los que las pruebas depende de este campo 
    params=session.query(TmpTable).filter(and_(
                TmpTable.ESTADO_GENERAL.in_(ESTADO_GENERAL),
                TmpTable.TECNOLOGIA_AUDIT!=None)).all()
    
    
    
    return params

def selectByIdFacAndIdParametro (idFac:str,idParametro:str,TmpTableParam):
    
    
    count=session.query(TmpTableParam).filter(TmpTableParam.ID_FAC == idFac,TmpTableParam.ID_PARAMETRO == idParametro).one_or_none()
    
    return count

def findBy_Version_NotIn_IdParam_And_NotIn_IdClassParametro (DimParametros2):
    query=session.query(DimParametros2).filter(
            DimParametros2.VS ==version,
            DimParametros2.UEN ==UEN,
            DimParametros2.SUBPROCESO == SUBPROCESO,
            DimParametros2.TIPO_AUDITORIA == TIPO_AUDITORIA,
            DimParametros2.PROCESO == PROCESO,
            DimParametros2.ID_PARAMETRO.not_in(['NOV']),
            DimParametros2.ID_CLASS_PARAMETRO.not_in(['MAT_REP'])

                    )

    params=query.all()
    
    return params

def updateCAL_GENERAL_en_Parametros(TmpTableParam):
    
    with session.begin():
        
        cases=case( 
                    (and_(TmpTableParam.CANT_FOTOS  == 1, (TmpTableParam.CANT_FOTOS -TmpTableParam.CANT_FOTOS_VAL)!= 0  ), 0),
                    (TmpTableParam.CAL_DESCRIPCION == 'SIN FOTO' , 0)
                    
                    )
        session.query(TmpTableParam).filter(
                    TmpTableParam.HALLAZGO == 'NO CUMPLE').\
                    update({TmpTableParam.CAL_GENERAL:
                    cases},
                    synchronize_session=False)
        
        session.commit()



def update_CALIDAD_DATA_en_premisas_por_fotos(strTmpTableParam,strTmpTable,strDimParametros2):
    
    with session.begin():
        query = """with para as 
            (select  C.ID_FAC, 
            cast(STUFF
                ((SELECT DISTINCT '/' + cast(B.CAL_DESCRIPCION AS varchar)
                    FROM          {0}   B left join {2} Dim on B.ID_PARAMETRO_TIPO = Dim.ID_PARAMETRO_TIPO 
                        and Dim.VS = B.VS
                    where Dim.REQ_EVIDEN = 'SI'
                    and   B.ID_FAC = C.ID_FAC
                    GROUP BY cast(B.CAL_DESCRIPCION AS varchar) FOR XML PATH('')), 1, 1, '') AS nvarchar) CAL_DESCRIPCION
            ,
            (SELECT SUM(D.CANT_ERROR)
            FROM {0} D left join {2} Dim on D.ID_PARAMETRO_TIPO = Dim.ID_PARAMETRO_TIPO 
                        and Dim.VS = D.VS
            where Dim.REQ_EVIDEN = 'SI'
            and   D.ID_FAC = C.ID_FAC 
            )CANT_ERROR
            FROM {0}  C 
            where C.CAL_GENERAL = 0
            group by C.ID_FAC, C.CAL_DESCRIPCION, C.CANT_ERROR)
            update A set A.CALIDAD_DATA = 'NO', 
            A.ERROR_DESCRIPCION = iif(A.ERROR_DESCRIPCION is null,para.CAL_DESCRIPCION,concat(A.ERROR_DESCRIPCION,' /',para.CAL_DESCRIPCION)),
            CUENTA_ERROR = IIF(CUENTA_ERROR IS NULL, Para.CANT_ERROR,CUENTA_ERROR+ para.CANT_ERROR)
            FROM para left join {1} a on para.id_FAC = a.ID
            where para.id_FAC = a.ID
            """.format(strTmpTableParam,strTmpTable,strDimParametros2)

        querySinParam =  """with para as 
            (select  C.ID_FAC, 
            cast(STUFF
                ((SELECT DISTINCT '/' + cast(B.CAL_DESCRIPCION AS varchar)
                    FROM          TmpParamAuditPremOff_HG_Aprov   B left join DimParametros2 Dim on B.ID_PARAMETRO_TIPO = Dim.ID_PARAMETRO_TIPO 
                        and Dim.VS = B.VS
                    where Dim.REQ_EVIDEN = 'SI'
                    and   B.ID_FAC = C.ID_FAC
                    GROUP BY cast(B.CAL_DESCRIPCION AS varchar) FOR XML PATH('')), 1, 1, '') AS nvarchar) CAL_DESCRIPCION
            ,
            (SELECT SUM(D.CANT_ERROR)
            FROM TmpParamAuditPremOff_HG_Aprov D left join DimParametros2 Dim on D.ID_PARAMETRO_TIPO = Dim.ID_PARAMETRO_TIPO 
                        and Dim.VS = D.VS
            where Dim.REQ_EVIDEN = 'SI'
            and   D.ID_FAC = C.ID_FAC 
            )CANT_ERROR
            FROM TmpParamAuditPremOff_HG_Aprov  C 
            where C.CAL_GENERAL = 0
            group by C.ID_FAC, C.CAL_DESCRIPCION, C.CANT_ERROR)
            update A set A.CALIDAD_DATA = 'NO', 
            A.ERROR_DESCRIPCION = iif(A.ERROR_DESCRIPCION is null,para.CAL_DESCRIPCION,concat(A.ERROR_DESCRIPCION,' /',para.CAL_DESCRIPCION)),
            CUENTA_ERROR = IIF(CUENTA_ERROR IS NULL, Para.CANT_ERROR,CUENTA_ERROR+ para.CANT_ERROR)
            FROM para left join TmpPremisasOff_HG_Aprov a on para.id_FAC = a.ID
            where para.id_FAC = a.ID
            """   
        session.execute(query) 
        session.commit()

def Etiquetar_Duplicados (TmpTable,TmpTableParam):
    ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
    ###### algo falla
    with session.begin():

        lista=[]
        dataparametros=findByLikeErrorDescripcion(TmpTable,TmpTableParam)
        for parametro in dataparametros:

            parametro.PEDIDO_ID = parametro.PEDIDO_ID+'-Dup'
            lista.append(parametro)
'''
        datapedidos=findByLikeErrorDescripcion()
        
        for pedido in datapedidos:
            
            pedido.PEDIDO_ID = pedido.PEDIDO_ID+'-Dup'
            lista.append(pedido)
        ######  Agrego la lista a la session para guardarlos en la base de datos y con el commit hago persistencia en la base de datos
        session.bulk_save_objects(lista)
        session.commit()
'''

def findByLikeErrorDescripcion(TmpTable,TmpTableParam):
    
    
    params=session.query(TmpTableParam).outerjoin(TmpTable,
                        TmpTableParam.ID_FAC == TmpTable.ID).filter(
                        TmpTable.ERROR_DESCRIPCION.ilike('%Dup%')).all()
    
    return params
    

def update_FECHA_AUDITORIA_NULL_en_parametros(TmpTableParam,FECHA:datetime):
    
    with session.begin():
        session.query(TmpTableParam).filter(
                    TmpTableParam.FECHA_AUDITORIA == None).\
                    update({TmpTableParam.FECHA_AUDITORIA:FECHA},
                    synchronize_session=False)
        
        session.commit()

def main(TmpTable,TmpTableParam,DimParametros2,strTmpTable,strTmpTableParam,strDimParametros2,readingDate:datetime):
    ERROR_DESC_CUENTA_ERROR_CALIDAD_DATA_PARAMETROS(TmpTableParam,TmpTable,DimParametros2)
    updateCAL_GENERAL_en_Parametros(TmpTableParam)
    update_CALIDAD_DATA_en_premisas_por_fotos(strTmpTableParam,strTmpTable,strDimParametros2)
    #Etiquetar_Duplicados(TmpTable,TmpTableParam)
    update_FECHA_AUDITORIA_NULL_en_parametros(TmpTableParam,readingDate)


