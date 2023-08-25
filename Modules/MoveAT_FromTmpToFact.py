import os
from Models.base import Base,session
from Modules import log
from Util.Paths import pathFromCloudAS,appSheetFolderName,excelName,pedidoExcelSheet,UEN,imagesFolderName,version,TIPO_AUDITORIA,UEN,PROCESO,SUBPROCESO,vsMateriales,SUBPROCESOMateriales ,SUBPROCESOParametros,vsParametros
from Util.Paths import AU_Identifier

absFilePath = os.path.abspath(__file__)
path, nombreController = os.path.split(absFilePath)

def insertAT_FromTmpToFac(TmpTableParam,TableParam,DimCampos):
    
    listaAInsertar=[]
    with session.begin():
        campos=findAllByTIPO_CAMPOForParametros(('GENERAL','PARAMETRO','SISTEMA'),DimCampos)
        tmpParametros=selectAllNoDup(TmpTableParam)
        for tmpParametro in tmpParametros:
            FacParametro=TableParam()
            for campo in campos:
                setattr(FacParametro,campo.NOMBRE_EN_BD.strip(),  getattr(tmpParametro,campo.NOMBRE_EN_BD.strip()))
                
            
                # FacParametro.ID_FAC = tmpParametro.ID_FAC
                # FacParametro.FECHA_AUDITORIA = tmpParametro.FECHA_AUDITORIA
                # FacParametro.PEDIDO_ID = tmpParametro.PEDIDO_ID
                # FacParametro.HALLAZGO = tmpParametro.HALLAZGO
                # FacParametro.ID_PARAMETRO = tmpParametro.ID_PARAMETRO
                # FacParametro.ID_PARAMETRO_TIPO = tmpParametro.ID_PARAMETRO_TIPO
                # FacParametro.FOTO1 = tmpParametro.FOTO1
                # FacParametro.CAL_FOTO1 = tmpParametro.CAL_FOTO1
                # FacParametro.FOTO2 = tmpParametro.FOTO2
                # FacParametro.CAL_FOTO2 = tmpParametro.CAL_FOTO2
                # FacParametro.FOTO3 = tmpParametro.FOTO3
                # FacParametro.CAL_FOTO3 = tmpParametro.CAL_FOTO3
                # FacParametro.DATALLE_HALLAZGO = tmpParametro.DATALLE_HALLAZGO
                # FacParametro.CANT_FOTOS = tmpParametro.CANT_FOTOS
                # FacParametro.CANT_FOTOS_VAL = tmpParametro.CANT_FOTOS_VAL
                # FacParametro.CAL_GENERAL = tmpParametro.CAL_GENERAL
                # FacParametro.CAL_DESCRIPCION = tmpParametro.CAL_DESCRIPCION
                # FacParametro.CANT_ERROR = tmpParametro.CANT_ERROR
                # FacParametro.OBSERVACIONES = tmpParametro.OBSERVACIONES
                # FacParametro.FECHA = tmpParametro.FECHA
                # FacParametro.FECHA_CARGA = tmpParametro.FECHA_CARGA
                # FacParametro.FECHA_VERF_MESA = tmpParametro.FECHA_VERF_MESA
                # FacParametro.VS = tmpParametro.VS
            

            listaAInsertar.append(FacParametro)
            
        
        session.bulk_save_objects(listaAInsertar)

        session.commit()

def selectAllNoDup (TmpTableParam):
    
   
    params=session.query(TmpTableParam).filter(~TmpTableParam.PEDIDO_ID.ilike('%Dup%') ).all()
    
    
    return params
def findAllByTIPO_CAMPOForParametros(TIPO_CAMPO:tuple,DimCampos):
    ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
    ###### algo falla
    
    params=session.query(DimCampos).filter(DimCampos.VS==vsParametros ,
                                            DimCampos.TIPO_AUDITORIA== TIPO_AUDITORIA,
                                            DimCampos.UEN== UEN,
                                            DimCampos.PROCESO== PROCESO,
                                            DimCampos.SUBPROCESO== SUBPROCESOParametros,
                                            DimCampos.TIPO_CAMPO.in_(TIPO_CAMPO)).all()
        
    return params

def updateIdParamsBeforeFac(strFactTable,strTmpTableParam):
    try:
        
            queryInicial = '''update a set a.ID_FAC = (select top 1 id from FacPremisasOff_HG_Aprov where PEDIDO_ID = a.pedido_id
                            and FECHA_AUDITORIA = a.FECHA_AUDITORIA and FECHA = a.FECHA )
                            from TmpParamAuditPremOff_HG_Aprov a where a.PEDIDO_ID not like ('%Dup%') '''

            query = '''update a set a.ID_FAC = (select top 1 id from {0} where PEDIDO_ID = a.pedido_id
                            and FECHA_AUDITORIA = a.FECHA_AUDITORIA and FECHA = a.FECHA )
                            from {1} a where a.PEDIDO_ID not like ('%Dup%') '''.format(strFactTable,strTmpTableParam)

            with session.begin():    
                session.execute(query)     
                session.commit()
            
    except Exception as e:
        log.insertLogToDB(str(e),'updateIdParamsBeforeFac','Failed execution in Mod MoveAT_FromTmpToFact')
        raise




def selectByEstadoGeneralInWithoutEspecialista (ESTADO_GENERAL,TmpTable):
    
   
    params=session.query(TmpTable).filter(
                TmpTable.ESTADO_GENERAL.not_in(ESTADO_GENERAL),TmpTable.AUDITOR != 'jaime.gallego@tigoune.com',~TmpTable.PEDIDO_ID.ilike('%Dup%') ).all()
    
    
    return params        

def findAllByTIPO_CAMPO(TIPO_CAMPO:tuple,DimCampos):
    ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
    ###### algo falla
    
    params=session.query(DimCampos).filter(DimCampos.VS==version ,
                                            DimCampos.TIPO_AUDITORIA== TIPO_AUDITORIA,
                                            DimCampos.UEN== UEN,
                                            DimCampos.PROCESO== PROCESO,
                                            DimCampos.SUBPROCESO== SUBPROCESO,
                                            DimCampos.TIPO_CAMPO.in_(TIPO_CAMPO)).all()
        
    return params

def check_if_Dup(strTmpTable):
    
    query ='''with  tmp (PEDIDO,veces)
                            as (
                            select PEDIDO_ID,COUNT(*)
                            from {0}
                            where ESTADO_GENERAL != 'sin gestion'
                            GROUP BY PEDIDO_ID
                            HAVING COUNT(*)>1 )
                            select count(*)
                            from tmp'''.format(strTmpTable)

    dup = session.execute(query).scalar()
    return dup
    

def findToPass_AU_Dup(): 
    
        query = '''select PEDIDO_ID,COUNT(*) as num_veces
                from TmpPremisasOff_HG_Aprov
                where ESTADO_GENERAL != 'sin gestion'
                GROUP BY PEDIDO_ID
                HAVING COUNT(*)>1'''
                
        result =  session.execute(query).fetchall()    
        return result
        

def findAU_Dup_withID(AU_Dup,strFactTable,date):
    print(AU_Dup)
    AU_Value = AU_Dup.PEDIDO_ID

    query = '''WITH PEDIDO_DUP (PEDIDO,VECES) AS (
                select PEDIDO_ID,COUNT(*)
                from {0}
                where ESTADO_GENERAL != 'sin gestion'
                and PEDIDO_ID = '{1}'
                and FECHA = '{2}'
                GROUP BY PEDIDO_ID
                HAVING COUNT(*)>1
                )SELECT ID, PEDIDO_ID FROM {0}
                WHERE PEDIDO_ID IN (
                SELECT PEDIDO FROM PEDIDO_DUP)'''.format(strFactTable,AU_Value,date)

    result = session.execute(query).fetchall()
    return result

    
def updateID_FAC(id_fac,AU_Value,strTmpTableParam,numberParams):
    
    
    query = '''UPDATE {2} set ID_FAC = {0}
            where id in ( 
            select top {3} id
            from {2}
            where PEDIDO_ID = '{1}'
            and ID_FAC = 0
            order by ID asc )
            '''.format(id_fac,AU_Value,strTmpTableParam,numberParams)

    
    session.execute(query)
    session.commit()

def updateWithZero_ID_FAC(AU_Value,strTmpTableParam):
    
    
    query = '''update {1} set id_fac = 0
                where PEDIDO_ID = '{0}'
            '''.format(AU_Value,strTmpTableParam)

    
    session.execute(query)    
    session.commit()

        

def countParametros(DimParametros2):
    
    try:
        params=session.query(DimParametros2).filter(DimParametros2.VS==vsParametros ,
                                                DimParametros2.TIPO_AUDITORIA== TIPO_AUDITORIA,
                                                DimParametros2.UEN== UEN,
                                                DimParametros2.PROCESO== PROCESO,
                                                DimParametros2.SUBPROCESO== SUBPROCESO).count()
            
        return params
    except Exception as e:
        log.insertLogToDB(str(e),'countParametros','Failed execution in Mod MoveAT_FromTmpToFact')
        raise
    
def trataID_FAC_paraDup(strTmpTable,strFactTable,TmpTable,readingDate,strTmpTableParam,DimParametros2):
    try:
        numberParams = countParametros(DimParametros2)
        date= readingDate.date()
        
        number_AU_Dup = check_if_Dup(strTmpTable)
        if number_AU_Dup !=0:
            
            Detail_AUs_Dup = findToPass_AU_Dup()
            for AU_Dup in Detail_AUs_Dup:
                updateWithZero_ID_FAC(AU_Dup.PEDIDO_ID,strTmpTableParam)
                AUs_With_ID = findAU_Dup_withID(AU_Dup,strFactTable,date)
                for AU_With_ID in AUs_With_ID:
                    ID = AU_With_ID.ID
                    AU_Value = AU_With_ID.PEDIDO_ID
                    updateID_FAC(ID,AU_Value,strTmpTableParam,numberParams)
    except Exception as e:
        log.insertLogToDB(str(e),'trataID_FAC_paraDup','Failed execution in Mod MoveAT_FromTmpToFact')
        raise

def main(TmpTable,strTmpTable,FactTable,DimCampos,TmpTableParam,TableParam,strFactTable,strTmpTableParam,readingDate,DimParametros2):
    #updateIdParamsBeforeFac(strFactTable,strTmpTableParam)
    #trataID_FAC_paraDup(strTmpTable,strFactTable,TmpTable,readingDate,strTmpTableParam,DimParametros2)
    insertAT_FromTmpToFac(TmpTableParam,TableParam,DimCampos)
    
    
     
        
    
