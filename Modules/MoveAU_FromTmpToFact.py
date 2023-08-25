import os
from Models.base import Base,session
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
    ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
    ###### algo falla
    with session.begin():    
        queryInicial = '''update a set a.ID_FAC = (select top 1 id from FacPremisasOff_HG_Aprov where PEDIDO_ID = a.pedido_id
                        and FECHA_AUDITORIA = a.FECHA_AUDITORIA and FECHA = a.FECHA )
                        from TmpParamAuditPremOff_HG_Aprov a where a.PEDIDO_ID not like ('%Dup%') '''

        query = '''update a set a.ID_FAC = (select top 1 id from {0} where PEDIDO_ID = a.pedido_id
                        and FECHA_AUDITORIA = a.FECHA_AUDITORIA and FECHA = a.FECHA )
                        from {1} a where a.PEDIDO_ID not like ('%Dup%') '''.format(strFactTable,strTmpTableParam)
        session.execute(query) 
        
        session.commit()

def insertAU_FromTmpToFac(TmpTable,FacTable,DimCampos):
    
    listaAInsertar=[]
    with session.begin():
        campos=findAllByTIPO_CAMPO(('GENERAL','PRUEBA','INTERNO','SISTEMA'),DimCampos)
        TmpPedidos=selectByEstadoGeneralInWithoutEspecialista(['SIN GESTION'],TmpTable)
        for tmpPedido in TmpPedidos:
            PedidoFac=FacTable()
            for campo in campos:
                
                
                setattr(PedidoFac,campo.NOMBRE_EN_BD.strip(),  getattr(tmpPedido,campo.NOMBRE_EN_BD.strip()))
                
            listaAInsertar.append(PedidoFac)
                
            
        session.bulk_save_objects(listaAInsertar)

        session.commit()

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



def main(TmpTable,FactTable,DimCampos,TmpTableParam,TableParam,strFactTable,strTmpTableParam):
    insertAU_FromTmpToFac(TmpTable,FactTable,DimCampos)
    
    
     
        
    
