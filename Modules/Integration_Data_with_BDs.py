from Models.base import Base,session
from Util.Paths import pathFromCloudAS,appSheetFolderName,excelName,pedidoExcelSheet,UEN,imagesFolderName
from Modules import BD_CS_GestionOperativa

def updateFUENTE(TmpTable):
    
    with session.begin():
        session.query(TmpTable).filter(
                     TmpTable.PROCESO_BD != None).\
                    update({TmpTable.FUENTE:'CLICK'},
                    synchronize_session=False)
        session.commit()

def updateSEGMENTO_BD(TmpTable):
    
    with session.begin():
        session.query(TmpTable).filter(
                     TmpTable.SEGMENTO_BD == None).\
                    update({TmpTable.SEGMENTO_BD:UEN},
                    synchronize_session=False)
        session.commit()

def updateEMPRESA_BD(TmpTable):
    ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
    ###### algo falla
    with session.begin():
        session.query(TmpTable).filter(
                     TmpTable.DESCRIPCION_INTERFAZ == 'EDATEL').\
                    update({TmpTable.EMPRESA_BD :'EDA'},
                    synchronize_session=False)
        session.commit()

def BD_GOWithDup(TmpTable):
    ###### Inicia la session de manera que todo lo que este identado dentro de este with haga rollback automaticamente si 
    ###### algo falla
    with session.begin():
        Pedidos=findByEstadoGeneralErrorDescripcionPedido_Id('SIN GESTION','%Dup%',TmpTable)

        lista=[]
        for pedido in Pedidos:
            lista.append(pedido[0].replace('-Dup',''))
        list=tuple((lista))
        listaAInsertar=[]
        if len(list)>0:
            dataLookUpDataAprov=BD_CS_GestionOperativa.lookUpDataAprovWithDup(list)
            for GestionOperativaDatos in dataLookUpDataAprov:
                pedido=session.query(TmpTable).filter(TmpTable.PEDIDO_ID==GestionOperativaDatos.PEDIDO_UNE+'-Dup').one_or_none() 
                if (pedido!=None):
                    # pedido.PROCESO_BD=GestionOperativaDatos.PROCESO
                    # pedido.EMPRESA_BD=GestionOperativaDatos.EMPRESA_ID
                    # pedido.COD_FUNCIONARIO=GestionOperativaDatos.COD_FUNCIONARIO
                    # pedido.NOMBRE_FUNCIONARIO=GestionOperativaDatos.NOMBRE_FUNCIONARIO
                    # pedido.DESCRIPCION_INTERFAZ=GestionOperativaDatos.DESCRIPCION_INTERFAZ
                    # pedido.REGIONAL_BD=GestionOperativaDatos.REGION
                    # listaAInsertar.append(pedido)
                    session.execute('''update TmpPremisasOff_HG_Aprov set PROCESO_BD='{0}',
                                                                        EMPRESA_BD='{1}',
                                                                        COD_FUNCIONARIO='{2}',
                                                                        NOMBRE_FUNCIONARIO='{3}',
                                                                        DESCRIPCION_INTERFAZ='{4}',
                                                                        REGIONAL_BD='{5}' where PEDIDO_ID ='{6}' '''.format(GestionOperativaDatos.PROCESO,
                                                                                                        GestionOperativaDatos.EMPRESA_ID,
                                                                                                        GestionOperativaDatos.COD_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.NOMBRE_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.DESCRIPCION_INTERFAZ,
                                                                                                        GestionOperativaDatos.REGION,
                                                                                                        GestionOperativaDatos.PEDIDO_UNE))
                    try:
                        lista.remove(GestionOperativaDatos.PEDIDO_UNE)
                    except:
                        pass
                
            for pedido_id  in lista:
                GestionOperativaDatos=BD_CS_GestionOperativa.lookUp("'"+str(pedido_id)+"'")
                if(GestionOperativaDatos!=None):
                    
                    # pedido=session.query(TmpPremisasOff_HG_Aprov).filter(TmpPremisasOff_HG_Aprov.PEDIDO_ID==pedido_id+'-Dup').one_or_none() 
                    # pedido.PROCESO_BD=GestionOperativaDatos.PROCESO
                    # pedido.EMPRESA_BD=GestionOperativaDatos.EMPRESA_ID
                    # pedido.COD_FUNCIONARIO=GestionOperativaDatos.COD_FUNCIONARIO
                    # pedido.NOMBRE_FUNCIONARIO=GestionOperativaDatos.NOMBRE_FUNCIONARIO
                    # pedido.DESCRIPCION_INTERFAZ=GestionOperativaDatos.DESCRIPCION_INTERFAZ
                    # pedido.REGIONAL_BD=GestionOperativaDatos.REGION
                    # listaAInsertar.append(pedido)
                    session.execute('''update TmpPremisasOff_HG_Aprov set PROCESO_BD='{0}',
                                                                        EMPRESA_BD='{1}',
                                                                        COD_FUNCIONARIO='{2}',
                                                                        NOMBRE_FUNCIONARIO='{3}',
                                                                        DESCRIPCION_INTERFAZ='{4}',
                                                                        REGIONAL_BD='{5}' where PEDIDO_ID ='{6}' '''.format(GestionOperativaDatos.PROCESO,
                                                                                                        GestionOperativaDatos.EMPRESA_ID,
                                                                                                        GestionOperativaDatos.COD_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.NOMBRE_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.DESCRIPCION_INTERFAZ,
                                                                                                        GestionOperativaDatos.REGION,
                                                                                                        GestionOperativaDatos.PEDIDO_UNE))
              
            ######  Agrego la lista a la session para guardarlos en la base de datos y con el commit hago persistencia en la base de datos
            # session.bulk_save_objects(listaAInsertar)
        session.commit()

def findByEstadoGeneralErrorDescripcionPedido_Id(estado_General:str,ErrorDescripcion:str,TmpTable):
    
    
    params=session.query(TmpTable.PEDIDO_ID).filter(TmpTable.ESTADO_GENERAL != estado_General,TmpTable.ERROR_DESCRIPCION.ilike(ErrorDescripcion)).all()
    
    return params            

def lookUpDataAprovWithDup (PEDIDO_ID:str):
    
    
    datos = session.execute('''select  F.PEDIDO_UNE,DE.EMPRESA_ID, DTT.PROCESO, DTT.TIPO_TRABAJO,DG.REGION,DG.DEPARTAMENTO_DANE,DG.MUNICIPIO_DANE,
                                DT.COD_FUNCIONARIO,DT.NOMBRE_FUNCIONARIO, FECHA_DESPACHO,ESTADO_CLICK,DI.DESCRIPCION_INTERFAZ 
                                from FacPedidos_SSMM F left join DimEmpresas DE on DE.ID_EMPRESA = F.ID_EMPRESA
                                left join DimTipoTrabajo DTT on F.ID_TRABAJO = DTT.ID_TRABAJO
                                left join DimGeografiaSSMM DGS on F.ID_GEOGRAFIA = DGS.ID_GEOGRAFIA
                                inner join DimGeografia DG on DGS.MUNICIPIO_ID = DG.MUNICIPIO_ID
                                LEFT JOIN DimTecnicos DT ON DT.ID_TECNICO = F.ID_TECNICO
                                left join  DimInterfaz DI on DI.ID_INTERFAZ = F.ID_INTERFAZ where proceso in ('Aprovisionamiento' ,'Aseguramiento','Aseguramiento B2B','Aprovisionamiento B2B')
                                and DTT.PROCESO_ACTA in ('Provision','Soporte')
                                and FECHA_CIERRE between getdate()-45 and GETDATE()
                                and F.PEDIDO_UNE in {0} '''.format(str(PEDIDO_ID))) 
        
    return datos.fetchall()
    



def BD_GO(TmpTable):
    
    with session.begin():
        
        listaAInsertar=[]
       
        Pedidos=findByEstadoGeneralGetPedido_Id('SIN GESTION',TmpTable)
        lista=[]
        for pedido in Pedidos:
            lista.append(pedido[0])
        
    
        list=tuple((lista))
         
        if len(list)>0:
            dataLookUpDataAprov=BD_CS_GestionOperativa.lookUpDataAprov(list) 
            for GestionOperativaDatos in dataLookUpDataAprov:
                pedido=session.query(TmpTable).filter(TmpTable.PEDIDO_ID==GestionOperativaDatos.PEDIDO_UNE).one_or_none() 
                if (pedido!=None):
                    # pedido.PROCESO_BD=GestionOperativaDatos.PROCESO
                    # pedido.EMPRESA_BD=GestionOperativaDatos.EMPRESA_ID
                    # pedido.COD_FUNCIONARIO=GestionOperativaDatos.COD_FUNCIONARIO
                    # pedido.NOMBRE_FUNCIONARIO=GestionOperativaDatos.NOMBRE_FUNCIONARIO
                    # pedido.DESCRIPCION_INTERFAZ=GestionOperativaDatos.DESCRIPCION_INTERFAZ
                    # pedido.REGIONAL_BD=GestionOperativaDatos.REGION
                    session.execute('''update TmpPremisasOff_HG_Aprov set PROCESO_BD='{0}',
                                                                        EMPRESA_BD='{1}',
                                                                        COD_FUNCIONARIO='{2}',
                                                                        NOMBRE_FUNCIONARIO='{3}',
                                                                        DESCRIPCION_INTERFAZ='{4}',
                                                                        REGIONAL_BD='{5}' where PEDIDO_ID ='{6}' '''.format(GestionOperativaDatos.PROCESO,
                                                                                                        GestionOperativaDatos.EMPRESA_ID,
                                                                                                        GestionOperativaDatos.COD_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.NOMBRE_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.DESCRIPCION_INTERFAZ,
                                                                                                        GestionOperativaDatos.REGION,
                                                                                                        GestionOperativaDatos.PEDIDO_UNE)) 
                    # listaAInsertar.append(pedido)
                
                    try:
                        lista.remove(GestionOperativaDatos.PEDIDO_UNE)
                    except:
                        pass

            
            for pedido_id  in lista:
                GestionOperativaDatos=BD_CS_GestionOperativa.lookUp("'"+str(pedido_id)+"'")
                if(GestionOperativaDatos!=None):
                    # pedido=session.query(TmpPremisasOff_HG_Aprov).filter(TmpPremisasOff_HG_Aprov.PEDIDO_ID==pedido_id).one_or_none() 
                    # pedido.PROCESO_BD=GestionOperativaDatos.PROCESO
                    # pedido.EMPRESA_BD=GestionOperativaDatos.EMPRESA_ID
                    # pedido.COD_FUNCIONARIO=GestionOperativaDatos.COD_FUNCIONARIO
                    # pedido.NOMBRE_FUNCIONARIO=GestionOperativaDatos.NOMBRE_FUNCIONARIO
                    # pedido.DESCRIPCION_INTERFAZ=GestionOperativaDatos.DESCRIPCION_INTERFAZ
                    # pedido.REGIONAL_BD=GestionOperativaDatos.REGION
                    # listaAInsertar.append(pedido)
                    session.execute('''update TmpPremisasOff_HG_Aprov set PROCESO_BD='{0}',
                                                                        EMPRESA_BD='{1}',
                                                                        COD_FUNCIONARIO='{2}',
                                                                        NOMBRE_FUNCIONARIO='{3}',
                                                                        DESCRIPCION_INTERFAZ='{4}',
                                                                        REGIONAL_BD='{5}' where PEDIDO_ID ='{6}' '''.format(GestionOperativaDatos.PROCESO,
                                                                                                        GestionOperativaDatos.EMPRESA_ID,
                                                                                                        GestionOperativaDatos.COD_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.NOMBRE_FUNCIONARIO,
                                                                                                        GestionOperativaDatos.DESCRIPCION_INTERFAZ,
                                                                                                        GestionOperativaDatos.REGION,
                                                                                                        GestionOperativaDatos.PEDIDO_UNE))
             
               
            ######  Agrego la lista a la session para guardarlos en la base de datos y con el commit hago persistencia en la base de datos
            # session.bulk_save_objects(listaAInsertar)
        session.commit()

def findByEstadoGeneralGetPedido_Id(estado_General:str,TmpTable):
    
    
    params=session.query(TmpTable.PEDIDO_ID).filter(TmpTable.ESTADO_GENERAL != estado_General).all()
    
    return params

def lookUpDataAprov (PEDIDOS_ID:str):

    
    datos = session.execute('''select  F.PEDIDO_UNE,DE.EMPRESA_ID, DTT.PROCESO, DTT.TIPO_TRABAJO,DG.REGION,DG.DEPARTAMENTO_DANE,DG.MUNICIPIO_DANE,
                                DT.COD_FUNCIONARIO,DT.NOMBRE_FUNCIONARIO, FECHA_DESPACHO,ESTADO_CLICK,DI.DESCRIPCION_INTERFAZ
                                from FacPedidos_SSMM F left join DimEmpresas DE on DE.ID_EMPRESA = F.ID_EMPRESA
                                left join DimTipoTrabajo DTT on F.ID_TRABAJO = DTT.ID_TRABAJO
                                left join DimGeografiaSSMM DGS on F.ID_GEOGRAFIA = DGS.ID_GEOGRAFIA
                                left join DimGeografia DG on DGS.MUNICIPIO_ID = DG.MUNICIPIO_ID
                                LEFT JOIN DimTecnicos DT ON DT.ID_TECNICO = F.ID_TECNICO
                                left join  DimInterfaz DI on DI.ID_INTERFAZ = F.ID_INTERFAZ
                                where  DTT.PROCESO_ACTA in ('Provision','Soporte')
                                and ESTADO_CLICK = 'Finalizada'
                                and FECHA_CIERRE between getdate()-45 and GETDATE()
                                and F.PEDIDO_UNE in {0}'''.format(str(PEDIDOS_ID))) 
    return datos.fetchall()

def lookUp (PEDIDO_ID:str):
    
    
    datos = session.execute('''select  F.PEDIDO_UNE,DE.EMPRESA_ID, DTT.PROCESO, DTT.TIPO_TRABAJO,DG.REGION,DG.DEPARTAMENTO_DANE,DG.MUNICIPIO_DANE,
                                    DT.COD_FUNCIONARIO,DT.NOMBRE_FUNCIONARIO, FECHA_CIERRE,ESTADO_CLICK,DTT.PROCESO_ACTA,DI.DESCRIPCION_INTERFAZ
                                    from FacPedidos_SSMM F left join DimEmpresas DE on DE.ID_EMPRESA = F.ID_EMPRESA
                                    left join DimTipoTrabajo DTT on F.ID_TRABAJO = DTT.ID_TRABAJO
                                    left join DimGeografiaSSMM DGS on F.ID_GEOGRAFIA = DGS.ID_GEOGRAFIA
                                    inner join DimGeografia DG on DGS.MUNICIPIO_ID = DG.MUNICIPIO_ID
                                    LEFT JOIN DimTecnicos DT ON DT.ID_TECNICO = F.ID_TECNICO left join  DimInterfaz DI on DI.ID_INTERFAZ = F.ID_INTERFAZ
                                    where F.PEDIDO_UNE = {0}'''.format(str(PEDIDO_ID))) 
        
    return datos.fetchone()
    


def main(TmpTable):
    BD_GO(TmpTable)
    BD_GOWithDup(TmpTable)
    updateFUENTE(TmpTable)
    updateSEGMENTO_BD(TmpTable)
    updateEMPRESA_BD(TmpTable)


