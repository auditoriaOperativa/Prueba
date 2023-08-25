from Models.baseGerencia import Base,session

def lookUpDataAprov (PEDIDOS_ID:str):

    with session.begin():
    
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
    

