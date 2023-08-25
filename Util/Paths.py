# -*- coding: utf-8 -*-
#Este script de python contiene todas las paths que pueden llegar a ser utilizadas en la ejecucion del proyecto
import os
from Util import Config
config=Config.readIni()   
pathFromCloudAS = config['PATHS']['pathFromCloudAS'].replace("/",os.sep)
pathAuthConsolidados = config['PATHS']['pathAuthConsolidados'].replace("/",os.sep)
pathAuditoriaPremisas_Hog = config['PATHS']['pathAuditoriaPremisas_Hog'].replace("/",os.sep)
pathConsolidados = config['PATHS']['pathConsolidados'].replace("/",os.sep)
pathOneDrive = config['ONEDRIVE']['pathOneDrive'].replace("/",os.sep)


#it is defined the indentifier audit unit
AU_Identifier = 'PEDIDO_ID'


appSheetFolderName = 'AppSheet_Premisas'
PkgName = 'Pkg_PremisasOff_HG_Aprov'

excelName = 'PremisasHG_APROV_OFFLINE.xlsx'
pedidoExcelSheet = 'HG_APROV_OFFLINE'
materialesExcelSheet = 'MAT_HG_APROV'

imagesFolderName = "PremisasHG_APROV_OFFLINE_Images/"
version = 'V6'
UEN ='HG'
PROCESO = 'PREMISAS'
SUBPROCESO = 'APROVISIONAMIENTO'
TIPO_AUDITORIA = 'NO PRESENCIAL'


vsMateriales = 'V1'
SUBPROCESOMateriales = 'MATERIALES APROVISIONAMIENTO'

vsParametros = 'V1'
SUBPROCESOParametros = 'PARAMETROS APROVISIONAMIENTO'
