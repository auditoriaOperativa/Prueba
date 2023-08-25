from datetime import datetime
from Util import Functions
from Modules import Audit_Units as mod_AU
from Modules import Audit_Test as mod_AT
from Models.base import Base
from Models import base
from Modules import PopulationAndQuality_AU as PopAndQua_AU
from Modules import PopulationAndQuality_AT as PopAndQua_AT
from Modules import MovePhotos as mod_MovPh
from Modules import Integration_Data_with_BDs as mod_Int_BD
from Modules import MoveAU_FromTmpToFact as mod_AU_TmpToFAct
from Modules import MoveAT_FromTmpToFact as mod_AT_TmpToFAct
from Modules import DeleteFolders as mod_delFold


###### Obtenemos las clases mapeadas por el ORM desde la base para utilizarlas en los CRUD
LOGINFO = Base.classes.LOGINFO
DimParametros2 = Base.classes.DimParametros2
strDimParametros2 = base.strDimParametros2
DimCampos = Base.classes.DimCampos

TmpTable = base.TmpTable
TmpTableParam = base.TmpTableParam
FactTable = base.FactTable
TableParam = base.TableParam

strTmpTable = base.strTmpTable
strTmpTableParam = base.strTmpTableParam
strFactTable = base.strFactTable
strTableParam = base.strTableParam

#import win32api 

def main():
    try:
        
        ##usuario = win32api.GetUserName()
        readingDate = Functions.yesterday()

        mod_AU.main(readingDate,strTmpTable,TmpTable,DimCampos)
        mod_AT.main(readingDate,strTmpTableParam,TmpTable,TmpTableParam,DimParametros2)
        PopAndQua_AU.main(TmpTable,FactTable,strTmpTable,strFactTable,readingDate)
        PopAndQua_AT.main(TmpTable,TmpTableParam,DimParametros2,strTmpTable,strTmpTableParam,strDimParametros2,readingDate)

        #load the data of the Audits not executed to template file 
        #mod_AU.LoadAuditsUnits(TmpTable,DimCampos)
        mod_Int_BD.main(TmpTable)
        mod_AU_TmpToFAct.main(TmpTable,FactTable,DimCampos,TmpTableParam,TableParam,strFactTable,strTmpTableParam)

        mod_AT_TmpToFAct.main(TmpTable,strTmpTable,FactTable,DimCampos,TmpTableParam,TableParam,strFactTable,strTmpTableParam,readingDate,DimParametros2)
        #mod_MovPh.movePhotos(readingDate)
        #mod_delFold.deleteFolder()
        

    except Exception as e:
        print(e)
        
    
if __name__ == "__main__":
    main()

