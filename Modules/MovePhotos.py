from datetime import datetime
import os
from Util import Functions
from Util.Paths import pathFromCloudAS,pathAuditoriaPremisas_Hog,pathConsolidados,pathAuthConsolidados,appSheetFolderName,imagesFolderName
import os
from Modules import log
absFilePath = os.path.abspath(__file__)
path, nombreController = os.path.split(absFilePath)


def movePhotos(date:datetime):
    try:

        ###### Dynamically create the source path with the one from a day ago.
        PathFromCloudAS=os.path.sep.join([pathFromCloudAS,"{0}","{0}_{1}_{2}_{3}", imagesFolderName]).format(appSheetFolderName,str(date.day),str(date.month),str(date.year))

        ###### Dynamically create the source path with the month and year from date of the day ago.
        PathAuthConsolidados=os.path.sep.join([pathAuthConsolidados,"Consolidado"+str(Functions.monthName(date.month))+str(date.year), imagesFolderName+os.sep])
        #PathAuditoriaPremisas_Hog=os.path.sep.join([pathAuditoriaPremisas_Hog,"{0}","{1}_{2}", imagesFolderName+os.sep]).format(str(date.year),str(date.month),str(Functions.monthName(date.month)))
        #PathConsolidados=os.path.sep.join([pathConsolidados,"Consolidado_{0}_{1}", imagesFolderName+os.sep]).format(str(Functions.monthName(date.month)),str(date.year))
        

        
        
        #the photos are moved from  "CopyFromToLocal" to AutoConsolidados Folder in BackupAppsheet folder
        Functions.CopyFolderFromTo(PathFromCloudAS,PathAuthConsolidados,"MovePhotos")
        #the photos are moved from  "CopyFromToLocal" to Compartida folder
        #Functions.CopyFolderFromTo(PathFromCloudAS,PathAuditoriaPremisas_Hog,"MovePhotos")
        #the photos are moved from  "CopyFromToLocal" to Condolodados Folder in BackupAppsheet folder
        #Functions.CopyFolderFromTo(PathFromCloudAS,PathConsolidados,"MovePhotos")
        

    except Exception as e:
        raise
