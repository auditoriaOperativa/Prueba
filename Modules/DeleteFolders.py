import os
from Util.Paths import pathOneDrive,appSheetFolderName,imagesFolderName
from Util import Functions
from Modules import log
absFilePath = os.path.abspath(__file__)
path, nombreController = os.path.split(absFilePath)
def deleteFolder():
    try:

        PathFromCloudAS=os.path.sep.join([pathOneDrive, "appsheet", appSheetFolderName,imagesFolderName])
        
        Functions.deleteFolder(PathFromCloudAS)
        log.insertLogToDB(str(e),'Delete Folder','Failed execution in Mod Audit_Units')
    except Exception as e:
        log.insertLogToDB(str(e),'truncateTmpTable','Failed execution in Mod Audit_Units')
        raise