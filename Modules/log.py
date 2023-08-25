from sqlalchemy import and_, case, exists, select
from Models.base import Base,session
import pandas as pd

from datetime import datetime

LOGINFO = Base.classes.LOGINFO


def insertLogToDB(msg:str,tipo:str,source:str):
    try:
        with session.begin():
                    
            LOG=LOGINFO()
            LOG.Message=msg
            LOG.Date=datetime.today()
            LOG.Tipo=tipo
            LOG.Descripcion=source
            
            session.add(LOG)
            session.commit()

        return ''

    except Exception as e:
        pass


