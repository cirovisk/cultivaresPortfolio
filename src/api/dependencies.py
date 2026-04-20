from fastapi import Depends
from sqlalchemy.orm import Session
from db.manager import get_db

def get_session(db: Session = Depends(get_db)) -> Session:
    return db
