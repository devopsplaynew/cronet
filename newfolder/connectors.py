from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def get_atls_engine():
    return create_engine(os.getenv('POSTGRES_URI', 'postgresql://admin:admin@localhost/atls'))

def get_adm_engine():
    return create_engine(os.getenv('ADM_POSTGRES_URI', 'postgresql://admin:admin@localhost/adm'))