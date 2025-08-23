from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import os

def get_atls_engine():
    """Get ATLS database engine with connection pooling"""
    database_url = os.getenv('ATLS_DATABASE_URL', 'postgresql://admin:admin@localhost:5432/atls')
    return create_engine(
        database_url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800  # Recycle connections after 30 minutes
    )

def get_adm_engine():
    """Get ADM database engine with connection pooling"""
    database_url = os.getenv('ADM_DATABASE_URL', 'postgresql://admin:admin@localhost:5432/adm')
    return create_engine(
        database_url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )