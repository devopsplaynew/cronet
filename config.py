import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    SQLALCHEMY_DATABASE_URI = os.environ.get('POSTGRES_URI')
    SQLALCHEMY_BINDS = {
        'oracle': os.environ.get('ORACLE_URI'),
        'impala': os.environ.get('IMPALA_URI')
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_size': 5,
    'max_overflow': 10,
    'pool_timeout': 30,
    'pool_recycle': 3600
}