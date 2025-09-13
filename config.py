"""
Configuration module for Ship Lineup Data Pipeline
"""
import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the application"""
    
    # Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///ship_lineup.db')
    
    # Data storage paths
    BASE_DATA_PATH = Path('./data')
    BRONZE_DATA_PATH = BASE_DATA_PATH / 'bronze'
    SILVER_DATA_PATH = BASE_DATA_PATH / 'silver'
    GOLD_DATA_PATH = BASE_DATA_PATH / 'gold'
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', './logs/ship_lineup.log')
    
    # API configuration
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    
    # Data sources
    DATA_SOURCES = {
        'paranagua': {
            'url': 'https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo',
            'name': 'Porto de Paranaguá',
            'code': 'PAR'
        },
        'santos': {
            'url': 'https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/',
            'name': 'Porto de Santos',
            'code': 'STS'
        }
    }
    
    # Create directories if they don't exist
    @classmethod
    def create_directories(cls):
        """Create necessary directories"""
        directories = [
            cls.BASE_DATA_PATH,
            cls.BRONZE_DATA_PATH,
            cls.SILVER_DATA_PATH,
            cls.GOLD_DATA_PATH,
            Path('./logs')
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    # Data validation rules
    VALIDATION_RULES = {
        'required_columns': [
            'porto', 'navio', 'produto', 'sentido', 'volume', 'data_chegada'
        ],
        'valid_ports': ['PARANAGUÁ', 'SANTOS'],
        'valid_directions': ['EXPORTAÇÃO', 'IMPORTAÇÃO', 'AMBOS'],
        'min_volume': 0,
        'max_volume': 10000000  # 10 million tons (more realistic)
    }

