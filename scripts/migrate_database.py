#!/usr/bin/env python3
"""
Database migration script to update schema for new columns
"""
import sqlite3
from pathlib import Path
from loguru import logger

def migrate_database():
    """Migrate database schema to support new columns"""
    
    db_path = Path("ship_lineup.db")
    
    if not db_path.exists():
        logger.info("Database doesn't exist, creating new one...")
        return
    
    logger.info("Starting database migration...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get current schema
        cursor.execute("PRAGMA table_info(bronze_ship_lineup)")
        bronze_columns = [row[1] for row in cursor.fetchall()]
        
        cursor.execute("PRAGMA table_info(silver_ship_lineup)")
        silver_columns = [row[1] for row in cursor.fetchall()]
        
        cursor.execute("PRAGMA table_info(gold_ship_lineup)")
        gold_columns = [row[1] for row in cursor.fetchall()]
        
        # Define new columns for each table
        new_bronze_columns = {
            'programacao': 'TEXT',
            'duv': 'TEXT', 
            'berco': 'TEXT',
            'imo': 'TEXT',
            'loa': 'TEXT',
            'dwt': 'TEXT',
            'bordo': 'TEXT',
            'agencia': 'TEXT',
            'operador': 'TEXT',
            'atracacao': 'TEXT',
            'janela_operacional': 'TEXT',
            'prancha_capacidade': 'TEXT',
            'tons_dia': 'TEXT',
            'volume_previsto': 'TEXT',
            'volume_realizado': 'TEXT',
            'saldo_operador': 'TEXT',
            'saldo_total': 'TEXT',
            'bandeira': 'TEXT',
            'comprimento_calado': 'TEXT',
            'navegacao': 'TEXT',
            'carimbo': 'TEXT',
            'viagem': 'TEXT',
            'prioridade': 'TEXT',
            'terminal': 'TEXT',
            'observacoes': 'TEXT',
            'tipo_carga': 'TEXT',
            'col_21': 'TEXT',
            'porto_codigo': 'TEXT',
            'data_coleta': 'TEXT',
            'fonte': 'TEXT',
            'volume': 'TEXT',
            'collection_date': 'TEXT',
            'source': 'TEXT',
            'processing_timestamp': 'TEXT'
        }
        
        new_silver_columns = {
            **new_bronze_columns,
            'ano': 'INTEGER',
            'mes': 'INTEGER', 
            'dia_semana': 'TEXT',
            'trimestre': 'INTEGER',
            'tipo_navio': 'TEXT',
            'categoria_produto': 'TEXT',
            'categoria_volume': 'TEXT',
            'status_operacao': 'TEXT',
            'flag_qualidade': 'TEXT'
        }
        
        new_gold_columns = {
            **new_silver_columns,
            'volume_total': 'REAL',
            'qtd_operacoes': 'INTEGER',
            'volume_medio': 'REAL',
            'qtd_navios': 'INTEGER',
            'volume_ma_7d': 'REAL',
            'volume_ma_30d': 'REAL',
            'crescimento_volume': 'REAL',
            'ranking_volume': 'REAL'
        }
        
        # Add missing columns to bronze table
        for col_name, col_type in new_bronze_columns.items():
            if col_name not in bronze_columns:
                logger.info(f"Adding column {col_name} to bronze_ship_lineup")
                cursor.execute(f"ALTER TABLE bronze_ship_lineup ADD COLUMN {col_name} {col_type}")
        
        # Add missing columns to silver table
        for col_name, col_type in new_silver_columns.items():
            if col_name not in silver_columns:
                logger.info(f"Adding column {col_name} to silver_ship_lineup")
                cursor.execute(f"ALTER TABLE silver_ship_lineup ADD COLUMN {col_name} {col_type}")
        
        # Add missing columns to gold table
        for col_name, col_type in new_gold_columns.items():
            if col_name not in gold_columns:
                logger.info(f"Adding column {col_name} to gold_ship_lineup")
                cursor.execute(f"ALTER TABLE gold_ship_lineup ADD COLUMN {col_name} {col_type}")
        
        conn.commit()
        logger.info("Database migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()
