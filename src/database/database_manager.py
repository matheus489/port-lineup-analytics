"""
Database manager for ship lineup data storage and retrieval
"""
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Float, DateTime, Integer
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from loguru import logger
from config import Config


class DatabaseManager:
    """Manages database operations for ship lineup data"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        
        # Create tables if they don't exist
        self._create_tables()
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        try:
            # Bronze layer table
            self.bronze_table = Table(
                'bronze_ship_lineup',
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('porto', String(50)),
                Column('navio', String(100)),
                Column('produto', String(100)),
                Column('sentido', String(20)),
                Column('volume', Float),
                Column('data_chegada', DateTime),
                Column('data_partida', DateTime),
                Column('armador', String(100)),
                Column('agente', String(100)),
                Column('collection_date', DateTime),
                Column('source', String(50)),
                Column('processing_timestamp', DateTime),
                Column('created_at', DateTime, default=datetime.now)
            )
            
            # Silver layer table
            self.silver_table = Table(
                'silver_ship_lineup',
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('porto', String(50)),
                Column('navio', String(100)),
                Column('produto', String(100)),
                Column('sentido', String(20)),
                Column('volume', Float),
                Column('data_chegada', DateTime),
                Column('data_partida', DateTime),
                Column('armador', String(100)),
                Column('agente', String(100)),
                Column('ano', Integer),
                Column('mes', Integer),
                Column('dia_semana', String(20)),
                Column('trimestre', Integer),
                Column('tipo_navio', String(50)),
                Column('categoria_produto', String(50)),
                Column('categoria_volume', String(20)),
                Column('status_operacao', String(20)),
                Column('flag_qualidade', String(20)),
                Column('collection_date', DateTime),
                Column('source', String(50)),
                Column('processing_timestamp', DateTime),
                Column('created_at', DateTime, default=datetime.now)
            )
            
            # Gold layer table
            self.gold_table = Table(
                'gold_ship_lineup',
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('porto', String(50)),
                Column('navio', String(100)),
                Column('produto', String(100)),
                Column('sentido', String(20)),
                Column('volume', Float),
                Column('data_chegada', DateTime),
                Column('data_partida', DateTime),
                Column('armador', String(100)),
                Column('agente', String(100)),
                Column('ano', Integer),
                Column('mes', Integer),
                Column('dia_semana', String(20)),
                Column('trimestre', Integer),
                Column('tipo_navio', String(50)),
                Column('categoria_produto', String(50)),
                Column('categoria_volume', String(20)),
                Column('status_operacao', String(20)),
                Column('flag_qualidade', String(20)),
                Column('volume_ma_7d', Float),
                Column('volume_ma_30d', Float),
                Column('crescimento_volume', Float),
                Column('ranking_volume', Float),
                Column('collection_date', DateTime),
                Column('source', String(50)),
                Column('processing_timestamp', DateTime),
                Column('created_at', DateTime, default=datetime.now)
            )
            
            # Create tables
            self.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
    
    def insert_bronze_data(self, df: pd.DataFrame) -> int:
        """Insert data into bronze layer"""
        try:
            df.to_sql('bronze_ship_lineup', self.engine, if_exists='append', index=False)
            logger.info(f"Inserted {len(df)} records into bronze layer")
            return len(df)
        except Exception as e:
            logger.error(f"Error inserting bronze data: {e}")
            return 0
    
    def insert_silver_data(self, df: pd.DataFrame) -> int:
        """Insert data into silver layer"""
        try:
            df.to_sql('silver_ship_lineup', self.engine, if_exists='append', index=False)
            logger.info(f"Inserted {len(df)} records into silver layer")
            return len(df)
        except Exception as e:
            logger.error(f"Error inserting silver data: {e}")
            return 0
    
    def insert_gold_data(self, df: pd.DataFrame) -> int:
        """Insert data into gold layer"""
        try:
            df.to_sql('gold_ship_lineup', self.engine, if_exists='append', index=False)
            logger.info(f"Inserted {len(df)} records into gold layer")
            return len(df)
        except Exception as e:
            logger.error(f"Error inserting gold data: {e}")
            return 0
    
    def get_latest_collection_date(self, source: str) -> Optional[datetime]:
        """Get the latest collection date for a source"""
        try:
            query = text("""
                SELECT MAX(collection_date) as latest_date 
                FROM bronze_ship_lineup 
                WHERE source = :source
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"source": source}).fetchone()
                return result[0] if result and result[0] else None
                
        except Exception as e:
            logger.error(f"Error getting latest collection date: {e}")
            return None
    
    def get_data_by_date_range(self, start_date: str, end_date: str, layer: str = 'gold') -> pd.DataFrame:
        """Get data by date range from specified layer"""
        try:
            table_name = f"{layer}_ship_lineup"
            query = f"""
                SELECT * FROM {table_name} 
                WHERE data_chegada BETWEEN :start_date AND :end_date
                ORDER BY data_chegada DESC
            """
            
            df = pd.read_sql(query, self.engine, params={
                "start_date": start_date,
                "end_date": end_date
            })
            
            logger.info(f"Retrieved {len(df)} records from {layer} layer")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving data: {e}")
            return pd.DataFrame()
    
    def get_aggregated_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get aggregated data for reporting"""
        try:
            query = text("""
                SELECT 
                    porto,
                    sentido,
                    produto,
                    COUNT(*) as qtd_operacoes,
                    SUM(volume) as volume_total,
                    AVG(volume) as volume_medio,
                    COUNT(DISTINCT navio) as qtd_navios,
                    MIN(data_chegada) as primeira_chegada,
                    MAX(data_chegada) as ultima_chegada
                FROM gold_ship_lineup 
                WHERE data_chegada BETWEEN :start_date AND :end_date
                GROUP BY porto, sentido, produto
                ORDER BY volume_total DESC
            """)
            
            df = pd.read_sql(query, self.engine, params={
                "start_date": start_date,
                "end_date": end_date
            })
            
            logger.info(f"Retrieved aggregated data: {len(df)} groups")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving aggregated data: {e}")
            return pd.DataFrame()
    
    def get_data_quality_report(self) -> pd.DataFrame:
        """Get data quality report"""
        try:
            query = text("""
                SELECT 
                    source,
                    flag_qualidade,
                    COUNT(*) as qtd_registros,
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY source) as percentual
                FROM gold_ship_lineup 
                WHERE created_at >= datetime('now', '-30 days')
                GROUP BY source, flag_qualidade
                ORDER BY source, qtd_registros DESC
            """)
            
            df = pd.read_sql(query, self.engine)
            logger.info("Generated data quality report")
            return df
            
        except Exception as e:
            logger.error(f"Error generating data quality report: {e}")
            return pd.DataFrame()
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old data from bronze layer"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Delete old bronze data
            query = text("""
                DELETE FROM bronze_ship_lineup 
                WHERE created_at < :cutoff_date
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"cutoff_date": cutoff_date})
                conn.commit()
                logger.info(f"Cleaned up {result.rowcount} old bronze records")
                
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            stats = {}
            
            # Count records in each layer
            for layer in ['bronze', 'silver', 'gold']:
                query = f"SELECT COUNT(*) FROM {layer}_ship_lineup"
                with self.engine.connect() as conn:
                    result = conn.execute(text(query)).fetchone()
                    stats[f"{layer}_count"] = result[0] if result else 0
            
            # Get date range
            query = text("""
                SELECT 
                    MIN(data_chegada) as earliest_date,
                    MAX(data_chegada) as latest_date
                FROM gold_ship_lineup
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchone()
                if result:
                    stats['earliest_date'] = result[0]
                    stats['latest_date'] = result[1]
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()
        logger.info("Database connection closed")

