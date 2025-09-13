"""
Medallion Architecture Pipeline Implementation
Bronze -> Silver -> Gold layers for ship lineup data
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from loguru import logger
from config import Config


class MedallionPipeline:
    """Implements the medallion architecture for data processing"""
    
    def __init__(self):
        self.bronze_path = Config.BRONZE_DATA_PATH
        self.silver_path = Config.SILVER_DATA_PATH
        self.gold_path = Config.GOLD_DATA_PATH
        
        # Create directories
        Config.create_directories()
    
    def process_bronze_layer(self, df: pd.DataFrame, source: str, collection_date: str) -> str:
        """
        Process data into Bronze layer (raw data)
        
        Args:
            df: Raw DataFrame
            source: Data source identifier
            collection_date: Date of data collection
            
        Returns:
            Path to saved bronze file
        """
        logger.info(f"Processing {len(df)} records to Bronze layer from {source}")
        
        # Add metadata
        df['collection_date'] = collection_date
        df['source'] = source
        df['processing_timestamp'] = datetime.now()
        
        # Save to bronze layer
        filename = f"bronze_{source}_{collection_date}.parquet"
        filepath = self.bronze_path / filename
        
        df.to_parquet(filepath, index=False)
        logger.info(f"Bronze data saved to {filepath}")
        
        return str(filepath)
    
    def process_silver_layer(self, bronze_file: str) -> str:
        """
        Process data from Bronze to Silver layer (cleaned and standardized)
        
        Args:
            bronze_file: Path to bronze data file
            
            Returns:
            Path to saved silver file
        """
        logger.info(f"Processing Bronze to Silver: {bronze_file}")
        
        # Load bronze data
        df = pd.read_parquet(bronze_file)
        
        # Data cleaning and standardization
        df_cleaned = self._clean_data(df)
        df_standardized = self._standardize_data(df_cleaned)
        df_enriched = self._enrich_data(df_standardized)
        
        # Save to silver layer
        bronze_path = Path(bronze_file)
        silver_filename = bronze_path.name.replace('bronze_', 'silver_')
        silver_filepath = self.silver_path / silver_filename
        
        df_enriched.to_parquet(silver_filepath, index=False)
        logger.info(f"Silver data saved to {silver_filepath}")
        
        return str(silver_filepath)
    
    def process_gold_layer(self, silver_file: str) -> str:
        """
        Process data from Silver to Gold layer (business-ready)
        
        Args:
            silver_file: Path to silver data file
            
        Returns:
            Path to saved gold file
        """
        logger.info(f"Processing Silver to Gold: {silver_file}")
        
        # Load silver data
        df = pd.read_parquet(silver_file)
        
        # Business logic and aggregations
        df_business = self._apply_business_logic(df)
        df_aggregated = self._create_aggregations(df_business)
        df_final = self._create_final_metrics(df_aggregated)
        
        # Save to gold layer
        silver_path = Path(silver_file)
        gold_filename = silver_path.name.replace('silver_', 'gold_')
        gold_filepath = self.gold_path / gold_filename
        
        df_final.to_parquet(gold_filepath, index=False)
        logger.info(f"Gold data saved to {gold_filepath}")
        
        return str(gold_filepath)
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean raw data"""
        logger.info("Cleaning data...")
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        logger.info(f"Removed {initial_count - len(df)} duplicate records")
        
        # Handle missing values
        df = df.dropna(subset=['navio', 'produto', 'sentido'])
        
        # Clean text fields
        text_columns = ['navio', 'produto', 'armador', 'agente']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        
        # Clean numeric fields
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df = df[df['volume'] > 0]  # Remove zero or negative volumes
        
        return df
    
    def _standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data formats and values"""
        logger.info("Standardizing data...")
        
        # Standardize port names
        df['porto'] = df['porto'].str.upper()
        df['porto'] = df['porto'].replace({
            'PARANAGUA': 'PARANAGUÁ',
            'PAR': 'PARANAGUÁ',
            'SANTOS': 'SANTOS',
            'STS': 'SANTOS'
        })
        
        # Standardize direction values
        df['sentido'] = df['sentido'].str.upper()
        df['sentido'] = df['sentido'].replace({
            'EXP': 'EXPORTAÇÃO',
            'IMP': 'IMPORTAÇÃO',
            'EXPORT': 'EXPORTAÇÃO',
            'IMPORT': 'IMPORTAÇÃO',
            'OUTBOUND': 'EXPORTAÇÃO',
            'INBOUND': 'IMPORTAÇÃO'
        })
        
        # Standardize product names
        if 'produto' in df.columns:
            df['produto'] = df['produto'].str.upper()
            # Common product standardizations
            product_mapping = {
                'SOJA': 'SOJA',
                'MILHO': 'MILHO',
                'AÇÚCAR': 'AÇÚCAR',
                'SUGAR': 'AÇÚCAR',
                'CONTAINER': 'CONTAINER',
                'CONTÊINER': 'CONTAINER',
                'FERTILIZANTE': 'FERTILIZANTE',
                'FERTILIZER': 'FERTILIZANTE'
            }
            df['produto'] = df['produto'].replace(product_mapping)
        
        # Standardize date formats
        date_columns = ['data_chegada', 'data_partida']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich data with additional calculated fields"""
        logger.info("Enriching data...")
        
        # Add calculated fields
        if 'data_chegada' in df.columns:
            df['ano'] = df['data_chegada'].dt.year
            df['mes'] = df['data_chegada'].dt.month
            df['dia_semana'] = df['data_chegada'].dt.day_name()
            df['trimestre'] = df['data_chegada'].dt.quarter
        
        # Add ship type classification
        if 'navio' in df.columns:
            df['tipo_navio'] = df['navio'].apply(self._classify_ship_type)
        
        # Add product category
        if 'produto' in df.columns:
            df['categoria_produto'] = df['produto'].apply(self._classify_product_category)
        
        # Add volume category
        if 'volume' in df.columns:
            df['categoria_volume'] = pd.cut(
                df['volume'],
                bins=[0, 1000, 5000, 10000, float('inf')],
                labels=['Pequeno', 'Médio', 'Grande', 'Muito Grande']
            )
        
        return df
    
    def _apply_business_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business-specific logic and rules"""
        logger.info("Applying business logic...")
        
        # Add business rules
        df['status_operacao'] = 'ATIVO'
        
        # Flag potential data quality issues
        df['flag_qualidade'] = 'OK'
        
        # Check for unusual volumes
        if 'volume' in df.columns:
            volume_stats = df['volume'].describe()
            q1, q3 = volume_stats['25%'], volume_stats['75%']
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            df.loc[df['volume'] < lower_bound, 'flag_qualidade'] = 'VOLUME_BAIXO'
            df.loc[df['volume'] > upper_bound, 'flag_qualidade'] = 'VOLUME_ALTO'
        
        # Check for future dates
        if 'data_chegada' in df.columns:
            today = datetime.now().date()
            df.loc[df['data_chegada'].dt.date > today, 'flag_qualidade'] = 'DATA_FUTURA'
        
        return df
    
    def _create_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create aggregated metrics"""
        logger.info("Creating aggregations...")
        
        # Keep original records
        df_original = df.copy()
        
        # Create daily aggregations
        daily_agg = df.groupby(['porto', 'data_chegada', 'sentido']).agg({
            'volume': ['sum', 'count', 'mean'],
            'navio': 'nunique'
        }).round(2)
        
        daily_agg.columns = ['volume_total', 'qtd_operacoes', 'volume_medio', 'qtd_navios']
        daily_agg = daily_agg.reset_index()
        
        # Create product aggregations
        product_agg = df.groupby(['porto', 'produto', 'sentido']).agg({
            'volume': ['sum', 'count', 'mean'],
            'navio': 'nunique'
        }).round(2)
        
        product_agg.columns = ['volume_total', 'qtd_operacoes', 'volume_medio', 'qtd_navios']
        product_agg = product_agg.reset_index()
        
        # Combine with original data
        df_combined = pd.concat([df_original, daily_agg, product_agg], ignore_index=True)
        
        return df_combined
    
    def _create_final_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create final business metrics"""
        logger.info("Creating final metrics...")
        
        # Add performance indicators
        if 'volume' in df.columns and 'data_chegada' in df.columns:
            # Calculate moving averages
            df_sorted = df.sort_values('data_chegada')
            df_sorted['volume_ma_7d'] = df_sorted['volume'].rolling(window=7, min_periods=1).mean()
            df_sorted['volume_ma_30d'] = df_sorted['volume'].rolling(window=30, min_periods=1).mean()
            
            # Calculate growth rates
            df_sorted['crescimento_volume'] = df_sorted['volume'].pct_change()
            
            df = df_sorted
        
        # Add ranking metrics
        if 'volume' in df.columns:
            df['ranking_volume'] = df.groupby(['porto', 'sentido'])['volume'].rank(ascending=False)
        
        return df
    
    def _classify_ship_type(self, ship_name: str) -> str:
        """Classify ship type based on name"""
        ship_name = str(ship_name).upper()
        
        if any(keyword in ship_name for keyword in ['BULK', 'GRAIN', 'CARGO']):
            return 'CARGA_GERAL'
        elif any(keyword in ship_name for keyword in ['CONTAINER', 'BOX']):
            return 'CONTAINER'
        elif any(keyword in ship_name for keyword in ['TANKER', 'OIL', 'PETROLEUM']):
            return 'TANQUE'
        elif any(keyword in ship_name for keyword in ['RO-RO', 'FERRY']):
            return 'RO-RO'
        else:
            return 'OUTROS'
    
    def _classify_product_category(self, product: str) -> str:
        """Classify product into categories"""
        product = str(product).upper()
        
        if any(keyword in product for keyword in ['SOJA', 'MILHO', 'TRIGO', 'ARROZ']):
            return 'GRÃOS'
        elif any(keyword in product for keyword in ['AÇÚCAR', 'SUGAR']):
            return 'AÇÚCAR'
        elif any(keyword in product for keyword in ['FERTILIZANTE', 'FERTILIZER']):
            return 'FERTILIZANTES'
        elif any(keyword in product for keyword in ['CONTAINER', 'CONTÊINER']):
            return 'CONTAINER'
        elif any(keyword in product for keyword in ['MINÉRIO', 'ORE', 'IRON']):
            return 'MINÉRIOS'
        else:
            return 'OUTROS'
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processed data across all layers"""
        summary = {
            'bronze_files': len(list(self.bronze_path.glob('*.parquet'))),
            'silver_files': len(list(self.silver_path.glob('*.parquet'))),
            'gold_files': len(list(self.gold_path.glob('*.parquet'))),
            'last_processing': datetime.now().isoformat()
        }
        
        return summary

