"""
Data collector for Porto de Santos
"""
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, List, Any
from datetime import datetime, timedelta
from loguru import logger
from .base_collector import BaseCollector


class SantosCollector(BaseCollector):
    """Collector for Porto de Santos data"""
    
    def __init__(self):
        super().__init__("SANTOS", "STS")
        self.base_url = "https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/"
    
    def collect_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Collect lineup data from Santos port website
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with collected data
        """
        logger.info(f"Collecting data from Santos from {start_date} to {end_date}")
        
        try:
            # Get the main page
            response = self.make_request(self.base_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for data tables or API endpoints
            df = self._extract_data_from_page(soup)
            
            # If no data found on main page, try alternative approaches
            if df.empty:
                df = self._try_alternative_data_sources()
            
            # Standardize and validate
            df = self.standardize_data(df)
            df = self.validate_data(df)
            
            logger.info(f"Successfully collected {len(df)} records from Santos")
            return df
            
        except Exception as e:
            logger.error(f"Error collecting data from Santos: {e}")
            return pd.DataFrame()
    
    def _extract_data_from_page(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Extract data from the main page"""
        # Look for tables with ship data
        tables = soup.find_all('table')
        
        logger.info(f"Found {len(tables)} tables on Santos page")
        
        all_dataframes = []
        
        for i, table in enumerate(tables):
            # Check if this looks like a ship data table
            headers = self._extract_table_headers(table)
            logger.info(f"Table {i+1} headers: {headers}")
            
            if self._is_ship_data_table(headers):
                logger.info(f"Found ship data table at index {i}")
                df = self._parse_table_data(table, headers)
                if not df.empty:
                    all_dataframes.append(df)
        
        # Combine all dataframes if we found any
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            logger.info(f"Combined {len(all_dataframes)} tables into {len(combined_df)} total records")
            return combined_df
        
        # Look for data in other formats (JSON, CSV links, etc.)
        return self._extract_alternative_data_formats(soup)
    
    def _extract_table_headers(self, table) -> List[str]:
        """Extract headers from a table"""
        headers = []
        header_row = table.find('tr')
        if header_row:
            for th in header_row.find_all(['th', 'td']):
                headers.append(th.get_text(strip=True))
        return headers
    
    def _is_ship_data_table(self, headers: List[str]) -> bool:
        """Check if headers indicate this is a ship data table"""
        # Check for ship-related keywords
        ship_keywords = ['navio', 'ship', 'vessel', 'produto', 'cargo', 'volume', 'sentido']
        header_text = ' '.join(headers).lower()
        
        # Also check for cargo type headers (like the ones we found)
        cargo_types = ['liquido', 'granel', 'trigo', 'grains', 'container', 'conteiners', 'roll', 'lash', 'cabotagem']
        
        return (any(keyword in header_text for keyword in ship_keywords) or 
                any(cargo_type in header_text for cargo_type in cargo_types))
    
    def _parse_table_data(self, table, headers: List[str]) -> pd.DataFrame:
        """Parse table data into DataFrame"""
        data = []
        rows = table.find_all('tr')
        
        # Find the actual data rows (skip header rows)
        # Skip first 2 rows: title row and header row
        for i, row in enumerate(rows[2:], start=2):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 3:  # Minimum expected columns
                row_data = [cell.get_text(strip=True) for cell in cells]
                # Only add rows with actual ship data
                if any(cell.strip() and len(cell.strip()) > 2 for cell in row_data):
                    data.append(row_data)
        
        logger.info(f"Found {len(data)} data rows in table with headers: {headers}")
        
        if data:
            # Use the first row length to determine column count
            max_cols = max(len(row) for row in data) if data else 0
            
            # Create generic headers if needed
            if len(headers) < max_cols:
                for i in range(len(headers), max_cols):
                    headers.append(f'coluna_{i+1}')
            
            df = pd.DataFrame(data, columns=headers[:max_cols])
            
            # Add cargo type information from table headers
            if headers and len(headers) > 0:
                cargo_type = headers[0] if headers[0] else 'DESCONHECIDO'
                df['tipo_carga'] = cargo_type
            
            logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
            return self._map_columns(df)
        else:
            logger.warning("No data rows found in table")
            return pd.DataFrame()
    
    def _extract_alternative_data_formats(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Extract data from alternative formats (JSON, CSV, etc.)"""
        # Look for JSON data in script tags
        scripts = soup.find_all('script', type='application/json')
        for script in scripts:
            try:
                import json
                data = json.loads(script.string)
                if self._is_ship_data_json(data):
                    return self._parse_json_data(data)
            except:
                continue
        
        # Look for CSV download links
        csv_links = soup.find_all('a', href=lambda x: x and '.csv' in x.lower())
        for link in csv_links:
            try:
                csv_url = link['href']
                if not csv_url.startswith('http'):
                    csv_url = f"https://www.portodesantos.com.br{csv_url}"
                
                response = self.make_request(csv_url)
                df = pd.read_csv(pd.StringIO(response.text))
                if self._is_ship_data_dataframe(df):
                    return self._map_columns(df)
            except:
                continue
        
        return pd.DataFrame()
    
    def _is_ship_data_json(self, data: Any) -> bool:
        """Check if JSON data contains ship information"""
        if isinstance(data, list) and data:
            first_item = data[0]
            if isinstance(first_item, dict):
                keys = [k.lower() for k in first_item.keys()]
                ship_keywords = ['navio', 'ship', 'vessel', 'produto', 'cargo']
                return any(keyword in ' '.join(keys) for keyword in ship_keywords)
        return False
    
    def _parse_json_data(self, data: List[Dict]) -> pd.DataFrame:
        """Parse JSON data into DataFrame"""
        df = pd.DataFrame(data)
        return self._map_columns(df)
    
    def _is_ship_data_dataframe(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame contains ship data"""
        columns = [col.lower() for col in df.columns]
        ship_keywords = ['navio', 'ship', 'vessel', 'produto', 'cargo']
        return any(keyword in ' '.join(columns) for keyword in ship_keywords)
    
    def _try_alternative_data_sources(self) -> pd.DataFrame:
        """Try alternative data sources for Santos port"""
        # This could include API endpoints, different pages, or data exports
        # For now, return empty DataFrame as placeholder
        logger.warning("No data found from primary Santos source, trying alternatives...")
        
        # Example: Try to find API endpoints or data export links
        alternative_urls = [
            "https://www.portodesantos.com.br/api/navios-esperados",
            "https://www.portodesantos.com.br/export/navios-csv",
            "https://www.portodesantos.com.br/dados-operacionais/navios"
        ]
        
        for url in alternative_urls:
            try:
                response = self.make_request(url)
                # Try to parse as JSON first
                try:
                    import json
                    data = json.loads(response.text)
                    if self._is_ship_data_json(data):
                        return self._parse_json_data(data)
                except:
                    pass
                
                # Try to parse as CSV
                try:
                    df = pd.read_csv(pd.StringIO(response.text))
                    if self._is_ship_data_dataframe(df):
                        return self._map_columns(df)
                except:
                    pass
                    
            except:
                continue
        
        return pd.DataFrame()
    
    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map column names to standard format based on Santos table structure"""
        # Based on the real Santos table structure from the image
        column_mapping = {
            # Exact column names from the website
            'Navio Ship': 'navio',
            'Bandeira Flag': 'bandeira',
            'Com/Len': 'comprimento',
            'Cal/Draft': 'calado',
            'Nav': 'navegacao',
            'Cheg/Arrival d/m/y': 'data_chegada',
            'Carimbo Notice': 'carimbo',
            'Agência Office': 'agencia',
            'Operaç Operat': 'operacao',
            'Mercadoria Goods': 'produto',
            'Peso Weight': 'volume',
            'Viagem Voyage': 'viagem',
            'DUV': 'duv',
            'P': 'prioridade',
            'Terminal': 'terminal',
            # Cargo type columns (these contain the actual ship names)
            'LIQUIDO A GRANEL': 'navio',
            'TRIGO': 'navio',
            'ROLL-IN-ROLL-OFF': 'navio',
            'LASH': 'navio',
            'CABOTAGEM': 'navio',
            'CONTEINERES': 'navio',
            'GRANEIS DE ORIGEM VEGETAL': 'navio',
            'GRANEIS SOLIDOS - IMPORTACAO': 'navio',
            'GRANEIS SOLIDOS - EXPORTACAO': 'navio',
            'PRIORIDADE C3': 'navio',
            'PRIORIDADE C5': 'navio',
            'SEM PRIORIDADE': 'navio',
            # Generic column mappings (for when we have coluna_X)
            'coluna_1': 'navio',      # NavioShip
            'coluna_2': 'bandeira',   # BandeiraFlag
            'coluna_3': 'comprimento_calado',  # Com/LenCal/Draft
            'coluna_4': 'navegacao',  # Nav
            'coluna_5': 'data_chegada',  # Cheg/Arrivald/m/y
            'coluna_6': 'carimbo',    # CarimboNotice
            'coluna_7': 'agencia',    # AgênciaOffice
            'coluna_8': 'sentido',    # OperaçOperat (EMB/DESC)
            'coluna_9': 'produto',    # MercadoriaGoods
            'coluna_10': 'volume',    # PesoWeight
            'coluna_11': 'viagem',    # ViagemVoyage
            'coluna_12': 'duv',       # DUV
            'coluna_13': 'prioridade', # P
            'coluna_14': 'terminal',  # Terminal
            'coluna_15': 'imo',       # IMO
            'coluna_16': 'observacoes',
            # Alternative mappings
            'Navio': 'navio',
            'Ship': 'navio',
            'Vessel': 'navio',
            'Produto': 'produto',
            'Cargo': 'produto',
            'Sentido': 'sentido',
            'Direction': 'sentido',
            'Volume': 'volume',
            'Tonnage': 'volume',
            'Data Chegada': 'data_chegada',
            'Arrival Date': 'data_chegada',
            'Data Partida': 'data_partida',
            'Departure Date': 'data_partida',
            'Armador': 'armador',
            'Owner': 'armador',
            'Agente': 'agencia',
            'Agent': 'agencia'
        }
        
        # Rename columns that exist
        existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_mapping)
        
        # Standardize direction values
        if 'sentido' in df.columns:
            df['sentido'] = df['sentido'].str.upper()
            df['sentido'] = df['sentido'].replace({
                'EXP': 'EXPORTAÇÃO',
                'IMP': 'IMPORTAÇÃO',
                'EXPORT': 'EXPORTAÇÃO',
                'IMPORT': 'IMPORTAÇÃO',
                'OUTBOUND': 'EXPORTAÇÃO',
                'INBOUND': 'IMPORTAÇÃO',
                'EMB': 'EXPORTAÇÃO',
                'DESC': 'IMPORTAÇÃO'
            })
        
        # Determine direction from operation type if available
        if 'operacao' in df.columns and 'sentido' not in df.columns:
            df['sentido'] = df['operacao'].str.upper()
            df['sentido'] = df['sentido'].replace({
                'EMB': 'EXPORTAÇÃO',
                'DESC': 'IMPORTAÇÃO'
            })
        
        # Convert volume to numeric
        volume_columns = ['volume', 'peso']
        for col in volume_columns:
            if col in df.columns:
                # Clean numeric values (remove commas, spaces, etc.)
                df[col] = df[col].astype(str).str.replace(',', '.').str.replace(' ', '')
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert dates
        date_columns = ['data_chegada', 'data_partida']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        
        # Convert numeric columns
        numeric_columns = ['comprimento', 'calado']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

