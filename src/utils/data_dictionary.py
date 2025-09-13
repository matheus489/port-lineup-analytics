"""
Data dictionary and metadata management for ship lineup data
"""
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from loguru import logger


class DataDictionary:
    """Manages data dictionary and metadata for ship lineup data"""
    
    def __init__(self):
        self.data_dictionary = self._initialize_data_dictionary()
        self.product_categories = self._initialize_product_categories()
        self.ship_types = self._initialize_ship_types()
        self.port_mapping = self._initialize_port_mapping()
    
    def _initialize_data_dictionary(self) -> Dict[str, Dict[str, Any]]:
        """Initialize the main data dictionary"""
        return {
            'porto': {
                'description': 'Nome do porto onde o navio está operando',
                'data_type': 'string',
                'valid_values': ['PARANAGUÁ', 'SANTOS'],
                'required': True,
                'example': 'PARANAGUÁ'
            },
            'porto_codigo': {
                'description': 'Código identificador do porto',
                'data_type': 'string',
                'valid_values': ['PAR', 'STS'],
                'required': True,
                'example': 'PAR'
            },
            'navio': {
                'description': 'Nome do navio',
                'data_type': 'string',
                'valid_values': 'Free text',
                'required': True,
                'example': 'MSC LORETO'
            },
            'produto': {
                'description': 'Tipo de produto/carga transportada',
                'data_type': 'string',
                'valid_values': 'Free text',
                'required': True,
                'example': 'SOJA'
            },
            'sentido': {
                'description': 'Direção do transporte (exportação ou importação)',
                'data_type': 'string',
                'valid_values': ['EXPORTAÇÃO', 'IMPORTAÇÃO'],
                'required': True,
                'example': 'EXPORTAÇÃO'
            },
            'volume': {
                'description': 'Volume de carga em toneladas',
                'data_type': 'float',
                'valid_values': '0 to 1,000,000',
                'required': False,
                'example': 65000.5
            },
            'data_chegada': {
                'description': 'Data de chegada do navio no porto',
                'data_type': 'datetime',
                'valid_values': 'Valid date',
                'required': True,
                'example': '2024-01-15 08:30:00'
            },
            'data_partida': {
                'description': 'Data de partida do navio do porto',
                'data_type': 'datetime',
                'valid_values': 'Valid date',
                'required': False,
                'example': '2024-01-17 14:20:00'
            },
            'armador': {
                'description': 'Nome da empresa armadora do navio',
                'data_type': 'string',
                'valid_values': 'Free text',
                'required': False,
                'example': 'MSC'
            },
            'agente': {
                'description': 'Nome do agente marítimo',
                'data_type': 'string',
                'valid_values': 'Free text',
                'required': False,
                'example': 'WILSON SONS'
            },
            'ano': {
                'description': 'Ano extraído da data de chegada',
                'data_type': 'integer',
                'valid_values': '2020-2030',
                'required': False,
                'example': 2024
            },
            'mes': {
                'description': 'Mês extraído da data de chegada',
                'data_type': 'integer',
                'valid_values': '1-12',
                'required': False,
                'example': 1
            },
            'dia_semana': {
                'description': 'Dia da semana da chegada',
                'data_type': 'string',
                'valid_values': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                'required': False,
                'example': 'Monday'
            },
            'trimestre': {
                'description': 'Trimestre do ano da chegada',
                'data_type': 'integer',
                'valid_values': '1-4',
                'required': False,
                'example': 1
            },
            'tipo_navio': {
                'description': 'Classificação do tipo de navio baseada no nome',
                'data_type': 'string',
                'valid_values': ['CARGA_GERAL', 'CONTAINER', 'TANQUE', 'RO-RO', 'OUTROS'],
                'required': False,
                'example': 'CONTAINER'
            },
            'categoria_produto': {
                'description': 'Categoria do produto baseada no tipo',
                'data_type': 'string',
                'valid_values': ['GRÃOS', 'AÇÚCAR', 'FERTILIZANTES', 'CONTAINER', 'MINÉRIOS', 'OUTROS'],
                'required': False,
                'example': 'GRÃOS'
            },
            'categoria_volume': {
                'description': 'Categoria do volume transportado',
                'data_type': 'string',
                'valid_values': ['Pequeno', 'Médio', 'Grande', 'Muito Grande'],
                'required': False,
                'example': 'Grande'
            },
            'status_operacao': {
                'description': 'Status da operação do navio',
                'data_type': 'string',
                'valid_values': ['ATIVO', 'CANCELADO', 'ADIADO'],
                'required': False,
                'example': 'ATIVO'
            },
            'flag_qualidade': {
                'description': 'Indicador de qualidade dos dados',
                'data_type': 'string',
                'valid_values': ['OK', 'VOLUME_BAIXO', 'VOLUME_ALTO', 'DATA_FUTURA', 'DADOS_INCONSISTENTES'],
                'required': False,
                'example': 'OK'
            },
            'collection_date': {
                'description': 'Data de coleta dos dados',
                'data_type': 'datetime',
                'valid_values': 'Valid date',
                'required': True,
                'example': '2024-01-15 10:00:00'
            },
            'source': {
                'description': 'Fonte dos dados (paranagua ou santos)',
                'data_type': 'string',
                'valid_values': ['paranagua', 'santos'],
                'required': True,
                'example': 'paranagua'
            },
            'processing_timestamp': {
                'description': 'Timestamp do processamento dos dados',
                'data_type': 'datetime',
                'valid_values': 'Valid datetime',
                'required': True,
                'example': '2024-01-15 10:05:30'
            }
        }
    
    def _initialize_product_categories(self) -> Dict[str, List[str]]:
        """Initialize product category mappings"""
        return {
            'GRÃOS': ['SOJA', 'MILHO', 'TRIGO', 'ARROZ', 'FEIJÃO', 'GIRASSOL', 'SORGO'],
            'AÇÚCAR': ['AÇÚCAR', 'SUGAR', 'AÇÚCAR CRISTAL', 'AÇÚCAR REFINADO'],
            'FERTILIZANTES': ['FERTILIZANTE', 'FERTILIZER', 'UREIA', 'FOSFATO', 'POTÁSSIO'],
            'CONTAINER': ['CONTAINER', 'CONTÊINER', 'CONTAINERIZED CARGO'],
            'MINÉRIOS': ['MINÉRIO', 'ORE', 'IRON ORE', 'MINÉRIO DE FERRO', 'BAUXITA'],
            'PETRÓLEO': ['PETRÓLEO', 'OIL', 'CRUDE OIL', 'PETROLEUM'],
            'QUÍMICOS': ['QUÍMICO', 'CHEMICAL', 'PRODUTOS QUÍMICOS'],
            'OUTROS': []  # Catch-all category
        }
    
    def _initialize_ship_types(self) -> Dict[str, List[str]]:
        """Initialize ship type classifications"""
        return {
            'CARGA_GERAL': ['BULK', 'GRAIN', 'CARGO', 'GENERAL CARGO'],
            'CONTAINER': ['CONTAINER', 'BOX', 'CONTÊINER'],
            'TANQUE': ['TANKER', 'OIL', 'PETROLEUM', 'CHEMICAL TANKER'],
            'RO-RO': ['RO-RO', 'FERRY', 'ROLO'],
            'GRANELEIRO': ['BULK CARRIER', 'GRANELEIRO', 'BULKER'],
            'OUTROS': []  # Catch-all category
        }
    
    def _initialize_port_mapping(self) -> Dict[str, Dict[str, str]]:
        """Initialize port mapping and metadata"""
        return {
            'PARANAGUÁ': {
                'code': 'PAR',
                'full_name': 'Porto de Paranaguá',
                'state': 'Paraná',
                'country': 'Brasil',
                'coordinates': {'lat': -25.5200, 'lon': -48.5075},
                'website': 'https://www.appaweb.appa.pr.gov.br/',
                'data_source': 'APPA - Administração dos Portos de Paranaguá e Antonina'
            },
            'SANTOS': {
                'code': 'STS',
                'full_name': 'Porto de Santos',
                'state': 'São Paulo',
                'country': 'Brasil',
                'coordinates': {'lat': -23.9608, 'lon': -46.3331},
                'website': 'https://www.portodesantos.com.br/',
                'data_source': 'Codesp - Companhia Docas do Estado de São Paulo'
            }
        }
    
    def get_field_definition(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Get definition for a specific field"""
        return self.data_dictionary.get(field_name)
    
    def get_all_fields(self) -> List[str]:
        """Get list of all field names"""
        return list(self.data_dictionary.keys())
    
    def get_required_fields(self) -> List[str]:
        """Get list of required fields"""
        return [
            field for field, definition in self.data_dictionary.items()
            if definition.get('required', False)
        ]
    
    def get_field_data_type(self, field_name: str) -> Optional[str]:
        """Get data type for a specific field"""
        definition = self.get_field_definition(field_name)
        return definition.get('data_type') if definition else None
    
    def get_valid_values(self, field_name: str) -> Optional[List[str]]:
        """Get valid values for a specific field"""
        definition = self.get_field_definition(field_name)
        return definition.get('valid_values') if definition else None
    
    def classify_product(self, product: str) -> str:
        """Classify a product into a category"""
        product_upper = str(product).upper()
        
        for category, keywords in self.product_categories.items():
            if any(keyword in product_upper for keyword in keywords):
                return category
        
        return 'OUTROS'
    
    def classify_ship_type(self, ship_name: str) -> str:
        """Classify a ship type based on name"""
        ship_upper = str(ship_name).upper()
        
        for ship_type, keywords in self.ship_types.items():
            if any(keyword in ship_upper for keyword in keywords):
                return ship_type
        
        return 'OUTROS'
    
    def get_port_info(self, port_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific port"""
        return self.port_mapping.get(port_name.upper())
    
    def generate_data_dictionary_report(self) -> str:
        """Generate a formatted data dictionary report"""
        report = f"""
Ship Lineup Data Dictionary
==========================
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Total Fields: {len(self.data_dictionary)}
Required Fields: {len(self.get_required_fields())}

Field Definitions:
"""
        
        for field_name, definition in self.data_dictionary.items():
            report += f"""
{field_name.upper()}:
  Description: {definition['description']}
  Data Type: {definition['data_type']}
  Required: {definition['required']}
  Valid Values: {definition['valid_values']}
  Example: {definition['example']}
"""
        
        return report
    
    def validate_field_value(self, field_name: str, value: Any) -> Tuple[bool, str]:
        """
        Validate a field value against its definition
        
        Args:
            field_name: Name of the field
            value: Value to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        definition = self.get_field_definition(field_name)
        if not definition:
            return False, f"Unknown field: {field_name}"
        
        # Check required fields
        if definition.get('required', False) and (value is None or value == ''):
            return False, f"Required field '{field_name}' is empty"
        
        # Check data type
        expected_type = definition.get('data_type')
        if expected_type == 'string' and not isinstance(value, str):
            return False, f"Field '{field_name}' should be string, got {type(value).__name__}"
        elif expected_type == 'integer' and not isinstance(value, int):
            return False, f"Field '{field_name}' should be integer, got {type(value).__name__}"
        elif expected_type == 'float' and not isinstance(value, (int, float)):
            return False, f"Field '{field_name}' should be float, got {type(value).__name__}"
        elif expected_type == 'datetime' and not isinstance(value, (pd.Timestamp, datetime)):
            return False, f"Field '{field_name}' should be datetime, got {type(value).__name__}"
        
        # Check valid values
        valid_values = definition.get('valid_values')
        if valid_values and valid_values != 'Free text' and value not in valid_values:
            return False, f"Field '{field_name}' has invalid value '{value}'. Valid values: {valid_values}"
        
        return True, ""
    
    def export_data_dictionary(self, filepath: str):
        """Export data dictionary to CSV file"""
        data = []
        for field_name, definition in self.data_dictionary.items():
            data.append({
                'field_name': field_name,
                'description': definition['description'],
                'data_type': definition['data_type'],
                'required': definition['required'],
                'valid_values': str(definition['valid_values']),
                'example': definition['example']
            })
        
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False)
        logger.info(f"Data dictionary exported to {filepath}")
    
    def get_metadata_summary(self) -> Dict[str, Any]:
        """Get summary metadata about the data dictionary"""
        return {
            'total_fields': len(self.data_dictionary),
            'required_fields': len(self.get_required_fields()),
            'optional_fields': len(self.data_dictionary) - len(self.get_required_fields()),
            'data_types': list(set(defn['data_type'] for defn in self.data_dictionary.values())),
            'product_categories': len(self.product_categories),
            'ship_types': len(self.ship_types),
            'ports': len(self.port_mapping),
            'last_updated': datetime.now().isoformat()
        }
