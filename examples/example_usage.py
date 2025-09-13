"""
Exemplo de uso do Ship Lineup Data Pipeline
"""
import sys
import os
from datetime import datetime, timedelta

# Adicionar o diretório raiz ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_collectors.paranagua_collector import ParanaguaCollector
from src.data_collectors.santos_collector import SantosCollector
from src.etl.medallion_pipeline import MedallionPipeline
from src.database.database_manager import DatabaseManager
from src.utils.data_validation import DataValidator
from src.utils.data_dictionary import DataDictionary
from config import Config


def example_data_collection():
    """Exemplo de coleta de dados"""
    print("=== Exemplo de Coleta de Dados ===")
    
    # Configurar datas
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"Coletando dados de {start_date} a {end_date}")
    
    # Coletor de Paranaguá
    print("\n1. Coletando dados de Paranaguá...")
    paranagua_collector = ParanaguaCollector()
    paranagua_data = paranagua_collector.collect_data(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    print(f"Dados coletados de Paranaguá: {len(paranagua_data)} registros")
    
    # Coletor de Santos
    print("\n2. Coletando dados de Santos...")
    santos_collector = SantosCollector()
    santos_data = santos_collector.collect_data(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    print(f"Dados coletados de Santos: {len(santos_data)} registros")
    
    return paranagua_data, santos_data


def example_data_processing(paranagua_data, santos_data):
    """Exemplo de processamento de dados"""
    print("\n=== Exemplo de Processamento de Dados ===")
    
    # Pipeline medallion
    pipeline = MedallionPipeline()
    
    # Processar dados de Paranaguá
    if not paranagua_data.empty:
        print("\n1. Processando dados de Paranaguá...")
        bronze_file = pipeline.process_bronze_layer(paranagua_data, 'paranagua', datetime.now().strftime('%Y-%m-%d'))
        print(f"Arquivo bronze criado: {bronze_file}")
        
        silver_file = pipeline.process_silver_layer(bronze_file)
        print(f"Arquivo silver criado: {silver_file}")
        
        gold_file = pipeline.process_gold_layer(silver_file)
        print(f"Arquivo gold criado: {gold_file}")
    
    # Processar dados de Santos
    if not santos_data.empty:
        print("\n2. Processando dados de Santos...")
        bronze_file = pipeline.process_bronze_layer(santos_data, 'santos', datetime.now().strftime('%Y-%m-%d'))
        print(f"Arquivo bronze criado: {bronze_file}")
        
        silver_file = pipeline.process_silver_layer(bronze_file)
        print(f"Arquivo silver criado: {silver_file}")
        
        gold_file = pipeline.process_gold_layer(silver_file)
        print(f"Arquivo gold criado: {gold_file}")


def example_data_validation():
    """Exemplo de validação de dados"""
    print("\n=== Exemplo de Validação de Dados ===")
    
    # Criar dados de exemplo
    import pandas as pd
    
    sample_data = pd.DataFrame({
        'porto': ['PARANAGUÁ', 'SANTOS', 'INVALID_PORT'],
        'navio': ['MSC LORETO', 'EVER GIVEN', ''],
        'produto': ['SOJA', 'CONTAINER', 'MILHO'],
        'sentido': ['EXPORTAÇÃO', 'IMPORTAÇÃO', 'INVALID'],
        'volume': [65000.5, 120000.0, -1000.0],
        'data_chegada': ['2024-01-15', '2024-01-16', 'invalid_date']
    })
    
    print("Dados de exemplo:")
    print(sample_data)
    
    # Validar dados
    validator = DataValidator()
    cleaned_data, validation_report = validator.validate_dataframe(sample_data)
    
    print(f"\nRelatório de Validação:")
    print(f"Total de registros: {validation_report['total_records']}")
    print(f"Registros válidos: {validation_report['valid_records']}")
    print(f"Registros inválidos: {validation_report['invalid_records']}")
    print(f"Score de qualidade: {validation_report['data_quality_score']:.2f}%")
    
    print(f"\nErros encontrados:")
    for error in validation_report['validation_errors']:
        print(f"- {error}")
    
    print(f"\nDados limpos:")
    print(cleaned_data)


def example_data_dictionary():
    """Exemplo de uso do dicionário de dados"""
    print("\n=== Exemplo de Dicionário de Dados ===")
    
    data_dict = DataDictionary()
    
    # Obter informações sobre campos
    print("Campos obrigatórios:")
    required_fields = data_dict.get_required_fields()
    for field in required_fields:
        print(f"- {field}")
    
    # Exemplo de classificação
    print(f"\nClassificação de produtos:")
    products = ['SOJA', 'AÇÚCAR', 'CONTAINER', 'FERTILIZANTE']
    for product in products:
        category = data_dict.classify_product(product)
        print(f"- {product} -> {category}")
    
    # Exemplo de classificação de navios
    print(f"\nClassificação de navios:")
    ships = ['MSC LORETO', 'EVER GIVEN', 'BULK CARRIER', 'OIL TANKER']
    for ship in ships:
        ship_type = data_dict.classify_ship_type(ship)
        print(f"- {ship} -> {ship_type}")
    
    # Informações dos portos
    print(f"\nInformações dos portos:")
    ports = ['PARANAGUÁ', 'SANTOS']
    for port in ports:
        port_info = data_dict.get_port_info(port)
        if port_info:
            print(f"- {port}: {port_info['full_name']} ({port_info['state']})")


def example_database_operations():
    """Exemplo de operações de banco de dados"""
    print("\n=== Exemplo de Operações de Banco de Dados ===")
    
    # Gerenciador de banco
    db_manager = DatabaseManager()
    
    # Estatísticas do banco
    stats = db_manager.get_database_stats()
    print("Estatísticas do banco de dados:")
    for key, value in stats.items():
        print(f"- {key}: {value}")
    
    # Relatório de qualidade
    quality_report = db_manager.get_data_quality_report()
    if not quality_report.empty:
        print(f"\nRelatório de qualidade de dados:")
        print(quality_report)
    
    # Dados agregados (últimos 30 dias)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    aggregated_data = db_manager.get_aggregated_data(start_date, end_date)
    if not aggregated_data.empty:
        print(f"\nDados agregados (últimos 30 dias):")
        print(aggregated_data.head())


def main():
    """Função principal do exemplo"""
    print("Ship Lineup Data Pipeline - Exemplos de Uso")
    print("=" * 50)
    
    try:
        # Criar diretórios necessários
        Config.create_directories()
        
        # Exemplo 1: Coleta de dados
        paranagua_data, santos_data = example_data_collection()
        
        # Exemplo 2: Processamento de dados
        example_data_processing(paranagua_data, santos_data)
        
        # Exemplo 3: Validação de dados
        example_data_validation()
        
        # Exemplo 4: Dicionário de dados
        example_data_dictionary()
        
        # Exemplo 5: Operações de banco de dados
        example_database_operations()
        
        print("\n" + "=" * 50)
        print("Todos os exemplos executados com sucesso!")
        
    except Exception as e:
        print(f"Erro durante execução dos exemplos: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
