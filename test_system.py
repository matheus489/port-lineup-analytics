"""
Teste simples do sistema Ship Lineup Data Pipeline
"""
import sys
import os
from datetime import datetime, timedelta

# Adicionar o diretório raiz ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Teste de imports básicos"""
    print("Testando imports...")
    
    try:
        from config import Config
        print("✓ Config importado com sucesso")
        
        from src.data_collectors.base_collector import BaseCollector
        print("✓ BaseCollector importado com sucesso")
        
        from src.data_collectors.paranagua_collector import ParanaguaCollector
        print("✓ ParanaguaCollector importado com sucesso")
        
        from src.data_collectors.santos_collector import SantosCollector
        print("✓ SantosCollector importado com sucesso")
        
        from src.etl.medallion_pipeline import MedallionPipeline
        print("✓ MedallionPipeline importado com sucesso")
        
        from src.database.database_manager import DatabaseManager
        print("✓ DatabaseManager importado com sucesso")
        
        from src.utils.data_validation import DataValidator
        print("✓ DataValidator importado com sucesso")
        
        from src.utils.data_dictionary import DataDictionary
        print("✓ DataDictionary importado com sucesso")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro ao importar: {e}")
        return False


def test_config():
    """Teste de configuração"""
    print("\nTestando configuração...")
    
    try:
        from config import Config
        
        # Testar criação de diretórios
        Config.create_directories()
        print("✓ Diretórios criados com sucesso")
        
        # Testar configurações
        print(f"✓ Database URL: {Config.DATABASE_URL}")
        print(f"✓ Bronze path: {Config.BRONZE_DATA_PATH}")
        print(f"✓ Silver path: {Config.SILVER_DATA_PATH}")
        print(f"✓ Gold path: {Config.GOLD_DATA_PATH}")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro na configuração: {e}")
        return False


def test_data_dictionary():
    """Teste do dicionário de dados"""
    print("\nTestando dicionário de dados...")
    
    try:
        from src.utils.data_dictionary import DataDictionary
        
        data_dict = DataDictionary()
        
        # Testar campos obrigatórios
        required_fields = data_dict.get_required_fields()
        print(f"✓ Campos obrigatórios: {len(required_fields)}")
        
        # Testar classificação de produtos
        product_category = data_dict.classify_product("SOJA")
        print(f"✓ Classificação de produto (SOJA): {product_category}")
        
        # Testar classificação de navios
        ship_type = data_dict.classify_ship_type("MSC LORETO")
        print(f"✓ Classificação de navio (MSC LORETO): {ship_type}")
        
        # Testar informações de porto
        port_info = data_dict.get_port_info("PARANAGUÁ")
        if port_info:
            print(f"✓ Informações do porto: {port_info['full_name']}")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro no dicionário de dados: {e}")
        return False


def test_data_validation():
    """Teste de validação de dados"""
    print("\nTestando validação de dados...")
    
    try:
        import pandas as pd
        from src.utils.data_validation import DataValidator
        
        # Criar dados de teste
        test_data = pd.DataFrame({
            'porto': ['PARANAGUÁ', 'SANTOS', 'INVALID'],
            'navio': ['MSC LORETO', 'EVER GIVEN', ''],
            'produto': ['SOJA', 'CONTAINER', 'MILHO'],
            'sentido': ['EXPORTAÇÃO', 'IMPORTAÇÃO', 'INVALID'],
            'volume': [65000.5, 120000.0, -1000.0],
            'data_chegada': ['2024-01-15', '2024-01-16', 'invalid']
        })
        
        validator = DataValidator()
        cleaned_data, validation_report = validator.validate_dataframe(test_data)
        
        print(f"✓ Dados originais: {validation_report['total_records']}")
        print(f"✓ Dados válidos: {validation_report['valid_records']}")
        print(f"✓ Score de qualidade: {validation_report['data_quality_score']:.2f}%")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro na validação: {e}")
        return False


def test_database():
    """Teste de banco de dados"""
    print("\nTestando banco de dados...")
    
    try:
        from src.database.database_manager import DatabaseManager
        
        db_manager = DatabaseManager()
        print("✓ Conexão com banco estabelecida")
        
        # Testar estatísticas
        stats = db_manager.get_database_stats()
        print(f"✓ Estatísticas do banco obtidas: {len(stats)} métricas")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro no banco de dados: {e}")
        return False


def test_collectors():
    """Teste dos coletores (sem coleta real)"""
    print("\nTestando coletores...")
    
    try:
        from src.data_collectors.paranagua_collector import ParanaguaCollector
        from src.data_collectors.santos_collector import SantosCollector
        
        # Testar inicialização
        paranagua_collector = ParanaguaCollector()
        print("✓ ParanaguaCollector inicializado")
        
        santos_collector = SantosCollector()
        print("✓ SantosCollector inicializado")
        
        # Testar propriedades
        print(f"✓ Porto Paranaguá: {paranagua_collector.port_name}")
        print(f"✓ Porto Santos: {santos_collector.port_name}")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro nos coletores: {e}")
        return False


def test_pipeline():
    """Teste do pipeline ETL"""
    print("\nTestando pipeline ETL...")
    
    try:
        import pandas as pd
        from src.etl.medallion_pipeline import MedallionPipeline
        
        # Criar dados de teste
        test_data = pd.DataFrame({
            'porto': ['PARANAGUÁ', 'SANTOS'],
            'navio': ['MSC LORETO', 'EVER GIVEN'],
            'produto': ['SOJA', 'CONTAINER'],
            'sentido': ['EXPORTAÇÃO', 'IMPORTAÇÃO'],
            'volume': [65000.5, 120000.0],
            'data_chegada': ['2024-01-15', '2024-01-16']
        })
        
        pipeline = MedallionPipeline()
        print("✓ MedallionPipeline inicializado")
        
        # Testar processamento bronze
        bronze_file = pipeline.process_bronze_layer(test_data, 'test', '2024-01-15')
        print(f"✓ Processamento bronze: {bronze_file}")
        
        # Testar processamento silver
        silver_file = pipeline.process_silver_layer(bronze_file)
        print(f"✓ Processamento silver: {silver_file}")
        
        # Testar processamento gold
        gold_file = pipeline.process_gold_layer(silver_file)
        print(f"✓ Processamento gold: {gold_file}")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro no pipeline: {e}")
        return False


def main():
    """Função principal de teste"""
    print("Ship Lineup Data Pipeline - Teste do Sistema")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_config,
        test_data_dictionary,
        test_data_validation,
        test_database,
        test_collectors,
        test_pipeline
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Erro inesperado em {test.__name__}: {e}")
    
    print("\n" + "=" * 50)
    print(f"Resultado: {passed}/{total} testes passaram")
    
    if passed == total:
        print("🎉 Todos os testes passaram! Sistema funcionando corretamente.")
        return True
    else:
        print("⚠️  Alguns testes falharam. Verifique os erros acima.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

