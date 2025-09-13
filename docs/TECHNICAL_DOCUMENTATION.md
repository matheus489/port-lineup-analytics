# Documentação Técnica - Ship Lineup Data Pipeline

## Visão Geral da Arquitetura

### Arquitetura Medallion

O sistema implementa a arquitetura medallion com três camadas distintas:

#### Bronze Layer (Dados Brutos)
- **Propósito**: Armazenamento de dados brutos coletados diretamente das fontes
- **Características**:
  - Dados não processados
  - Metadados de coleta (timestamp, fonte, etc.)
  - Preservação da estrutura original
  - Formato: Parquet + SQLite/PostgreSQL

#### Silver Layer (Dados Processados)
- **Propósito**: Dados limpos, padronizados e enriquecidos
- **Transformações**:
  - Limpeza de dados (remover duplicatas, valores nulos)
  - Padronização (nomes de portos, direções, produtos)
  - Enriquecimento (classificações, categorias)
  - Validação de qualidade

#### Gold Layer (Dados de Negócio)
- **Propósito**: Dados prontos para análise e relatórios
- **Características**:
  - Métricas de negócio
  - Agregações temporais
  - Indicadores de performance
  - Dados otimizados para consulta

## Componentes do Sistema

### 1. Coletores de Dados (Data Collectors)

#### BaseCollector
```python
class BaseCollector(ABC):
    """Classe base abstrata para todos os coletores"""
    
    def __init__(self, port_name: str, port_code: str)
    def collect_data(self, start_date: str, end_date: str) -> pd.DataFrame
    def make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response
    def standardize_data(self, df: pd.DataFrame) -> pd.DataFrame
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame
```

**Características**:
- Classe abstrata para padronização
- Tratamento de erros e retry automático
- Validação básica de dados
- Metadados de coleta

#### ParanaguaCollector
- **Fonte**: APPA (Administração dos Portos de Paranaguá e Antonina)
- **URL**: https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo
- **Método**: Web scraping com BeautifulSoup
- **Dados**: Lineup retroativo de navios

#### SantosCollector
- **Fonte**: Codesp (Companhia Docas do Estado de São Paulo)
- **URL**: https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/
- **Método**: Web scraping + APIs alternativas
- **Dados**: Navios esperados com carga

### 2. Pipeline ETL (MedallionPipeline)

#### Processamento Bronze → Silver
```python
def process_silver_layer(self, bronze_file: str) -> str:
    # 1. Carregar dados bronze
    # 2. Limpeza de dados
    # 3. Padronização
    # 4. Enriquecimento
    # 5. Salvar silver
```

**Transformações Silver**:
- Remoção de duplicatas
- Tratamento de valores nulos
- Padronização de texto (maiúsculas, trim)
- Conversão de tipos de dados
- Classificação de produtos e navios
- Extração de campos temporais

#### Processamento Silver → Gold
```python
def process_gold_layer(self, silver_file: str) -> str:
    # 1. Carregar dados silver
    # 2. Aplicar regras de negócio
    # 3. Criar agregações
    # 4. Calcular métricas
    # 5. Salvar gold
```

**Transformações Gold**:
- Aplicação de regras de negócio
- Cálculo de métricas (médias móveis, crescimento)
- Criação de rankings
- Flags de qualidade
- Agregações por período/produto/porto

### 3. Gerenciador de Banco de Dados (DatabaseManager)

#### Estrutura das Tabelas

**Bronze Table**:
```sql
CREATE TABLE bronze_ship_lineup (
    id INTEGER PRIMARY KEY,
    porto VARCHAR(50),
    navio VARCHAR(100),
    produto VARCHAR(100),
    sentido VARCHAR(20),
    volume FLOAT,
    data_chegada DATETIME,
    data_partida DATETIME,
    armador VARCHAR(100),
    agente VARCHAR(100),
    collection_date DATETIME,
    source VARCHAR(50),
    processing_timestamp DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

**Silver Table** (campos adicionais):
```sql
-- Campos da bronze +
ano INTEGER,
mes INTEGER,
dia_semana VARCHAR(20),
trimestre INTEGER,
tipo_navio VARCHAR(50),
categoria_produto VARCHAR(50),
categoria_volume VARCHAR(20),
status_operacao VARCHAR(20),
flag_qualidade VARCHAR(20)
```

**Gold Table** (campos adicionais):
```sql
-- Campos da silver +
volume_ma_7d FLOAT,
volume_ma_30d FLOAT,
crescimento_volume FLOAT,
ranking_volume FLOAT
```

#### Operações Principais
- `insert_bronze_data()`: Inserção de dados brutos
- `insert_silver_data()`: Inserção de dados processados
- `insert_gold_data()`: Inserção de dados de negócio
- `get_latest_collection_date()`: Última coleta por fonte
- `get_data_by_date_range()`: Consulta por período
- `get_aggregated_data()`: Dados agregados
- `get_data_quality_report()`: Relatório de qualidade

### 4. Agendador (DailyScheduler)

#### Funcionalidades
- **Coleta Diária**: Execução diária às 06:00
- **Atualização Incremental**: Execução às 10:00, 14:00, 18:00
- **Limpeza Semanal**: Execução domingos às 02:00
- **Coleta Manual**: Para períodos específicos

#### Fluxo de Execução
```python
def run_daily_collection(self):
    # 1. Calcular período (últimos 7 dias)
    # 2. Coletar dados de ambas as fontes
    # 3. Processar através das camadas medallion
    # 4. Inserir no banco de dados
    # 5. Gerar relatório diário
```

### 5. Validação de Dados (DataValidator)

#### Regras de Validação
- **Campos Obrigatórios**: porto, navio, produto, sentido, data_chegada
- **Valores Válidos**: Portos (PARANAGUÁ, SANTOS), Sentidos (EXPORTAÇÃO, IMPORTAÇÃO)
- **Faixas de Volume**: 0 a 1.000.000 toneladas
- **Datas**: Validação de formato e consistência temporal

#### Métricas de Qualidade
- **Data Quality Score**: Percentual de registros válidos
- **Flags de Qualidade**: OK, VOLUME_BAIXO, VOLUME_ALTO, DATA_FUTURA
- **Detecção de Anomalias**: Valores fora do padrão estatístico

### 6. Dicionário de Dados (DataDictionary)

#### Classificações Automáticas

**Produtos**:
- GRÃOS: SOJA, MILHO, TRIGO, ARROZ
- AÇÚCAR: AÇÚCAR, SUGAR
- FERTILIZANTES: FERTILIZANTE, UREIA, FOSFATO
- CONTAINER: CONTAINER, CONTÊINER
- MINÉRIOS: MINÉRIO, IRON ORE
- OUTROS: Categoria catch-all

**Tipos de Navio**:
- CARGA_GERAL: BULK, GRAIN, CARGO
- CONTAINER: CONTAINER, BOX
- TANQUE: TANKER, OIL, PETROLEUM
- RO-RO: RO-RO, FERRY
- OUTROS: Categoria catch-all

## Fluxo de Dados

### 1. Coleta
```
Fonte Web → Coletor → Dados Brutos → Validação Básica
```

### 2. Processamento Bronze
```
Dados Brutos → Metadados → Armazenamento Bronze → Arquivo Parquet
```

### 3. Processamento Silver
```
Bronze → Limpeza → Padronização → Enriquecimento → Silver
```

### 4. Processamento Gold
```
Silver → Regras Negócio → Agregações → Métricas → Gold
```

### 5. Armazenamento
```
Gold → Banco de Dados → Relatórios → APIs (futuro)
```

## Tratamento de Erros

### 1. Erros de Coleta
- **Timeout**: Retry automático com backoff exponencial
- **Rate Limiting**: Delay entre requisições
- **Dados Inacessíveis**: Log de erro e continuação com outras fontes

### 2. Erros de Processamento
- **Dados Inválidos**: Remoção e log de erro
- **Falha de Transformação**: Rollback e reprocessamento
- **Falha de Banco**: Retry e fallback para arquivo

### 3. Monitoramento
- **Logs Estruturados**: Níveis INFO, WARNING, ERROR
- **Métricas de Qualidade**: Score em tempo real
- **Alertas**: Falhas críticas (futuro)

## Configuração e Deploy

### 1. Configuração Local
```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar ambiente
cp .env.example .env
# Editar .env com configurações

# Executar setup
python setup.py

# Testar sistema
python main.py test
```

### 2. Deploy com Docker
```bash
# Build da imagem
docker build -t ship-lineup-pipeline .

# Executar com docker-compose
docker-compose up -d
```

### 3. Deploy em Produção
- **Servidor**: Linux com Python 3.8+
- **Banco**: PostgreSQL recomendado
- **Monitoramento**: Logs + métricas
- **Backup**: Automatizado diário

## Performance e Escalabilidade

### 1. Otimizações Atuais
- **Parquet**: Formato otimizado para analytics
- **Índices**: Índices em campos de consulta frequente
- **Batch Processing**: Processamento em lotes
- **Connection Pooling**: Pool de conexões de banco

### 2. Limitações Conhecidas
- **Web Scraping**: Dependente da estrutura dos sites
- **Processamento Sequencial**: Não paralelizado
- **Memória**: Carregamento completo de DataFrames

### 3. Melhorias Futuras
- **Processamento Paralelo**: Múltiplas fontes simultâneas
- **Streaming**: Processamento em tempo real
- **Cache**: Cache de dados frequentes
- **APIs**: Substituição de web scraping por APIs

## Segurança

### 1. Medidas Implementadas
- **Rate Limiting**: Controle de requisições
- **User-Agent**: Identificação adequada
- **Timeout**: Prevenção de travamentos
- **Validação**: Sanitização de dados

### 2. Considerações
- **Dados Públicos**: Apenas dados públicos são coletados
- **Respeito às Fontes**: Rate limiting e identificação
- **Logs**: Não exposição de dados sensíveis

## Monitoramento e Observabilidade

### 1. Logs
- **Estruturados**: Formato JSON para parsing
- **Rotação**: Diária com retenção de 30 dias
- **Níveis**: DEBUG, INFO, WARNING, ERROR

### 2. Métricas
- **Qualidade**: Score de qualidade de dados
- **Volume**: Quantidade de registros processados
- **Performance**: Tempo de processamento
- **Erros**: Taxa de erro por fonte

### 3. Relatórios
- **Diários**: Resumo de processamento
- **Qualidade**: Relatório de qualidade de dados
- **Agregações**: Métricas de negócio

## Extensibilidade

### 1. Novos Portos
```python
class NovoPortoCollector(BaseCollector):
    def collect_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        # Implementar coleta específica
        pass
```

### 2. Novas Fontes
- Implementar interface BaseCollector
- Adicionar configuração em Config.DATA_SOURCES
- Configurar validações específicas

### 3. Novas Transformações
- Estender MedallionPipeline
- Adicionar regras de negócio
- Implementar novas métricas

## Troubleshooting

### 1. Problemas Comuns

**Erro de Coleta**:
- Verificar conectividade
- Verificar se o site está acessível
- Ajustar timeout se necessário

**Dados Inválidos**:
- Verificar logs de validação
- Ajustar regras de validação
- Verificar mudanças na fonte

**Falha de Banco**:
- Verificar conexão
- Verificar espaço em disco
- Verificar permissões

### 2. Comandos de Diagnóstico
```bash
# Teste básico
python main.py test

# Verificar logs
tail -f logs/ship_lineup.log

# Verificar banco
python -c "from src.database.database_manager import DatabaseManager; db = DatabaseManager(); print(db.get_database_stats())"
```

## Roadmap Técnico

### Fase 1 (Atual)
- ✅ Arquitetura medallion
- ✅ Coletores básicos
- ✅ Pipeline ETL
- ✅ Validação de dados
- ✅ Agendamento

### Fase 2 (Próxima)
- 🔄 API REST
- 🔄 Dashboard web
- 🔄 Alertas automáticos
- 🔄 Processamento paralelo

### Fase 3 (Futuro)
- 📋 Machine Learning
- 📋 Integração com BI
- 📋 Cache distribuído
- 📋 Microserviços

