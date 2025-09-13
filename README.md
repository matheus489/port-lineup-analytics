# Ship Lineup Data Pipeline

Sistema de coleta e processamento de dados de lineup de navios dos portos de Paranaguá e Santos, implementando a arquitetura medallion (bronze, silver, gold) para garantir qualidade e rastreabilidade dos dados.

## 📋 Visão Geral

Este projeto implementa um pipeline de dados robusto para coleta diária de informações sobre navios esperados nos portos de Paranaguá e Santos, incluindo volumes transportados por produto, sentido (exportação/importação) e preservação do histórico de dados.

## 🏗️ Arquitetura

### Arquitetura Medallion

O sistema implementa a arquitetura medallion com três camadas:

- **Bronze Layer**: Dados brutos coletados diretamente das fontes
- **Silver Layer**: Dados limpos, padronizados e enriquecidos
- **Gold Layer**: Dados prontos para negócio com métricas e agregações

### Estrutura do Projeto

```
ship-lineup-pipeline/
├── src/
│   ├── data_collectors/          # Coletores de dados
│   │   ├── base_collector.py     # Classe base para coletores
│   │   ├── paranagua_collector.py # Coletor do porto de Paranaguá
│   │   └── santos_collector.py   # Coletor do porto de Santos
│   ├── etl/                      # Pipeline de ETL
│   │   └── medallion_pipeline.py # Implementação da arquitetura medallion
│   ├── database/                 # Gerenciamento de banco de dados
│   │   └── database_manager.py   # Operações de banco de dados
│   ├── scheduler/                # Agendamento de tarefas
│   │   └── daily_scheduler.py    # Agendador diário
│   └── utils/                    # Utilitários
│       ├── data_validation.py    # Validação de dados
│       └── data_dictionary.py    # Dicionário de dados
├── data/                         # Armazenamento de dados
│   ├── bronze/                   # Camada bronze
│   ├── silver/                   # Camada silver
│   └── gold/                     # Camada gold
├── logs/                         # Logs do sistema
├── config.py                     # Configurações
├── main.py                       # Ponto de entrada principal
└── requirements.txt              # Dependências
```

## 🚀 Instalação e Configuração

### Pré-requisitos

- Python 3.8+
- PostgreSQL (opcional, SQLite por padrão)

### Instalação

1. Clone o repositório:
```bash
git clone <repository-url>
cd ship-lineup-pipeline
```

2. Crie um ambiente virtual:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

4. Configure as variáveis de ambiente (opcional):
```bash
cp .env.example .env
# Edite o arquivo .env com suas configurações
```

## 📊 Fontes de Dados

### Porto de Paranaguá
- **URL**: https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo
- **Fonte**: APPA - Administração dos Portos de Paranaguá e Antonina
- **Dados**: Lineup retroativo de navios

### Porto de Santos
- **URL**: https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/
- **Fonte**: Codesp - Companhia Docas do Estado de São Paulo
- **Dados**: Navios esperados com carga

## 🔧 Uso

### Comandos Disponíveis

#### Coleta Diária
```bash
python main.py daily
```

#### Atualização Incremental
```bash
python main.py incremental
```

#### Coleta Manual (período específico)
```bash
python main.py manual --start-date 2024-01-01 --end-date 2024-01-31
```

#### Executar Agendador Automático
```bash
python main.py scheduler
```

#### Limpeza de Dados
```bash
python main.py cleanup
```

#### Teste do Sistema
```bash
python main.py test
```

### Agendamento Automático

O sistema inclui agendamento automático com os seguintes horários:

- **Coleta Diária**: 06:00
- **Atualizações Incrementais**: 10:00, 14:00, 18:00
- **Limpeza Semanal**: Domingo 02:00

## 📈 Funcionalidades

### Coleta de Dados
- Coleta automatizada de dados dos portos de Paranaguá e Santos
- Tratamento de erros e retry automático
- Validação de dados em tempo real

### Processamento ETL
- **Bronze Layer**: Armazenamento de dados brutos com metadados
- **Silver Layer**: Limpeza, padronização e enriquecimento
- **Gold Layer**: Métricas de negócio e agregações

### Validação de Dados
- Validação de campos obrigatórios
- Verificação de consistência de dados
- Detecção de duplicatas
- Relatórios de qualidade de dados

### Atualização Incremental
- Preservação do histórico de dados
- Detecção de novos dados
- Processamento apenas de dados novos

### Dicionário de Dados
- Documentação completa dos campos
- Classificação automática de produtos e tipos de navio
- Mapeamento de portos e metadados

## 📊 Estrutura de Dados

### Campos Principais

| Campo | Tipo | Descrição | Obrigatório |
|-------|------|-----------|-------------|
| porto | string | Nome do porto (PARANAGUÁ/SANTOS) | Sim |
| navio | string | Nome do navio | Sim |
| produto | string | Tipo de produto/carga | Sim |
| sentido | string | Direção (EXPORTAÇÃO/IMPORTAÇÃO) | Sim |
| volume | float | Volume em toneladas | Não |
| data_chegada | datetime | Data de chegada | Sim |
| data_partida | datetime | Data de partida | Não |
| armador | string | Empresa armadora | Não |
| agente | string | Agente marítimo | Não |

### Campos Enriquecidos (Silver/Gold)

- **Temporais**: ano, mês, dia_semana, trimestre
- **Classificações**: tipo_navio, categoria_produto, categoria_volume
- **Métricas**: volume_ma_7d, volume_ma_30d, crescimento_volume
- **Qualidade**: flag_qualidade, status_operacao

## 🔍 Monitoramento e Logs

### Logs
- Logs estruturados com diferentes níveis
- Rotação diária de logs
- Retenção de 30 dias

### Relatórios
- Relatórios diários de processamento
- Relatórios de qualidade de dados
- Estatísticas de banco de dados

### Métricas de Qualidade
- Score de qualidade de dados
- Detecção de anomalias
- Flags de qualidade automáticos

## 🛠️ Desenvolvimento

### Estrutura Modular
- Separação clara de responsabilidades
- Classes base para extensibilidade
- Configuração centralizada

### Extensibilidade
- Fácil adição de novos portos
- Suporte a novas fontes de dados
- Configuração flexível de validações

### Testes
```bash
# Executar teste básico
python main.py test
```

## 📋 Considerações e Hipóteses

### Hipóteses Adotadas

1. **Disponibilidade de Dados**: As fontes web mantêm dados históricos acessíveis
2. **Formato de Dados**: Os sites mantêm estrutura consistente de dados
3. **Frequência de Atualização**: Dados são atualizados diariamente
4. **Qualidade dos Dados**: Dados das fontes oficiais são confiáveis

### Limitações Conhecidas

1. **Dependência de Fontes Web**: Sistema depende da disponibilidade dos sites
2. **Mudanças de Estrutura**: Alterações nos sites podem quebrar coletores
3. **Rate Limiting**: Possível limitação de requisições pelos sites
4. **Dados Incompletos**: Nem todos os campos podem estar disponíveis

### Considerações de Segurança

1. **Rate Limiting**: Implementado para evitar sobrecarga das fontes
2. **User-Agent**: Identificação adequada nas requisições
3. **Timeout**: Configuração de timeouts para evitar travamentos
4. **Validação**: Validação rigorosa de dados coletados

## 🚀 Próximos Passos

### Melhorias Planejadas

1. **API REST**: Exposição de dados via API
2. **Dashboard Web**: Interface para visualização de dados
3. **Alertas**: Sistema de alertas para falhas de coleta
4. **Machine Learning**: Detecção de anomalias com ML
5. **Integração**: Integração com sistemas de BI
6. **Cache**: Implementação de cache para melhor performance
7. **Métricas**: Métricas de negócio mais avançadas
8. **Backup**: Sistema de backup automatizado

### Expansão

1. **Novos Portos**: Adição de outros portos brasileiros
2. **Fontes Alternativas**: Integração com APIs oficiais
3. **Dados Históricos**: Coleta de dados históricos mais antigos
4. **Integração Externa**: Conexão com sistemas portuários

## 📞 Suporte

Para dúvidas ou problemas:

1. Verifique os logs em `./logs/`
2. Execute `python main.py test` para diagnóstico
3. Consulte a documentação do código
4. Abra uma issue no repositório

## 📄 Licença

Este projeto foi desenvolvido para fins de avaliação técnica.

---

**Desenvolvido com ❤️ para Veeries**

