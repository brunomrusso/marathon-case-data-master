# Case Engenharia de Dados — World Marathon Majors

## I. Objetivo do Case

Desenvolver uma solução completa de Engenharia de Dados para ingerir, processar, armazenar e visualizar dados de resultados de maratonas. A solução demonstra extração, ingestão batch, arquitetura medalhão (Bronze/Silver/Gold), observabilidade, segurança, mascaramento de dados sensíveis, escalabilidade e reprodutibilidade.

## II. Arquitetura

### Tecnologias
- **Cloud:** Microsoft Azure
- **Armazenamento:** Azure Data Lake Storage Gen2 com Delta Lake
- **Processamento:** Azure Databricks + PySpark
- **Orquestração:** Databricks Workflows
- **Observabilidade:** Azure Monitor + Databricks Job Metrics
- **Segurança:** Azure Key Vault, RBAC, criptografia e mascaramento
- **Dashboard:** Power BI ou Streamlit

### Arquitetura Medalhão
- **Bronze:** ingestão dos CSVs brutos das quatro origens, com registro de arquivos processados e carga incremental via `MERGE`.
- **Silver:** limpeza, padronização de schema, integração das fontes e mascaramento/anonimização.
- **Gold:** agregações e métricas para alimentar o dashboard final.

### Fluxo de Dados
```
CSV raw -> Azure Data Lake raw -> Bronze (Delta) -> Silver (Delta) -> Gold (Delta) -> Dashboard
```

## III. Fontes de Dados

As origens usadas neste case são públicas e disponíveis para download nos links abaixo.

- **Chicago Marathon 2000–2025:** https://www.kaggle.com/datasets/ramostherunning/chicago-marathon-2000-2025
- **London Marathon Results:** https://www.kaggle.com/datasets/kevinegan/london-marathon-results
- **New York City Marathon Results:** https://www.kaggle.com/datasets/runningwithrock/nyc-marathon-results-all-years
- **BMW Berlin Marathon 1999–2025:** https://doi.org/10.5281/zenodo.19342683

Atenção: os nomes dos atletas são campos sensíveis. Na camada Silver eles são removidos e substituídos por um hash, preservando a privacidade e atendendo ao conceito de LGPD no caso.

## IV. Guia de Instalação e Execução

### 1. Pré-requisitos

Antes de começar, você precisa de:

- Uma **conta Microsoft Azure** ativa com crédito ou faturamento habilitado.
- Permissões para criar Resource Groups, Storage Accounts e Databricks Workspaces.
- **Python 3.10+** instalado localmente.
- Acesso aos datasets listados acima.

### 2. Clonar e preparar o ambiente

```powershell
git clone <URL_DO_REPOSITORIO>
cd marathon-case-data-master
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Baixar os datasets

1. Faça o download dos arquivos CSV de cada fonte.
2. Coloque os arquivos na pasta `data/raw/` do repositório.
3. Sugestão de nomes (o orquestrador reconhece os prefixos):
   - `data/raw/Chicago_Marathon_2000-2025.csv`
   - `data/raw/London_2014_mass_results.csv`
   - `data/raw/London_2014_elite_results.csv`
   - ... (demais anos de 2014 a 2022)
   - `data/raw/NYC Marathon Results.csv`
   - `data/raw/berlin_marathon_all_years.csv`

### 4. Inspecionar os dados localmente

Verifique se os arquivos estão corretos antes de subir para a nuvem:

```powershell
python scripts/inspect_raw_data.py
```

### 5. Provisionar a infraestrutura Azure

No PowerShell:

```powershell
.\scripts\setup.ps1
```

O script vai:
- Instalar o **Azure CLI**, se necessário.
- Fazer login no Azure.
- Criar o Resource Group, Storage Account (ADLS Gen2), Container `marathon-data`, Databricks Workspace e Key Vault.
- Exibir a **Storage Access Key** e a **URL do Databricks Workspace**.

> **Atenção MFA:** se o `az login` falhar por exigência de autenticação multifator, use:
>
> ```powershell
> az login --tenant <SEU_TENANT_ID>
> ```

### 6. Configurar credenciais do Databricks

1. Acesse a URL do Databricks Workspace exibida no passo anterior.
2. Gere um **Personal Access Token** em `User Settings > Access tokens`.
3. Defina as variáveis de ambiente:

```powershell
$env:DATABRICKS_HOST = "https://<id>.azuredatabricks.net"
$env:DATABRICKS_TOKEN = "dapi..."
$env:STORAGE_ACCESS_KEY = "SUA_STORAGE_KEY_AQUI"
```

### 7. Salvar o segredo no Databricks

```powershell
python scripts/setup_databricks_secrets.py
```

Esse script cria o **Secret Scope** `marathon-scope` e salva a chave do ADLS com o nome `adls-access-key`.

### 8. Subir os CSVs para o DBFS

```powershell
python scripts/upload_raw_data.py
```

Os arquivos vão para `dbfs:/FileStore/marathon/raw/`.

### 9. Criar o Databricks Workflow

```powershell
$env:DATABRICKS_REPO_PATH = "/Repos/<usuario>/marathon-case-data-master"
python scripts/create_databricks_workflow.py
```

Substitua `DATABRICKS_REPO_PATH` pelo caminho do repositório importado no Databricks Repos.

Se já tiver um cluster, pode usar:

```powershell
python scripts/create_databricks_workflow.py --cluster-id "<cluster-id>"
```

### 10. Executar o Workflow

No Databricks, acesse `Workflows > Jobs`, selecione `marathon-case-bronze-silver-gold` e clique em **Run Now**.

O workflow executa em sequência:
1. **00_bronze_orchestrator** — varre o DBFS e ingere todos os CSVs na Bronze.
2. **02_silver_etl** — gera a tabela `silver.marathons`.
3. **03_gold_aggregations** — gera as tabelas `gold.*` para o dashboard.

### 11. Conectar o Dashboard

As tabelas Gold estão prontas em `gold.*`. Você pode conectar:
- **Power BI** usando o conector do Databricks.
- **Streamlit** exportando as tabelas Gold para CSV/Parquet ou conectando via JDBC.

## V. Estrutura do Repositório

```text
marathon-case-data-master/
├── README.md
├── .gitignore
├── requirements.txt
├── config/
│   └── config.yaml
├── data/
│   └── raw/                  # CSVs brutos (não versionados)
├── infrastructure/
│   ├── main.bicep
│   ├── resources.bicep
│   └── parameters.json
├── notebooks/
│   ├── 00_bronze_orchestrator.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_etl.py
│   └── 03_gold_aggregations.py
├── scripts/
│   ├── setup.ps1
│   ├── setup_databricks_secrets.py
│   ├── upload_raw_data.py
│   ├── create_databricks_workflow.py
│   └── inspect_raw_data.py
├── src/
│   ├── utils.py
│   └── data_quality.py
└── docs/
    └── architecture.md
```

## VI. Melhorias e Considerações Finais

- Implementar testes de qualidade automatizados na Silver.
- Adicionar notificações de falha e alertas no Databricks Workflow.
- Incluir dados de clima para análise de correlação com performance.
- Automatizar a ingestão via Azure Data Factory ou Event Grid.
- Otimizar o particionamento das tabelas Gold conforme os padrões de acesso do dashboard.
- Expandir as fontes para Boston, Tóquio e outras majors, aproveitando a arquitetura extensível.
