# Case Engenharia de Dados — World Marathon Majors

## I. Objetivo do Case

Desenvolver uma solução completa de Engenharia de Dados para ingerir, processar, armazenar e visualizar dados de resultados de maratonas. A solução demonstra extração, ingestão batch, arquitetura medalhão (Bronze/Silver/Gold), observabilidade, segurança, mascaramento de dados sensíveis, escalabilidade, governança via Unity Catalog e reprodutibilidade.

## II. Arquitetura

### Tecnologias
- **Cloud:** Microsoft Azure
- **Armazenamento:** Azure Data Lake Storage Gen2 com Delta Lake
- **Processamento:** Azure Databricks + PySpark
- **Orquestração:** Databricks Workflows
- **Ingestão:** File Arrival Trigger no Databricks Workflow
- **Governança:** Unity Catalog, External Locations e Managed Identities
- **Observabilidade:** Databricks Job Metrics + tabela `bronze.file_metadata`
- **Segurança:** Azure Key Vault, RBAC, criptografia, mascaramento e Access Connector
- **Dashboard:** Power BI ou Streamlit

### Arquitetura Medalhão
- **Raw:** arquivos CSV brutos armazenados no ADLS (`raw/`), servindo como landing zone.
- **Bronze:** ingestão dos CSVs com registro de arquivos processados e carga incremental via `MERGE`. Tabelas **externas** no ADLS (`bronze/<source>`).
- **Silver:** limpeza, padronização de schema, integração das fontes, mascaramento/anonimização e validação. Tabela **externa** no ADLS (`silver/marathons`).
- **Gold:** agregações e métricas para alimentar o dashboard. Tabelas **externas** no ADLS (`gold/<tabela>`).

Todas as camadas são catalogadas no **Unity Catalog** (`marathon.bronze.*`, `marathon.silver.*`, `marathon.gold.*`), mas com os arquivos Delta armazenados em locais controlados pelo ADLS.

### Fluxo de Dados
```
CSV local -> ADLS raw/ -> File Arrival Trigger -> 00_bronze_orchestrator -> 01_bronze_ingestion -> Bronze (Delta)
Bronze -> 02_silver_etl -> Silver (Delta) -> 03_gold_aggregations -> Gold (Delta) -> Dashboard
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
3. Nomes reconhecidos pelo orquestrador:
   - `data/raw/Chicago_Marathon_2000-2025.csv`
   - `data/raw/London_2014_mass_results.csv`
   - `data/raw/London_2014_elite_results.csv`
   - ... (demais anos de 2014 a 2022)
   - `data/raw/NYC Marathon Results.csv`
   - `data/raw/Berlin_Marathon_1999-2025_original.csv`

### 4. Inspecionar os dados localmente

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
- Criar o Resource Group, Storage Account (ADLS Gen2), Container `marathon-data`, Databricks Workspace, Access Connector e Key Vault.
- Exibir o **Storage Account**, a **Storage Access Key** e o **Access Connector ID**.

> **Atenção MFA:** se o `az login` falhar por exigência de autenticação multifator, use:
>
> ```powershell
> az login --tenant <SEU_TENANT_ID>
> ```

### 6. Configurar o Databricks Workspace

1. Acesse a URL do Databricks Workspace exibida no passo anterior.
2. Gere um **Personal Access Token** em `User Settings > Access tokens`.
3. Defina as variáveis de ambiente:

```powershell
$env:DATABRICKS_HOST = "https://<id>.azuredatabricks.net"
$env:DATABRICKS_TOKEN = "dapi..."
$env:ACCESS_CONNECTOR_ID = "/subscriptions/.../accessConnectors/ac-marathon-case-v2"
$env:STORAGE_ACCESS_KEY = "SUA_STORAGE_KEY_AQUI"
```

O `ACCESS_CONNECTOR_ID` e a `STORAGE_ACCESS_KEY` são exibidos no final do `setup.ps1`.

### 7. Criar o catálogo e external location no Unity Catalog

```powershell
python scripts/setup_unity_catalog.py
```

Esse script:
- Cria um **Storage Credential** usando o Access Connector.
- Cria um **External Location** apontando para `abfss://marathon-data@<storage>.dfs.core.windows.net/`.
- Cria o catálogo `marathon` com managed storage.
- Salva o segredo `catalog_name` no scope `marathon-scope`.

> Importante: execute este passo **antes** de criar os outros segredos, pois os notebooks dependem do catálogo `marathon`.

### 8. Salvar o segredo do config.yaml no Databricks

```powershell
python scripts/setup_databricks_secrets.py
```

Esse script cria o **Secret Scope** `marathon-scope` e salva o conteúdo do arquivo `config/config.yaml` com o nome `config_yaml`.

### 9. Subir os CSVs para a camada raw do ADLS

```powershell
python scripts/upload_raw_data.py
```

Os arquivos vão para `abfss://marathon-data@<storage>.dfs.core.windows.net/raw/`.

> A partir deste ponto, novos arquivos colocados na pasta `raw/` do ADLS disparam automaticamente o pipeline via **File Arrival Trigger** (passo 10).

### 10. Criar o Databricks Workflow com File Arrival Trigger

```powershell
$env:DATABRICKS_REPO_PATH = "/Workspace/Repos/<usuario>/marathon-case-data-master"
python scripts/create_databricks_workflow.py
```

Substitua `DATABRICKS_REPO_PATH` pelo caminho do repositório importado no Databricks Repos.

O workflow criado já vem com um trigger do tipo **File Arrival** que monitora a pasta `raw/` do ADLS. Sempre que novos arquivos chegarem, o Databricks inicia o cluster, executa Bronze → Silver → Gold e desliga o cluster automaticamente.

Se já tiver um cluster existente, pode usá-lo:

```powershell
$env:DATABRICKS_REPO_PATH = "/Workspace/Repos/<usuario>/marathon-case-data-master"
$env:DATABRICKS_CLUSTER_ID = "<cluster-id>"
python scripts/create_databricks_workflow.py
```

### 11. Executar o Workflow

A primeira execução pode ser iniciada manualmente no Databricks em `Workflows > Jobs`, selecionando `marathon-case-bronze-silver-gold` e clicando em **Run Now**.

Nas próximas vezes, o pipeline dispara sozinho quando novos arquivos chegam na camada `raw`.

O workflow executa em sequência:
1. **00_bronze_orchestrator** — lê `raw/` do ADLS e ingere os CSVs por fonte na Bronze (uma chamada por fonte; London lido de uma só vez via glob).
2. **01_bronze_ingestion** — executado internamente pelo orquestrador; lê, limpa e grava cada tabela Bronze.
3. **02_silver_etl** — gera a tabela `silver.marathons`.
4. **03_gold_aggregations** — gera as tabelas `gold.*` para o dashboard.

### 12. Conectar o Dashboard

As tabelas Gold estão prontas em `gold.*`. Você pode conectar:
- **Power BI** usando o conector do Databricks.
- **Streamlit** conectando via JDBC/ODBC ou exportando as tabelas Gold para CSV/Parquet.

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
├── docs/
│   └── architecture.md
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
│   ├── setup_unity_catalog.py
│   ├── setup_databricks_secrets.py
│   ├── upload_raw_data.py
│   ├── create_databricks_workflow.py
│   └── inspect_raw_data.py
├── src/
│   ├── utils.py
│   └── data_quality.py
```

## VI. Melhorias e Considerações Finais

- Implementar testes de qualidade automatizados na Silver.
- Adicionar notificações de falha e alertas no Databricks Workflow.
- Incluir dados de clima para análise de correlação com performance.
- Automatizar a ingestão via Azure Data Factory ou Event Grid.
- Otimizar o particionamento das tabelas Gold conforme os padrões de acesso do dashboard.
- Expandir as fontes para Boston, Tóquio e outras majors, aproveitando a arquitetura extensível.
