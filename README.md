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
- **Observabilidade:** Databricks Job Metrics + tabela `monitoring.data_quality_log` com contagem in/out, % nulos, rejeitados, schema drift e tempo de execução por camada. Alertas por email no workflow. Lineage automático do Unity Catalog.
- **Segurança:** Azure Key Vault, RBAC, criptografia, mascaramento e Access Connector
- **Dashboard:** Power BI ou Streamlit

### Arquitetura Medalhão
- **Raw:** landing zone para CSV de resultados e JSONs brutos da API Open-Meteo (`raw/weather_api/`). Nenhum dado é processado nesta camada.
- **Bronze:** ingestão dos CSVs com registro de arquivos processados, carga incremental via `MERGE` e detecção de schema drift. Tabelas **externas** no ADLS (`bronze/<source>`). Inclui `bronze.marathon_metadata` e `bronze.weather_raw`.
- **Silver:** limpeza, padronização de schema, integração das fontes, mascaramento/anonimização e validação. Inclui `silver.marathons` e `silver.marathons_with_weather` (enriquecida com clima). Tabelas **externas** no ADLS.
- **Gold:** agregações e métricas para alimentar o dashboard. Tabelas **externas** no ADLS (`gold/<tabela>`). Ver seção de tabelas Gold.
- **Monitoring:** tabela `monitoring.data_quality_log` com métricas de qualidade por camada, rastreabilidade end-to-end via `run_id`/`batch_id`, schema drift e tempo de execução.

Todas as camadas são catalogadas no **Unity Catalog** (`marathon.bronze.*`, `marathon.silver.*`, `marathon.gold.*`), mas com os arquivos Delta armazenados em locais controlados pelo ADLS.

### Fluxo de Dados
```
CSV local ──► ADLS raw/ ──► File Arrival Trigger ──► 00_bronze_orchestrator (gera run_id/batch_id)
                                                              │
                                              ┌───────────────┘
                                              ▼
                                    01_bronze_ingestion ──► Bronze (Delta) + monitoring.data_quality_log
                                              │
                                              ▼
                                    02_silver_etl ──► silver.marathons + monitoring.data_quality_log
                                              │
                                              ▼
                                    04_weather_enrichment ──► Open-Meteo API ──► raw/weather_api/ (JSON bruto)
                                              │                               ──► bronze.weather_raw
                                              │                               ──► silver.marathons_with_weather
                                              │
                                              ▼
                                    03_gold_aggregations ──► gold.* + monitoring.data_quality_log
                                              │
                                              ▼
                                         Dashboard
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
- **Azure CLI** instalado e logado (`az login`).
- **Terraform** instalado (CLI): https://developer.hashicorp.com/terraform/install
- Acesso aos datasets listados acima.
- Permissão para criar o metastore do Unity Catalog (Account Admin ou Metastore Admin na Databricks; o setup guia a obtencao do Account ID).

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

### 5. Setup unificado (recomendado)

Crie o arquivo `.env` a partir do exemplo:

```powershell
cp .env.example .env
```

Edite o `.env` e preencha pelo menos:

```env
ALERT_EMAIL=seu-email@exemplo.com                    # opcional
DATABRICKS_REPO_PATH=/Workspace/Repos/<usuario>/marathon-case-data-master
```

> `DATABRICKS_ACCOUNT_ID` sera solicitado durante o setup. Nao precisa preencher antecipadamente.

Execute o setup unico:

```powershell
python scripts/setup_all.py
```

Esse script orquestra todo o resto, com **apenas uma acao manual**: colar o Account ID.

1. Checa prerequisitos (Python, Azure CLI, Terraform)
2. Garante login no Azure
3. Cria a infraestrutura Azure via Terraform (resource group, storage, workspace, access connector, key vault)
4. Atualiza `config/config.yaml` com os recursos criados
5. Gera Azure AD token para a API do Databricks
6. **Abre o workspace no navegador e pede o Databricks Account ID** (copiado da URL do Account Console)
7. Cria/escolhe o metastore, atribui ao workspace e cria storage credential, external location e catalog
8. Salva secrets no Databricks
9. Registra EventGrid provider e atribui roles ao Access Connector
10. Sobe os CSVs para `raw/`
11. Cria o workflow com File Arrival Trigger

Se falhar em qualquer passo, basta corrigir o problema e rodar novamente:

```powershell
python scripts/setup_all.py
```

O script retoma de onde parou, pois salva o progresso em `.setup_state.json`.

Para recomecar do zero:

```powershell
python scripts/setup_all.py --reset
```

> **Atenção MFA:** se o `az login` falhar por exigência de autenticação multifator, use:
>
> ```powershell
> az login --tenant <SEU_TENANT_ID>
> ```

### 6. Executar o Workflow

A primeira execução pode ser iniciada manualmente no Databricks em `Workflows > Jobs`, selecionando `marathon-case-bronze-silver-gold` e clicando em **Run Now**.

Nas próximas vezes, o pipeline dispara sozinho quando novos arquivos chegam na camada `raw`.

> **Importante:** O Databricks provisiona automaticamente um EventGrid System Topic no storage account na primeira execução do trigger. Isso é esperado e indica que o File Arrival está funcionando. O status do trigger deve mudar de `failed` para `no run triggered` (aguardando novos arquivos) após o provisionamento.
>
> Se o trigger continuar falhando por mais de 30 min após os passos acima, pause e despausa o trigger em **Workflows > Jobs > marathon-case-bronze-silver-gold > Triggers** para forçar um retry limpo.

---

### Setup manual (alternativa)

Se preferir executar cada passo separadamente, os scripts individuais continuam disponiveis:

- `infrastructure/terraform/` — deploy completo via Terraform
- `scripts/setup_all.py` — orquestracao unificada
- `scripts/upload_raw_data.py` — upload dos CSVs
- `scripts/create_databricks_workflow.py` — criação do workflow

> **O Bicep (`infrastructure/main.bicep` e `resources.bicep`) ainda existe como alternativa**, mas nao cria automaticamente o metastore do Unity Catalog. Use o Terraform para provisionamento end-to-end.

O workflow executa em sequência:
1. **00_bronze_orchestrator** — lê `raw/` do ADLS, gera `run_id`/`batch_id` e ingere os CSVs por fonte na Bronze (uma chamada por fonte; London lido de uma só vez via glob).
2. **01_bronze_ingestion** — executado internamente pelo orquestrador; lê, limpa, deduplica e grava cada tabela Bronze. Loga métricas em `monitoring.data_quality_log`.
3. **02_silver_etl** — gera a tabela `silver.marathons` e loga qualidade (registros inválidos, % nulos, schema drift).
4. **04_weather_enrichment** — enriquece a Silver com dados climáticos do dia da prova (temperatura, precipitação, vento) via API pública Open-Meteo. Salva o JSON bruto da API em `raw/weather_api/` (padrão raw landing), gera `bronze.marathon_metadata`, `bronze.weather_raw` e `silver.marathons_with_weather`. Também loga API failures e cache.
5. **03_gold_aggregations** — gera as tabelas `gold.*` para o dashboard, incluindo `gold.weather_impact`, e loga agregações e schema drift.

> **Rastreamento end-to-end:** `run_id` e `batch_id` são gerados no `00_bronze_orchestrator` e propagados via `dbutils.jobs.taskValues` para Silver, Weather e Gold. A tabela `monitoring.data_quality_log` permite rastrear cada execução por camada, incluindo `row_count_in`, `row_count_out`, `rejected_records`, `% nulos`, `schema_drift_flag` e `execution_time_sec`.
>
> **Sobre as datas das provas:** O notebook `04_weather_enrichment` gera `bronze.marathon_metadata` estimando a data de cada prova com base em padrões históricos (ex: último domingo de setembro para Berlim). Se quiser datas exatas, crie um arquivo `data/raw/marathon_metadata.csv` com as colunas `source,year,marathon_name,city,country,latitude,longitude,race_date` e suba para o ADLS raw/. O notebook faz MERGE/upsert nessa tabela e usa o CSV automaticamente quando ele existe. O exemplo está em `notebooks/marathon_metadata.csv.example`.

### 12. Tabelas Gold — Finalidade

Todas as tabelas Gold ficam em `marathon.gold.*` e são o ponto de consumo do dashboard.

| Tabela | Finalidade |
|---|---|
| `gold.kpi_summary` | KPIs principais por maratona e ano: total de finishers, tempo médio, record da prova e % feminino. Ponto de entrada do dashboard. |
| `gold.finishers_by_year` | Evolução histórica do número de finishers por maratona. Permite visualizar crescimento ou queda de participação ao longo dos anos. |
| `gold.top_countries` | Ranking dos países com mais finishers por maratona, útil para análise de diversidade geográfica. |
| `gold.athletes_by_country` | Contagem de atletas únicos por país e maratona, diferenciando participação individual de contagem de finishes. |
| `gold.times_distribution` | Distribuição dos tempos de chegada em faixas (ex: < 3h, 3–4h, 4–5h, > 5h) por maratona e ano. Permite análise de perfil de desempenho. |
| `gold.marathon_comparison` | Comparativo direto entre as quatro maratonas: tempo médio, record, total de finishers e % feminino. Ideal para gráficos de barras comparativos. |
| `gold.age_gender_profile` | Perfil demográfico dos finishers: contagem e tempo médio por grupo etário e gênero. Permite identificar o perfil dominante em cada prova. |
| `gold.weather_impact` | Correlação entre condições climáticas (temperatura, precipitação, vento) e desempenho médio dos atletas. Disponível somente quando `silver.marathons_with_weather` está populada. |

### 13. Conectar o Dashboard

As tabelas Gold estão prontas em `gold.*`. Você pode conectar:
- **Power BI** usando o conector do Databricks.
- **Streamlit** conectando via JDBC/ODBC ou exportando as tabelas Gold para CSV/Parquet.

## V. Estrutura do Repositório

```text
marathon-case-data-master/
├── README.md
├── .gitignore
├── .env.example            # template de variaveis de ambiente
├── requirements.txt
├── config/
│   └── config.yaml
├── data/
│   └── raw/                # CSVs brutos (não versionados)
├── docs/
│   └── architecture.md
├── infrastructure/
│   ├── terraform/           # provisionamento end-to-end (recomendado)
│   │   ├── main.tf
│   │   ├── metastore.tf
│   │   ├── providers.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── terraform.tfvars.example
│   ├── main.bicep           # alternativa Azure-only
│   ├── resources.bicep
│   └── parameters.json
├── notebooks/
│   ├── 00_bronze_orchestrator.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_etl.py
│   ├── 03_gold_aggregations.py
│   ├── 04_weather_enrichment.py
│   └── marathon_metadata.csv.example
├── scripts/
│   ├── setup_all.py              # setup unificado (recomendado)
│   ├── setup_unity_catalog.py    # cria/escolhe metastore e configura UC
│   ├── setup.ps1                 # deploy do Bicep (alternativa)
│   ├── setup_databricks_secrets.py
│   ├── upload_raw_data.py
│   ├── create_databricks_workflow.py
│   ├── enable_file_events.py     # versao Python
│   ├── enable_file_events.ps1    # versao PowerShell
│   └── inspect_raw_data.py
├── src/
│   ├── utils.py
│   └── data_quality.py
```

## VI. Melhorias e Considerações Finais

- Implementar testes de qualidade automatizados na Silver (Great Expectations / Delta Live Tables expectations).
- Adicionar dashboard de observabilidade com custo/tempo por run a partir de `monitoring.data_quality_log`.
- Otimizar o particionamento das tabelas Gold conforme os padrões de acesso do dashboard.
- Expandir as fontes para Boston, Tóquio e outras majors, aproveitando a arquitetura extensível.
- Buscar datas exatas das provas via API de calendário/esportes para substituir a estimativa heurística usada no `04_weather_enrichment`.
- `monitoring.data_quality_log` já funciona em modo append; adicionar particionamento por `batch_id` para histórico de longo prazo.

## VII. Changelog

### [2025] — Setup unificado e simplificacao

- **Setup unificado:** novo `scripts/setup_all.py` executa todo o provisionamento e configuracao em um unico comando, com persistencia de estado para retomada.
- **Infraestrutura como Terraform:** pasta `infrastructure/terraform/` cria Azure resources e Databricks workspace de forma automatizada.
- **Configuracao do Unity Catalog via script:** `scripts/setup_unity_catalog.py` cria/escolhe metastore, atribui o workspace e cria storage credential, external location e catalog.
- **Sem token manual:** o setup usa Azure AD token do CLI, eliminando a geracao manual de Databricks Personal Access Token.
- **Unica acao manual:** colar o Databricks Account ID (obtido da URL do Account Console) no terminal durante o setup.
- **Arquivo `.env`:** centraliza configuracoes de ambiente (Databricks account ID, email de alerta, repo path).
- **Versao Python do enable_file_events:** nao depende mais exclusivamente do PowerShell.
- **Bicep mantido como alternativa:** arquivos `infrastructure/main.bicep` e `resources.bicep` continuam disponiveis, mas nao automatizam o metastore.

### [2025] — Ajustes de execução e correções de pipeline

- **Observabilidade append-only:** tabela `monitoring.data_quality_log` migrada de `overwrite` para `append` com `mergeSchema=true`, eliminando conflito de schema entre notebooks executados na mesma run.
- **Ignorar arquivos não-fonte no orquestrador:** `00_bronze_orchestrator.py` agora ignora arquivos como `marathon_metadata.csv` que não são fontes de resultados de maratona, em vez de abortar com `ValueError`.
- **Correção de conflito de nomes PySpark/Python no Gold:** no `03_gold_aggregations.py`, as funções `round`, `sum`, `min` e `max` importadas do PySpark foram renomeadas para `spark_round`, `spark_sum`, `spark_min` e `spark_max`, preservando os built-ins do Python para uso em listas e arredondamento escalares.
- **Upload automático de container:** o script `upload_raw_data.py` agora cria o container ADLS automaticamente se ele não existir.
