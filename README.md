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
- **Dashboard:** Databricks AI/BI provisionado por Terraform; Streamlit opcional para consumo externo

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
- **Terraform 64-bit** instalado e no `PATH`: https://developer.hashicorp.com/terraform/install
  - O provider Databricks não suporta Windows 32-bit (`windows_386`). No Windows, instale a versão AMD64; o setup também detecta instalações feitas pelo WinGet.
- Acesso aos datasets listados acima.
- Permissão de **Workspace Admin** no Databricks workspace que será criado.

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

O `.env` pode manter apenas configurações opcionais:

```env
ALERT_EMAIL=seu-email@exemplo.com                        # opcional
DATABRICKS_WORKSPACE_ROOT=                               # opcional; padrao: /Workspace/Shared/marathon-case
```

> Nenhum token GitHub ou Databricks precisa ser criado. O setup usa a identidade Microsoft Entra ID da sessão `az login` e obtém tokens temporários por `DefaultAzureCredential`.

Execute o setup unico:

```powershell
python scripts/setup_all.py
```

Esse script orquestra todo o resto usando a sessão Microsoft Entra ID já autenticada no Azure CLI, sem persistir tokens de acesso.

Fluxo do script:

1. Checa prerequisitos (Python, Azure CLI, Terraform)
2. Garante login no Azure
3. Cria a infraestrutura Azure via Terraform (resource group, storage, workspace, access connector, key vault)
4. Atualiza `config/config.yaml` com os recursos criados
5. Autentica no Databricks via Microsoft Entra ID e verifica o Unity Catalog
6. Cria storage credential, external location e catalog no Unity Catalog
7. Salva secrets no Databricks
8. Registra EventGrid provider e atribui roles ao Access Connector
9. Sobe os CSVs para `raw/`
10. Implanta os notebooks locais em `/Workspace/Shared/marathon-case/notebooks`
11. Valida individualmente os notebooks implantados
12. Cria o workflow com File Arrival Trigger
13. Cria o SQL Warehouse e publica o Databricks AI/BI Dashboard

O deploy usa a Workspace API com sobrescrita idempotente. O GitHub permanece como controle de versão, mas não é uma dependência do setup nem da execução do workflow. Para uma migração temporária, `DATABRICKS_REPO_PATH` ainda é aceito internamente como caminho legado se `DATABRICKS_WORKSPACE_ROOT` não estiver definido.

O upload para o ADLS usa `DefaultAzureCredential` e o RBAC `Storage Blob Data Contributor` atribuído pelo Terraform à identidade que executa o setup. Para registrar a storage credential no Unity Catalog, a mesma identidade recebe `Contributor` somente sobre o Access Connector. Nenhuma Storage Account Key ou PAT é consultada ou persistida. O uploader aguarda automaticamente a propagação do RBAC em respostas temporárias `401/403`.

Se falhar em qualquer passo, basta corrigir o problema e rodar novamente:

```powershell
python scripts/setup_all.py
```

O script retoma de onde parou, pois salva o progresso em `.setup_state.json`.

Para recomecar do zero (não apaga a infraestrutura, apenas o estado do setup):

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

### 13. Dashboards

#### 13.1 Databricks AI/BI Dashboard

O dashboard oficial do case é versionado em `dashboard/databricks/marathon_dashboard.lvdash.json` e provisionado automaticamente pelo último passo de `scripts/setup_all.py`.

O Terraform separado em `infrastructure/terraform/databricks/` cria:

- SQL Warehouse Serverless `2X-Small`, Photon habilitado, cluster único e auto-stop de 10 minutos;
- dashboard publicado em `/Shared/marathon-case`;
- associação automática ao catálogo `marathon` e schema `gold`;
- outputs do warehouse, HTTP Path e dashboard.

O setup usa `no_wait=true`: não bloqueia esperando o compute iniciar e imprime a URL publicada ao final. Na primeira consulta, o warehouse pode permanecer em `STARTING` enquanto a Azure provisiona o cluster.

O dashboard possui páginas para:

- visão executiva e KPIs;
- evolução de concluintes e top países;
- participação e tempo médio por país;
- distribuição de tempos, quartis e mediana;
- perfil por faixa etária e gênero;
- comparação histórica entre as quatro maratonas;
- relação entre clima e performance;
- qualidade e observabilidade usando `marathon.monitoring.data_quality_log`.

As oito tabelas de `marathon.gold` são consumidas explicitamente pelo dashboard.

#### 13.2 Streamlit opcional

O consumidor externo em `dashboard/app.py` continua disponível. O setup preenche `DATABRICKS_HTTP_PATH` automaticamente com o warehouse provisionado.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run dashboard/app.py
```

### 14. CI/CD federado com GitHub Actions

A esteira usa GitHub OIDC e uma User Assigned Managed Identity. Não existem client secrets, PATs ou chaves de Storage no GitHub. Pull requests e pushes na `main` executam somente validações locais; tags `v*` acionam o provisionamento completo no environment protegido `production`. Para isolar ownership no metastore compartilhado, o setup local usa o catálogo `marathon` e a esteira usa `marathon_prod`, sem alterar o nome visual do dashboard.

#### 14.1 Bootstrap único

O bootstrap cria recursos persistentes que não fazem parte do ambiente descartável:

- Resource Group `rg-marathon-bootstrap`;
- identidade `id-marathon-github` e credencial federada para o environment `production`, vinculada aos IDs imutáveis do proprietário e do repositório;
- backend privado para os states Terraform;
- Storage Seed privado para os CSVs brutos;
- Resource Group vazio `rg-marathon-case`, onde a identidade recebe `Contributor` e `User Access Administrator`.

Execute o bootstrap apenas com o ambiente descartável removido, pois ele passa a ser o proprietário do Resource Group alvo:

```powershell
az login
python scripts/bootstrap_ci.py
```

O script aplica `infrastructure/terraform/bootstrap`, envia os CSVs locais para o Seed usando Entra ID e imprime as GitHub Actions Variables necessárias. O state do bootstrap permanece local e deve ser preservado em armazenamento administrativo seguro; ele não contém credenciais.

#### 14.2 Configuração do GitHub

Crie o environment `production`, restrinja-o a tags `v*` e, para uma implantação controlada, configure required reviewers. Cadastre nele como **Variables**, não Secrets, os valores impressos pelo bootstrap:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `TF_BACKEND_RESOURCE_GROUP`
- `TF_BACKEND_STORAGE_ACCOUNT`
- `TF_BACKEND_CONTAINER`
- `SEED_STORAGE_ACCOUNT`
- `SEED_CONTAINER`

O workflow possui somente `contents: read` e `id-token: write`; as ações externas estão fixadas por commit SHA. O deploy não contém comandos `destroy` e usa concurrency lock para impedir duas implantações simultâneas.

#### 14.3 Criar uma release

```powershell
git tag v1.0.0
git push origin v1.0.0
```

O workflow `.github/workflows/release-deploy.yml` autentica por OIDC, baixa o Seed privado, aplica os states remotos de Azure e Databricks, executa o setup sem interação e aguarda o workflow Bronze/Silver/Gold finalizar. O workflow `.github/workflows/validate.yml` não recebe identidade Azure.

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
│   │   ├── terraform.tfvars.example
│   │   ├── databricks/       # SQL Warehouse e AI/BI Dashboard
│   │   ├── bootstrap/        # identidade OIDC, backend e Storage Seed
│   │   └── ci/               # roots Terraform com backend remoto
│   ├── main.bicep           # alternativa Azure-only
│   ├── resources.bicep
│   └── parameters.json
├── dashboard/
│   ├── app.py                       # consumidor Streamlit opcional
│   └── databricks/
│       └── marathon_dashboard.lvdash.json
├── notebooks/
│   ├── 00_bronze_orchestrator.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_etl.py
│   ├── 03_gold_aggregations.py
│   ├── 04_weather_enrichment.py
│   └── marathon_metadata.csv.example
├── .github/workflows/            # validação e deploy por tag com OIDC
├── scripts/
│   ├── bootstrap_ci.py           # bootstrap único da esteira
│   ├── run_databricks_workflow.py
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
- Expandir o dashboard de observabilidade com métricas de custo por run e alertas operacionais.
- Otimizar o particionamento das tabelas Gold conforme os padrões de acesso do dashboard.
- Expandir as fontes para Boston, Tóquio e outras majors, aproveitando a arquitetura extensível.
- Buscar datas exatas das provas via API de calendário/esportes para substituir a estimativa heurística usada no `04_weather_enrichment`.
- `monitoring.data_quality_log` já funciona em modo append; adicionar particionamento por `batch_id` para histórico de longo prazo.

## VII. Changelog

### [2025] — Setup unificado e simplificacao

- **Setup unificado:** novo `scripts/setup_all.py` executa todo o provisionamento e configuracao em um unico comando, com persistencia de estado para retomada.
- **Infraestrutura como Terraform:** pasta `infrastructure/terraform/` cria Azure resources e Databricks workspace de forma automatizada.
- **Configuracao do Unity Catalog via script:** `scripts/setup_unity_catalog.py` cria/escolhe metastore, atribui o workspace e cria storage credential, external location e catalog.
- **Autenticação sem PAT:** APIs e SQL Connector usam tokens temporários Microsoft Entra ID obtidos por `DefaultAzureCredential`; nenhum PAT é solicitado ou persistido.
- **Arquivo `.env`:** centraliza somente configurações não secretas, como host, email de alerta, HTTP Path e caminho opcional no workspace.
- **Versao Python do enable_file_events:** nao depende mais exclusivamente do PowerShell.
- **Bicep mantido como alternativa:** arquivos `infrastructure/main.bicep` e `resources.bicep` continuam disponiveis, mas nao automatizam o metastore.

### [2025] — Ajustes de execução e correções de pipeline

- **Observabilidade append-only:** tabela `monitoring.data_quality_log` migrada de `overwrite` para `append` com `mergeSchema=true`, eliminando conflito de schema entre notebooks executados na mesma run.
- **Ignorar arquivos não-fonte no orquestrador:** `00_bronze_orchestrator.py` agora ignora arquivos como `marathon_metadata.csv` que não são fontes de resultados de maratona, em vez de abortar com `ValueError`.
- **Correção de conflito de nomes PySpark/Python no Gold:** no `03_gold_aggregations.py`, as funções `round`, `sum`, `min` e `max` importadas do PySpark foram renomeadas para `spark_round`, `spark_sum`, `spark_min` e `spark_max`, preservando os built-ins do Python para uso em listas e arredondamento escalares.
- **Upload sem chaves:** o script `upload_raw_data.py` usa Microsoft Entra ID, `DefaultAzureCredential` e RBAC com retry de propagação; nenhuma Storage Account Key é consultada ou persistida.
