# Arquitetura do Case

## Visão Geral

Solução de Engenharia de Dados na Azure para processar e visualizar dados das maratonas de Chicago, Londres, Nova York e Berlim. A arquitetura usa ADLS Gen2 como data lake, Databricks com PySpark/Delta Lake e Unity Catalog para governança.

## Camadas

### Raw
- Landing zone para os arquivos CSV brutos e para os JSONs brutos da API Open-Meteo.
- Armazenada em `abfss://marathon-data@<storage>.dfs.core.windows.net/raw/`.
- Os arquivos `raw/weather_api/{source}/{year}/{race_date}.json` representam o landing de dados externos via API, mantendo o mesmo padrão raw → bronze.
- A ingestão CSV é acionada por **File Arrival Trigger** sempre que novos arquivos chegam.

### Bronze
- Recebe os arquivos CSV brutos de cada origem.
- Mantém os dados com o mínimo de transformação.
- Aplica registro de arquivos processados (`bronze.file_metadata`).
- Carga incremental via `MERGE` no Delta Lake, usando hash de linha para idempotência.
- Tabelas **externas** armazenadas em `abfss://.../bronze/<source>`.

### Silver
- Limpa e padroniza os dados de cada fonte.
- Unifica os esquemas diferentes das quatro maratonas.
- Aplica mascaramento e anonimização de atletas.
- Garante qualidade com regras de validação.
- Tabela **externa** armazenada em `abfss://.../silver/marathons`.
- Tabela `bronze.marathon_metadata` — data, cidade, país, latitude e longitude de cada prova (gerada via heurística ou carregada de CSV).
- Tabela `bronze.weather_raw` — cache dos dados de clima parseados a partir dos JSONs brutos em `raw/weather_api/`.
- Tabela `silver.marathons_with_weather` enriquece os resultados com condições climáticas do dia da prova (temperatura, precipitação, vento).

### Gold

Gera agregações e métricas para o dashboard. Tabelas **externas** armazenadas em `abfss://.../gold/<tabela>`.

| Tabela | Finalidade |
|---|---|
| `gold.kpi_summary` | KPIs principais por maratona e ano: total de finishers, tempo médio, record da prova e % feminino. Ponto de entrada do dashboard. |
| `gold.finishers_by_year` | Evolução histórica do número de finishers por maratona ao longo dos anos. |
| `gold.top_countries` | Ranking dos países com mais finishers por maratona, para análise de diversidade geográfica. |
| `gold.athletes_by_country` | Contagem de atletas únicos por país e maratona. |
| `gold.times_distribution` | Distribuição dos tempos de chegada em faixas (ex: < 3h, 3–4h, 4–5h, > 5h) por maratona e ano. |
| `gold.marathon_comparison` | Comparativo direto entre as quatro maratonas: tempo médio, record, total e % feminino. |
| `gold.age_gender_profile` | Perfil demográfico dos finishers: contagem e tempo médio por grupo etário e gênero. |
| `gold.weather_impact` | Correlação entre condições climáticas e desempenho médio. Criada somente quando `silver.marathons_with_weather` estiver disponível. |

### Monitoring

- `monitoring.data_quality_log`: tabela em modo **append** com uma linha por step/notebook/run. Campos: `run_id`, `batch_id`, `layer`, `step`, `source`, `year`, `row_count_in`, `row_count_out`, `rejected_records`, `key_columns_null_pct_json`, `schema_drift_flag`, `execution_time_sec`, `status`, `details`, `recorded_at`.

## Fluxo de Execução

1. Arquivos CSV são enviados para `raw/` via `scripts/upload_raw_data.py` (cria o container se não existir).
2. O **File Arrival Trigger** detecta a chegada e dispara o Databricks Workflow.
3. `00_bronze_orchestrator`:
   - Gera `run_id` (UUID) e `batch_id` (timestamp), propagados via `dbutils.jobs.taskValues`.
   - Lista os CSVs em `raw/`, **ignora** arquivos não reconhecidos como fontes de resultados (ex: `marathon_metadata.csv`) sem abortar.
   - Agrupa por fonte e chama `01_bronze_ingestion` uma vez por origem.
4. `01_bronze_ingestion` lê os CSVs, sanitiza colunas, deduplica por hash de linha e carrega na Bronze via `MERGE`. Registra métricas em `monitoring.data_quality_log` (modo append).
5. `02_silver_etl` lê as tabelas Bronze, normaliza schemas, aplica mascaramento e grava `silver.marathons`. Registra métricas.
6. `04_weather_enrichment`:
   - Lê `raw/marathon_metadata.csv` (se existir) para datas exatas; caso contrário usa heurística.
   - Grava/atualiza `bronze.marathon_metadata` via `MERGE`.
   - Para cada `(source, year)` sem registro, chama Open-Meteo, persiste JSON bruto em `raw/weather_api/`, parseia e grava em `bronze.weather_raw` via `MERGE`.
   - Cria `silver.marathons_with_weather` com join por `source + year`.
   - Registra métricas em `monitoring.data_quality_log`.
7. `03_gold_aggregations` gera todas as tabelas Gold. Usa `silver.marathons_with_weather` quando disponível; cai para `silver.marathons` caso contrário. Registra métricas.
8. O dashboard consome as tabelas Gold.

## Governança e Segurança

- Todas as tabelas são registradas no **Unity Catalog** (`marathon.bronze.*`, `marathon.silver.*`, `marathon.gold.*`, `marathon.monitoring.*`).
- Dados sensíveis (nomes e identificadores de atletas) mascarados na Silver via hash SHA-256.
- Acesso ao ADLS via **Azure Access Connector** e managed identity.
- Criptografia em trânsito e em repouso do ADLS Gen2.
- Controle de acesso via RBAC do Azure e permissões do Unity Catalog.
- **Lineage automático:** o Unity Catalog captura automaticamente o lineage de leitura/escrita entre tabelas e notebooks executados no Databricks. Para visualizar, acesse **Catalog > Tables** e clique em **Lineage** nas tabelas `silver.marathons`, `silver.marathons_with_weather` ou `gold.*`.

## Escalabilidade e Observabilidade

- ADLS Gen2 para armazenamento distribuído.
- Databricks auto-scaling para processamento.
- Delta Lake com partições por `source` e `year`.
- Ingestão event-driven: cluster só liga quando arquivos chegam.
- Ingestão de London otimizada com leitura em lote ao invés de uma chamada por arquivo.
- **Observabilidade:** tabela `monitoring.data_quality_log` (append-only, `mergeSchema=true`) registra por step/notebook:
  - `row_count_in` / `row_count_out`
  - `% nulos` em colunas-chave (`key_columns_null_pct_json`)
  - `rejected_records` (inclui arquivos ignorados no orquestrador)
  - `schema_drift_flag`
  - `execution_time_sec` — identifica gargalos por etapa
- **Rastreabilidade end-to-end:** `run_id` (UUID) e `batch_id` (timestamp) gerados no `00_bronze_orchestrator` e propagados via `dbutils.jobs.taskValues` para todos os notebooks downstream.
- **Alertas:** notificações por email configuradas no Databricks Workflow para falhas (`ALERT_EMAIL`).
- **Lineage:** o Unity Catalog captura automaticamente lineage de leitura/escrita. Visualize em **Catalog > Tables > Lineage** nas tabelas Silver e Gold.

## Decisões de Implementação

| Problema | Decisão |
|---|---|
| Schema conflict no `monitoring.data_quality_log` | Modo `overwrite` causava `DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`; migrado para `append` com `mergeSchema=true` |
| `marathon_metadata.csv` em `raw/` abortava o orquestrador | Arquivos não reconhecidos agora são ignorados com log, sem falha |
| Conflito `round`/`sum`/`min`/`max` PySpark vs Python em Gold | Funções PySpark renomeadas para `spark_round`, `spark_sum`, `spark_min`, `spark_max`; built-ins Python preservados |
| Container ADLS ausente após limpeza de storage | `upload_raw_data.py` cria o container automaticamente se não existir |
