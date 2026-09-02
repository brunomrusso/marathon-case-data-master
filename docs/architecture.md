# Arquitetura do Case

## Visão Geral

Solução de Engenharia de Dados na Azure para processar e visualizar dados das maratonas de Chicago, Londres, Nova York e Berlim. A arquitetura usa ADLS Gen2 como data lake, Databricks com PySpark/Delta Lake e Unity Catalog para governança.

## Camadas

### Raw
- Landing zone para os arquivos CSV brutos.
- Armazenada em `abfss://marathon-data@<storage>.dfs.core.windows.net/raw/`.
- A ingestão é acionada por **File Arrival Trigger** sempre que novos arquivos chegam.

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
- Tabela `bronze.weather_raw` — cache dos dados de clima obtidos da API pública Open-Meteo.
- Tabela `silver.marathons_with_weather` enriquece os resultados com condições climáticas do dia da prova (temperatura, precipitação, vento).

### Gold
- Gera agregações e métricas para o dashboard.
- Tabelas otimizadas para leitura, particionadas conforme os filtros do dashboard.
- Tabelas **externas** armazenadas em `abfss://.../gold/<tabela>`.

## Fluxo de Execução

1. Arquivos CSV são enviados para `raw/` no ADLS via `scripts/upload_raw_data.py` ou outro processo.
2. O **File Arrival Trigger** detecta a chegada e executa o Databricks Workflow.
3. `00_bronze_orchestrator` agrupa os CSVs por fonte e executa `01_bronze_ingestion` uma vez por origem. Para London, usa um glob para ler todos os anos de uma só vez.
4. `01_bronze_ingestion` lê os CSVs, sanitiza colunas, deduplica e grava na camada Bronze.
5. `02_silver_etl` processa as tabelas Bronze e gera a tabela unificada `silver.marathons`.
6. `04_weather_enrichment`:
   - Gera a tabela `bronze.marathon_metadata` (data, local, coordenadas de cada prova).
   - Busca o clima histórico na Open-Meteo e armazena em `bronze.weather_raw`.
   - Cria `silver.marathons_with_weather` juntando resultados e clima.
7. `03_gold_aggregations` cria as tabelas Gold, incluindo `gold.weather_impact`.
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
- **Observabilidade:** tabela `monitoring.data_quality_log` registra por camada:
  - `row_count_in` / `row_count_out`
  - `% nulos` em colunas-chave (`key_columns_null_pct_json`)
  - `rejected_records`
  - `schema_drift_flag`
  - `execution_time_sec` (identifica gargalos)
- **Rastreabilidade:** `run_id` e `batch_id` gerados no `00_bronze_orchestrator` e propagados via `dbutils.jobs.taskValues` para Silver, Weather e Gold.
- **Alertas:** notificações por email em falhas do Databricks Workflow (`ALERT_EMAIL`).
