# Arquitetura do Case

## Visão Geral

Solução de Engenharia de Dados na Azure para processar e visualizar dados das maratonas de Chicago, Londres e Berlim.

## Camadas

### Bronze
- Recebe os arquivos CSV brutos de cada origem.
- Mantém os dados com o mínimo de transformação.
- Aplica registro de arquivos processados (`bronze.file_metadata`).
- Carga incremental via `MERGE` no Delta Lake, usando hash de linha para idempotência.

### Silver
- Limpa e padroniza os dados de cada fonte.
- Unifica os esquemas diferentes das três maratonas.
- Aplica mascaramento e anonimização de atletas.
- Garante qualidade com regras de validação.

### Gold
- Gera agregações e métricas para o dashboard.
- Tabelas otimizadas para leitura, particionadas conforme os filtros do dashboard.

## Fluxo de Execução

1. Databricks Workflow executa `01_bronze_ingestion` para cada arquivo novo.
2. `02_silver_etl` processa as tabelas Bronze e gera a tabela unificada `silver.marathons`.
3. `03_gold_aggregations` cria as tabelas Gold.
4. O dashboard consome as tabelas Gold.

## Segurança

- Credenciais armazenadas no Azure Key Vault.
- Dados sensíveis (nomes e identificadores de atletas) mascarados na Silver.
- Criptografia em trânsito e em repouso do ADLS Gen2.
- Controle de acesso via RBAC.

## Escalabilidade

- ADLS Gen2 para armazenamento distribuído.
- Databricks auto-scaling para processamento.
- Delta Lake com partições por `source` e `year`.
