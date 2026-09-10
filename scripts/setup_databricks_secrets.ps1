# Helper legado para criar o secret scope usado pelos notebooks
# O acesso ao ADLS usa Access Connector e Managed Identity, sem chaves de storage.

$ErrorActionPreference = "Stop"

databricks secrets create-scope --scope marathon-scope --initial-manage-principal users

Write-Host "Secret scope 'marathon-scope' criado. Nenhuma chave do ADLS e necessaria."
