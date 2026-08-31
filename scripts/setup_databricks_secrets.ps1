# Helper para criar o secret scope e salvar a chave do ADLS no Databricks
# Substitua o valor abaixo pela Storage Access Key exibida pelo setup.ps1

$ErrorActionPreference = "Stop"

$storageKey = Read-Host "Cole a Storage Access Key"

databricks secrets create-scope --scope marathon-scope --initial-manage-principal users
databricks secrets put --scope marathon-scope --key adls-access-key --string-value $storageKey

Write-Host "Segredo 'adls-access-key' salvo no scope 'marathon-scope'."
