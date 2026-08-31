# Script de setup do projeto marathon-case-data-master
# Executar no PowerShell

$ErrorActionPreference = "Stop"

function Refresh-EnvPath {
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
}

function Test-LoggedIn {
    $account = az account show 2>&1 | ConvertFrom-Json
    return [bool]$account
}

function Assert-Command($cmd) {
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Falha no comando: $cmd"
    }
}

# Verificar Azure CLI
if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    Write-Host "Instalando Azure CLI..."
    winget install -e --id Microsoft.AzureCLI
    Refresh-EnvPath
    if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
        Write-Error "Azure CLI foi instalado, mas 'az' ainda nao foi encontrado no PATH. Feche e abra um novo PowerShell e rode o script novamente."
    }
}

# Login na Azure
az login
if (-not (Test-LoggedIn)) {
    Write-Error "Login no Azure falhou. Verifique MFA/condicional access e tente 'az login --tenant <TENANT_ID>'."
}

# Definir subscription, se fornecida
$subscriptionId = Read-Host "Subscription ID (deixe em branco para usar a atual)"
if ($subscriptionId) {
    az account set --subscription $subscriptionId
    Assert-Command "az account set"
}

$location = "eastus"
$deployName = "marathon-case-deploy-{0:yyyyMMddHHmmss}" -f (Get-Date)
$bicepFile = "infrastructure/main.bicep"
$parametersFile = "infrastructure/parameters.json"

# Deploy Bicep
Write-Host "Criando infraestrutura no Azure (deployment: $deployName)..."
az deployment sub create `
  --name $deployName `
  --location $location `
  --template-file $bicepFile `
  --parameters $parametersFile
Assert-Command "az deployment sub create"

# Recuperar outputs
$deployment = (az deployment sub show --name $deployName) | ConvertFrom-Json
if ($deployment.properties.provisioningState -ne "Succeeded") {
    Write-Error "O deploy do Bicep nao foi concluido com sucesso. Verifique o erro acima."
}

$rg = $deployment.properties.outputs.resourceGroupName.value
$storage = $deployment.properties.outputs.storageAccountName.value
$wsUrl = $deployment.properties.outputs.databricksWorkspaceUrl.value
$kv = $deployment.properties.outputs.keyVaultName.value
$accessConnectorId = $deployment.properties.outputs.accessConnectorId.value

# Obter storage key
$storageKey = (az storage account keys list `
  --account-name $storage `
  --resource-group $rg `
  --query '[0].value' `
  -o tsv)
Assert-Command "az storage account keys list"

Write-Host ""
Write-Host "==== Recursos criados ===="
Write-Host "Resource Group: $rg"
Write-Host "Storage Account: $storage"
Write-Host "Container: marathon-data"
Write-Host "Databricks Workspace URL: $wsUrl"
Write-Host "Key Vault: $kv"
Write-Host "Access Connector ID: $accessConnectorId"
Write-Host ""
Write-Host "Storage Access Key (guarde em local seguro):"
Write-Host $storageKey
Write-Host ""
Write-Host "Proximos passos:"
Write-Host "1. Acesse o Databricks Workspace: $wsUrl"
Write-Host "2. Gere um Personal Access Token em User Settings > Access tokens"
Write-Host "3. Defina as variaveis de ambiente:"
Write-Host "   `$env:DATABRICKS_HOST = `"$wsUrl`""
Write-Host "   `$env:DATABRICKS_TOKEN = `"seu-token`""
Write-Host "   `$env:STORAGE_ACCESS_KEY = `"$storageKey`""
Write-Host "   `$env:ACCESS_CONNECTOR_ID = `"$accessConnectorId`""
Write-Host "4. Salve os segredos: python scripts/setup_databricks_secrets.py"
Write-Host "5. Configure o Unity Catalog: python scripts/setup_unity_catalog.py"
Write-Host "6. Suba os CSVs: python scripts/upload_raw_data.py"
Write-Host "7. Crie o workflow: python scripts/create_databricks_workflow.py"
