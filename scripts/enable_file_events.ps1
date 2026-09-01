# Habilita file events para a external location do Databricks
# Atribui as roles necessarias ao system-assigned managed identity do Azure Access Connector

param(
    [string]$SubscriptionId = "",
    [string]$ResourceGroup = "rg-marathon-case",
    [string]$StorageAccount = "stmarathoncase",
    [string]$AccessConnector = "ac-marathon-case-v2"
)

$ErrorActionPreference = "Stop"

if (-not $SubscriptionId) {
    $account = az account show --query id -o tsv
    if (-not $account) {
        Write-Error "Execute 'az login' ou passe -SubscriptionId"
    }
    $SubscriptionId = $account
}

$scopeStorage = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Storage/storageAccounts/$StorageAccount"
$scopeRG = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup"
$connectorId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Databricks/accessConnectors/$AccessConnector"

Write-Host "Obtendo principalId do Access Connector $AccessConnector ..."
$principalId = az resource show --ids $connectorId --query properties.managedIdentity.principalId -o tsv
Write-Host "principalId = $principalId"

$roles = @(
    "Storage Blob Data Contributor",
    "Storage Queue Data Contributor",
    "Storage Account Contributor"
)

foreach ($role in $roles) {
    Write-Host "Atribuindo $role no storage account..."
    az role assignment create --assignee-object-id $principalId --assignee-principal-type ServicePrincipal --role $role --scope $scopeStorage
}

Write-Host "Atribuindo EventGrid EventSubscription Contributor no resource group..."
az role assignment create --assignee-object-id $principalId --assignee-principal-type ServicePrincipal --role "EventGrid EventSubscription Contributor" --scope $scopeRG

Write-Host "Roles atribuidas. Pode levar alguns minutos para propagar no Azure."
