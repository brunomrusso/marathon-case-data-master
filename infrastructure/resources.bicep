targetScope = 'resourceGroup'

param location string
param projectName string
param environment string
param tags object = {}

var storageAccountName = 'st${projectName}${environment}'
var containerName = 'marathon-data'
var databricksName = 'dbw-${projectName}-${environment}'
var keyVaultName = 'kv-${projectName}-${environment}'
var databricksManagedRg = 'databricks-rg-${projectName}-${environment}'

resource storage 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    isHnsEnabled: true
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
  }
  tags: tags
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
  name: '${storage.name}/default/${containerName}'
  properties: {}
}

resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksName
  location: location
  sku: { name: 'premium' }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', databricksManagedRg)
  }
  tags: tags
}

resource keyVault 'Microsoft.KeyVault/vaults@2022-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { name: 'standard', family: 'A' }
    enableRbacAuthorization: true
  }
  tags: tags
}

output storageAccountName string = storage.name
output databricksWorkspaceName string = databricks.name
output databricksWorkspaceUrl string = databricks.properties.workspaceUrl
output keyVaultName string = keyVault.name
