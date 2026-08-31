targetScope = 'resourceGroup'

param location string
param projectName string
param environment string
param tags object = {}

var storageAccountName = 'st${projectName}${environment}'
var containerName = 'marathon-data'
var databricksName = 'dbw-${projectName}-${environment}'
var keyVaultName = 'kv-${projectName}-${environment}'
var accessConnectorName = 'ac-${projectName}-${environment}-v2'
var databricksManagedRg = 'databricks-rg-${projectName}-${environment}'
var storageBlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

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

resource accessConnector 'Microsoft.Databricks/accessConnectors@2022-10-01-preview' = {
  name: accessConnectorName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
  tags: tags
}

resource storageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(accessConnector.id, storage.id, storageBlobDataContributor)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributor)
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output storageAccountName string = storage.name
output databricksWorkspaceName string = databricks.name
output databricksWorkspaceUrl string = databricks.properties.workspaceUrl
output keyVaultName string = keyVault.name
output accessConnectorId string = accessConnector.id
