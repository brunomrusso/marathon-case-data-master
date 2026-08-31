targetScope = 'subscription'

param location string
param resourceGroupName string
param projectName string
param environment string
param tags object = {}

var rgTags = tags

resource rg 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: resourceGroupName
  location: location
  tags: rgTags
}

module resources 'resources.bicep' = {
  name: 'marathonResources'
  scope: rg
  params: {
    location: location
    projectName: projectName
    environment: environment
    tags: rgTags
  }
}

output resourceGroupName string = rg.name
output storageAccountName string = resources.outputs.storageAccountName
output databricksWorkspaceName string = resources.outputs.databricksWorkspaceName
output databricksWorkspaceUrl string = resources.outputs.databricksWorkspaceUrl
output keyVaultName string = resources.outputs.keyVaultName
