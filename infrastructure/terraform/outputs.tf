output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "container_name" {
  value = azurerm_storage_container.raw.name
}

output "databricks_workspace_name" {
  value = azurerm_databricks_workspace.this.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.this.workspace_id
}

output "key_vault_name" {
  value = azurerm_key_vault.this.name
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}

output "access_connector_principal_id" {
  value = azurerm_databricks_access_connector.this.identity[0].principal_id
}

output "workspace_id" {
  value = azurerm_databricks_workspace.this.workspace_id
}
