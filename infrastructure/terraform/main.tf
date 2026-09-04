locals {
  resource_group_name = "rg-${var.project_name}-${var.environment}"
  storage_name        = "st${var.project_name}${var.environment}"
  databricks_name     = "dbw-${var.project_name}-${var.environment}"
  keyvault_name       = "kv-${var.project_name}-${var.environment}"
  access_connector    = "ac-${var.project_name}-${var.environment}-v2"
  databricks_managed_rg = "databricks-rg-${var.project_name}-${var.environment}"
  storage_blob_data_contributor = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"
}

resource "azurerm_resource_group" "this" {
  name     = local.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "this" {
  name                     = local.storage_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  allow_nested_items_to_be_public = false

  tags = var.tags
}

resource "azurerm_storage_container" "raw" {
  name                  = "marathon-data"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_databricks_workspace" "this" {
  name                = local.databricks_name
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location
  sku                 = "premium"
  managed_resource_group_name = local.databricks_managed_rg

  tags = var.tags
}

resource "azurerm_key_vault" "this" {
  name                       = local.keyvault_name
  location                   = var.location
  resource_group_name        = azurerm_resource_group.this.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  tags = var.tags
}

resource "azurerm_databricks_access_connector" "this" {
  name                = local.access_connector
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location
  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "access_connector_blob" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

resource "azurerm_role_assignment" "access_connector_queue" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

resource "azurerm_role_assignment" "access_connector_storage_account" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Account Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

resource "azurerm_role_assignment" "access_connector_eventgrid" {
  scope                = azurerm_resource_group.this.id
  role_definition_name = "EventGrid EventSubscription Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

data "azurerm_client_config" "current" {}
