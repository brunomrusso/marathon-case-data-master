terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.117.1"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.7.2"
    }
  }
}

provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 8
  upper   = false
  special = false
}

resource "azurerm_resource_group" "bootstrap" {
  name     = "rg-marathon-bootstrap"
  location = var.location
  tags     = var.tags
}

resource "azurerm_resource_group" "target" {
  name     = "rg-marathon-case"
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "state" {
  name                            = "stmarathontf${random_string.suffix.result}"
  resource_group_name             = azurerm_resource_group.bootstrap.name
  location                        = azurerm_resource_group.bootstrap.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}

resource "azurerm_role_assignment" "executor_state" {
  scope                = azurerm_storage_account.state.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_container" "state" {
  name                  = "tfstate"
  storage_account_name  = azurerm_storage_account.state.name
  container_access_type = "private"
  depends_on            = [azurerm_role_assignment.executor_state]
}

resource "azurerm_storage_account" "seed" {
  name                            = "stmarathonseed${random_string.suffix.result}"
  resource_group_name             = azurerm_resource_group.bootstrap.name
  location                        = azurerm_resource_group.bootstrap.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}

resource "azurerm_storage_container" "seed" {
  name                  = "marathon-seed"
  storage_account_name  = azurerm_storage_account.seed.name
  container_access_type = "private"
  depends_on            = [azurerm_role_assignment.executor_seed]
}

resource "azurerm_user_assigned_identity" "github" {
  name                = "id-marathon-github"
  resource_group_name = azurerm_resource_group.bootstrap.name
  location            = azurerm_resource_group.bootstrap.location
  tags                = var.tags
}

resource "azurerm_federated_identity_credential" "github_production" {
  name                = "github-production"
  resource_group_name = azurerm_resource_group.bootstrap.name
  parent_id           = azurerm_user_assigned_identity.github.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = "https://token.actions.githubusercontent.com"
  subject             = "repo:${var.github_repository}:environment:production"
}

resource "azurerm_role_assignment" "github_target_contributor" {
  scope                = azurerm_resource_group.target.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_user_assigned_identity.github.principal_id
}

resource "azurerm_role_assignment" "github_target_rbac" {
  scope                = azurerm_resource_group.target.id
  role_definition_name = "User Access Administrator"
  principal_id         = azurerm_user_assigned_identity.github.principal_id
}

resource "azurerm_role_assignment" "github_state" {
  scope                = azurerm_storage_account.state.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.github.principal_id
}

resource "azurerm_role_assignment" "github_seed_reader" {
  scope                = azurerm_storage_account.seed.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.github.principal_id
}

resource "azurerm_role_assignment" "executor_seed" {
  scope                = azurerm_storage_account.seed.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

output "azure_client_id" {
  value = azurerm_user_assigned_identity.github.client_id
}

output "azure_tenant_id" {
  value = data.azurerm_client_config.current.tenant_id
}

output "azure_subscription_id" {
  value = data.azurerm_client_config.current.subscription_id
}

output "state_resource_group" {
  value = azurerm_resource_group.bootstrap.name
}

output "state_storage_account" {
  value = azurerm_storage_account.state.name
}

output "state_container" {
  value = azurerm_storage_container.state.name
}

output "seed_storage_account" {
  value = azurerm_storage_account.seed.name
}

output "seed_container" {
  value = azurerm_storage_container.seed.name
}

output "github_identity_principal_id" {
  value = azurerm_user_assigned_identity.github.principal_id
}
