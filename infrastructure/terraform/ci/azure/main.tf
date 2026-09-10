terraform {
  required_version = ">= 1.5.0"

  backend "azurerm" {}

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.117.1"
    }
  }
}

provider "azurerm" {
  features {}
}

module "platform" {
  source                        = "../.."
  create_resource_group         = false
  storage_account_name_override = "stmarathonprod"
}

output "resource_group_name" {
  value = module.platform.resource_group_name
}

output "storage_account_name" {
  value = module.platform.storage_account_name
}

output "container_name" {
  value = module.platform.container_name
}

output "databricks_workspace_name" {
  value = module.platform.databricks_workspace_name
}

output "databricks_workspace_url" {
  value = module.platform.databricks_workspace_url
}

output "databricks_workspace_id" {
  value = module.platform.databricks_workspace_id
}

output "databricks_workspace_resource_id" {
  value = module.platform.databricks_workspace_resource_id
}

output "key_vault_name" {
  value = module.platform.key_vault_name
}

output "access_connector_id" {
  value = module.platform.access_connector_id
}

output "access_connector_principal_id" {
  value = module.platform.access_connector_principal_id
}

output "workspace_id" {
  value = module.platform.workspace_id
}
