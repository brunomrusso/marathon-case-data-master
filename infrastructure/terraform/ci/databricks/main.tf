terraform {
  required_version = ">= 1.5.0"

  backend "azurerm" {}

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.105.0"
    }
  }
}

provider "databricks" {}

module "workspace" {
  source = "../../databricks"
}

output "sql_warehouse_id" {
  value = module.workspace.sql_warehouse_id
}

output "sql_warehouse_http_path" {
  value = module.workspace.sql_warehouse_http_path
}

output "dashboard_id" {
  value = module.workspace.dashboard_id
}
