terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.105.0"
    }
  }
}

provider "databricks" {}

resource "databricks_sql_endpoint" "dashboard" {
  name                      = "marathon-dashboard-warehouse"
  cluster_size              = "2X-Small"
  min_num_clusters          = 1
  max_num_clusters          = 1
  auto_stop_mins            = 10
  enable_photon             = true
  enable_serverless_compute = true
  warehouse_type            = "PRO"
  no_wait                   = true
}

resource "databricks_dashboard" "marathon" {
  display_name      = "Marathon Majors Analytics"
  warehouse_id      = databricks_sql_endpoint.dashboard.id
  file_path         = "${path.module}/../../../dashboard/databricks/marathon_dashboard.lvdash.json"
  embed_credentials = true
  parent_path       = "/Shared/marathon-case"
  dataset_catalog   = "marathon"
  dataset_schema    = "gold"
}

resource "databricks_permissions" "warehouse_usage" {
  sql_endpoint_id = databricks_sql_endpoint.dashboard.id

  access_control {
    group_name       = "users"
    permission_level = "CAN_USE"
  }
}

resource "databricks_permissions" "dashboard_usage" {
  dashboard_id = databricks_dashboard.marathon.id

  access_control {
    group_name       = "users"
    permission_level = "CAN_RUN"
  }
}

output "sql_warehouse_id" {
  value = databricks_sql_endpoint.dashboard.id
}

output "sql_warehouse_http_path" {
  value = databricks_sql_endpoint.dashboard.odbc_params[0].path
}

output "dashboard_id" {
  value = databricks_dashboard.marathon.id
}
