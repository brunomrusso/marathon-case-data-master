variable "location" {
  description = "Regiao Azure"
  type        = string
  default     = "eastus"
}

variable "project_name" {
  description = "Nome do projeto (prefixo dos recursos)"
  type        = string
  default     = "marathon"
}

variable "environment" {
  description = "Ambiente (ex: case, dev, prod)"
  type        = string
  default     = "case"
}

variable "storage_account_name_override" {
  description = "Nome alternativo para isolar o storage entre modos de deploy"
  type        = string
  default     = null
}

variable "create_resource_group" {
  description = "Cria o resource group; use false quando ele for gerenciado pelo bootstrap de CI/CD"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags aplicadas aos recursos"
  type        = map(string)
  default = {
    project     = "marathon-case-data-master"
    environment = "case"
  }
}
