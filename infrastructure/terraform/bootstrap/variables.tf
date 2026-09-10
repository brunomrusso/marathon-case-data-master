variable "location" {
  type    = string
  default = "eastus"
}

variable "github_repository" {
  type    = string
  default = "brunomrusso/marathon-case-data-master"
}

variable "tags" {
  type = map(string)
  default = {
    project     = "marathon-case-data-master"
    environment = "bootstrap"
  }
}
