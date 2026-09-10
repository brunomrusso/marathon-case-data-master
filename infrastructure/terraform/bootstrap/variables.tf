variable "location" {
  type    = string
  default = "eastus"
}

variable "github_repository" {
  type    = string
  default = "brunomrusso/marathon-case-data-master"
}

variable "github_oidc_subject" {
  type    = string
  default = "repo:brunomrusso@30119424/marathon-case-data-master@1352762490:environment:production"
}

variable "tags" {
  type = map(string)
  default = {
    project     = "marathon-case-data-master"
    environment = "bootstrap"
  }
}
