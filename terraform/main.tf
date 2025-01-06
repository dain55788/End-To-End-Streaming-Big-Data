# Main terraform setup with AWS
terraform {
  required_version = ">=1.0"
  backend "local" {}
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

provider "aws" {
#  project = var.project
#  region  = var.region
#  zone    = var.zone
  // credentials = file(var.credentials)
}
