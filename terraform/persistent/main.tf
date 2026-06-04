###############################################################################
# Persistent Stack — Provider & Backend
# Holds data + catalog that must survive `terraform destroy` of compute.
###############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "nateeatsrice-master-s3"
    key            = "terraform-state/nyc-taxi-pipeline/persistent.tfstate"
    region         = "us-east-2"
    dynamodb_table = "nateeatsrice-tflock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
