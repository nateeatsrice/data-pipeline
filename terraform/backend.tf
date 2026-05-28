terraform {
  backend "s3" {
    bucket         = "nateeatsrice-master-s3"
    key            = "terraform-state/nyc-taxi-pipeline/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "nateeatsrice-tflock"
    encrypt        = true
  }
}