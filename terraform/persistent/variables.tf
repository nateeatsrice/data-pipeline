###############################################################################
# Project-wide variables
# These are referenced across all Terraform files. Override in terraform.tfvars
###############################################################################

variable "project_name" {
  description = "Project identifier used to name all AWS resources"
  type        = string
  default     = "nyc-taxi-pipeline"
}

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-2"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# S3
variable "data_bucket_name" {
  description = "Name for the main data lake S3 bucket"
  type        = string
  default     = "" # Will be auto-generated if empty
}

variable "scripts_bucket_name" {
  description = "Name for the S3 bucket that holds PySpark scripts"
  type        = string
  default     = "" # Will be auto-generated if empty
}

# EMR Serverless
variable "emr_release_label" {
  description = "EMR release version for Spark runtime"
  type        = string
  default     = "emr-7.1.0"
}

variable "spark_driver_cores" {
  description = "vCPU cores for Spark driver"
  type        = string
  default     = "2"
}

variable "spark_driver_memory" {
  description = "Memory for Spark driver"
  type        = string
  default     = "4g"
}

variable "spark_executor_cores" {
  description = "vCPU cores per Spark executor"
  type        = string
  default     = "2"
}

variable "spark_executor_memory" {
  description = "Memory per Spark executor"
  type        = string
  default     = "4g"
}

variable "emr_max_capacity_cpu" {
  description = "Max vCPU for EMR Serverless auto-scaling"
  type        = string
  default     = "16"
}

variable "emr_max_capacity_memory" {
  description = "Max memory for EMR Serverless auto-scaling"
  type        = string
  default     = "64 GB"
}

variable "emr_idle_timeout" {
  description = "Minutes of inactivity before EMR Serverless releases resources"
  type        = number
  default     = 5
}

# Tags applied to every resource for cost tracking
variable "tags" {
  description = "Default tags for all resources"
  type        = map(string)
  default     = {}
}

locals {
  # Generate bucket names if not explicitly set
  data_bucket_name    = var.data_bucket_name != "" ? var.data_bucket_name : "${var.project_name}-data-lake-${var.environment}"
  scripts_bucket_name = var.scripts_bucket_name != "" ? var.scripts_bucket_name : "${var.project_name}-scripts-${var.environment}"

  common_tags = merge(var.tags, {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  })
}
