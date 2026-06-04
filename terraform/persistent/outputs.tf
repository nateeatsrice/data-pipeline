###############################################################################
# Persistent Stack Outputs
# These define the CONTRACT this stack exposes to the ephemeral stack
# (via terraform_remote_state) and to setup_env.sh.
# Only export what the persistent stack actually manages: the Glue catalog.
###############################################################################

output "glue_database_bronze" {
  description = "Glue catalog database name for bronze layer"
  value       = aws_glue_catalog_database.bronze.name
}

output "glue_database_silver" {
  description = "Glue catalog database name for silver layer"
  value       = aws_glue_catalog_database.silver.name
}

output "glue_database_gold" {
  description = "Glue catalog database name for gold layer"
  value       = aws_glue_catalog_database.gold.name
}

output "data_lake_root" {
  description = "S3 root URI for the data lake (master bucket prefix)"
  value       = local.data_lake_root
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}
