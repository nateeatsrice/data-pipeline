###############################################################################
# Ephemeral Stack Outputs
# Exports compute resources this stack owns (EMR, Athena, IAM) PLUS
# re-exports the persistent values it reads via remote state — so
# setup_env.sh can pull everything from one `terraform output` call.
###############################################################################

# --- Owned by this (ephemeral) stack ---
output "emr_serverless_app_id" {
  description = "EMR Serverless application ID for job submissions"
  value       = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  description = "IAM role ARN that EMR Serverless assumes"
  value       = aws_iam_role.emr_serverless.arn
}

output "pipeline_runner_role_arn" {
  description = "IAM role ARN for Airflow/local scripts"
  value       = aws_iam_role.pipeline_runner.arn
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}

# --- Re-exported from the persistent stack (read via remote state) ---
output "glue_database_bronze" {
  description = "Glue catalog database name for bronze layer"
  value       = local.glue_db_bronze
}

output "glue_database_silver" {
  description = "Glue catalog database name for silver layer"
  value       = local.glue_db_silver
}

output "glue_database_gold" {
  description = "Glue catalog database name for gold layer"
  value       = local.glue_db_gold
}

output "data_lake_root" {
  description = "S3 root URI for the data lake (master bucket prefix)"
  value       = local.data_lake_root
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}
