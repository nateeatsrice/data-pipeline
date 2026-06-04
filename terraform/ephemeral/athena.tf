###############################################################################
# Athena Workgroup
# Workgroups let you set per-query data scan limits (cost control)
# and route query results to a specific S3 location.
###############################################################################

resource "aws_athena_workgroup" "main" {
  name          = "${var.project_name}-${var.environment}"
  description   = "Workgroup for querying gold layer feature tables"
  force_destroy = var.environment == "dev"

  configuration {
    # Enforce the result location so you don't accidentally write elsewhere
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://nateeatsrice-master-s3/athena-results/data-pipeline/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    # Cost control: limit each query to scanning 1 GB max.
    # At $5/TB, this caps any single query at ~$0.005.
    # Increase this if you need to scan more data.
    bytes_scanned_cutoff_per_query = 1073741824 # 1 GB
  }

  tags = {
    Name = "Pipeline Query Workgroup"
  }
}
