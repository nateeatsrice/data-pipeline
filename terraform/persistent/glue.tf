###############################################################################
# AWS Glue Data Catalog
# Schema registry for the data lake. Spark writes tables here; Athena queries
# them via SQL. Databases point at prefixes inside the shared master bucket,
# which is owned OUTSIDE Terraform — so the data survives any destroy.
###############################################################################

locals {
  data_lake_root = "s3://nateeatsrice-master-s3/data-lake"
}

# Bronze database — raw ingested data
resource "aws_glue_catalog_database" "bronze" {
  name         = "${replace(var.project_name, "-", "_")}_bronze_${var.environment}"
  description  = "Raw ingested data — exact copies from source systems"
  location_uri = "${local.data_lake_root}/bronze/"

  lifecycle {
    prevent_destroy = true
  }
}

# Silver database — cleaned and standardized data
resource "aws_glue_catalog_database" "silver" {
  name         = "${replace(var.project_name, "-", "_")}_silver_${var.environment}"
  description  = "Cleaned, deduplicated, type-cast data"
  location_uri = "${local.data_lake_root}/silver/"

  lifecycle {
    prevent_destroy = true
  }
}

# Gold database — analytics-ready feature tables
resource "aws_glue_catalog_database" "gold" {
  name         = "${replace(var.project_name, "-", "_")}_gold_${var.environment}"
  description  = "Feature tables and aggregations for analytics and ML"
  location_uri = "${local.data_lake_root}/gold/"

  lifecycle {
    prevent_destroy = true
  }
}
