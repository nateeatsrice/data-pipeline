###############################################################################
# Glue Crawler — Bronze table registration
# Bronze data is written as raw parquet straight to S3 (no Spark table
# registration), so it isn't queryable in Athena. This crawler scans the
# bronze prefixes and registers tables in the persistent bronze database.
# Run on demand: `aws glue start-crawler --name <name>`.
###############################################################################

# Role the crawler assumes. Needs S3 read on the data lake + Glue write.
resource "aws_iam_role" "bronze_crawler" {
  name = "${var.project_name}-bronze-crawler-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
      }
    ]
  })

  tags = {
    Name = "Bronze Crawler Role"
  }
}

# AWS-managed policy covering the Glue catalog + CloudWatch perms a crawler
# needs. (S3 read is granted separately below, scoped to our bucket.)
resource "aws_iam_role_policy_attachment" "bronze_crawler_glue" {
  role       = aws_iam_role.bronze_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Scoped S3 read on the bronze data only.
resource "aws_iam_role_policy" "bronze_crawler_s3" {
  name = "s3-bronze-read"
  role = aws_iam_role.bronze_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = "arn:aws:s3:::nateeatsrice-master-s3"
        Condition = {
          StringLike = { "s3:prefix" = ["data-lake/bronze/*"] }
        }
      },
      {
        Sid      = "ReadBronze"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::nateeatsrice-master-s3/data-lake/bronze/*"
      }
    ]
  })
}

# The crawler itself. Two S3 targets -> two tables in the bronze database.
resource "aws_glue_crawler" "bronze" {
  name          = "${var.project_name}-bronze-crawler-${var.environment}"
  role          = aws_iam_role.bronze_crawler.arn
  database_name = "${replace(var.project_name, "-", "_")}_bronze_${var.environment}"

  s3_target {
    path = "s3://nateeatsrice-master-s3/data-lake/bronze/nyc_tlc/yellow/"
  }

  s3_target {
    path = "s3://nateeatsrice-master-s3/data-lake/bronze/noaa_weather/nyc_daily/"
  }

  # Keep table definitions stable across reruns; only add new columns.
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Name = "Bronze Crawler"
  }
}