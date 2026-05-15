###############################################################################
# IAM Roles & Policies
# Follows least-privilege principle: each service gets only what it needs
###############################################################################

# ─── EMR Serverless Execution Role ──────────────────────────────────────────
# This role is assumed by EMR Serverless when running your PySpark jobs.
# It needs: read/write S3 data, read scripts, write to Glue catalog.

resource "aws_iam_role" "emr_serverless" {
  name = "${var.project_name}-emr-serverless-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "EMR Serverless Execution Role"
  }
}

# S3 access for EMR Serverless
resource "aws_iam_role_policy" "emr_s3_access" {
  name = "s3-access"
  role = aws_iam_role.emr_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadWriteDataLake"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Sid    = "ReadScripts"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      },
      {
        Sid    = "WriteAthenaResults"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      }
    ]
  })
}

# Glue Data Catalog access for EMR Serverless
# Spark needs this to register tables and read schema info
resource "aws_iam_role_policy" "emr_glue_access" {
  name = "glue-catalog-access"
  role = aws_iam_role.emr_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.project_name}_*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.project_name}_*/*"
        ]
      }
    ]
  })
}

# CloudWatch Logs for EMR Serverless job logs
resource "aws_iam_role_policy" "emr_cloudwatch" {
  name = "cloudwatch-logs"
  role = aws_iam_role.emr_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/emr-serverless/*"
      }
    ]
  })
}

# ─── Pipeline Runner Role ───────────────────────────────────────────────────
# Used by Airflow (running locally) to submit EMR jobs, run Athena queries,
# and upload data to S3. You'll configure your local AWS CLI profile with
# credentials that can assume this role.

resource "aws_iam_role" "pipeline_runner" {
  name = "${var.project_name}-pipeline-runner-${var.environment}"

  # Allow your IAM user to assume this role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
      }
    ]
  })

  tags = {
    Name = "Pipeline Runner Role"
  }
}

resource "aws_iam_role_policy" "pipeline_runner" {
  name = "pipeline-permissions"
  role = aws_iam_role.pipeline_runner.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DataAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*",
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        Sid    = "EMRServerlessAccess"
        Effect = "Allow"
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun",
          "emr-serverless:ListJobRuns",
          "emr-serverless:GetApplication",
          "emr-serverless:ListApplications"
        ]
        Resource = "*"
      },
      {
        Sid      = "PassRoleToEMR"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.emr_serverless.arn
      },
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution"
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueReadAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = "*"
      }
    ]
  })
}
