###############################################################################
# EMR Serverless Application
# This is the "application" (runtime environment) — it sits idle at $0 cost.
# Actual compute only spins up when you submit a job run.
###############################################################################

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark-${var.environment}"
  release_label = var.emr_release_label
  type          = "spark"

  # Maximum resources the application can use across ALL concurrent jobs.
  # This is a safety cap, not a reservation — you're not charged for this.
  maximum_capacity {
    cpu    = "${var.emr_max_capacity_cpu} vCPU"
    memory = var.emr_max_capacity_memory
  }

  # Auto-stop releases all pre-initialized capacity after idle timeout.
  # This is what makes serverless cheap: no idle cost.
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.emr_idle_timeout
  }

  # Auto-start spins up the app when a job is submitted.
  auto_start_configuration {
    enabled = true
  }

  tags = {
    Name = "Spark Processing Engine"
  }
}
