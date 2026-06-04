###############################################################################
# Remote State — read the persistent stack's outputs.
# This is how ephemeral consumes values it does NOT own (Glue DB names,
# data lake path). Ephemeral reads persistent.tfstate but never modifies it.
###############################################################################

data "terraform_remote_state" "persistent" {
  backend = "s3"

  config = {
    bucket = "nateeatsrice-master-s3"
    key    = "terraform-state/nyc-taxi-pipeline/persistent.tfstate"
    region = "us-east-2"
  }
}

# Convenience locals so the rest of the stack reads cleanly.
locals {
  glue_db_bronze = data.terraform_remote_state.persistent.outputs.glue_database_bronze
  glue_db_silver = data.terraform_remote_state.persistent.outputs.glue_database_silver
  glue_db_gold   = data.terraform_remote_state.persistent.outputs.glue_database_gold
  data_lake_root = data.terraform_remote_state.persistent.outputs.data_lake_root
}
