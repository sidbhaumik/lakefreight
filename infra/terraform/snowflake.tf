# --- Warehouses: one per lane --------------------------------------------------

resource "snowflake_warehouse" "lane" {
  for_each = var.lanes

  name                = "${local.sf}_${each.key}_WH"
  warehouse_size      = each.value.warehouse_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  comment             = "lakefreight ${each.key} lane"
}

# --- Control database: control tables (created by sql/ddl) and stages ----------

resource "snowflake_database" "control" {
  name    = var.control_database
  comment = "lakefreight control tables and stages"
}

resource "snowflake_schema" "control" {
  database = snowflake_database.control.name
  name     = var.control_schema
}

# Reads unloaded files back to check their values against the source. Binary
# columns must stay binary, not be decoded as text.
resource "snowflake_file_format_parquet" "reconcile" {
  database       = snowflake_database.control.name
  schema         = snowflake_schema.control.name
  name           = "LAKEFREIGHT_PARQUET"
  binary_as_text = "false"
  comment        = "lakefreight: read unloaded Parquet back for reconciliation"
}

resource "snowflake_grant_privileges_to_account_role" "file_format" {
  for_each = local.zones

  account_role_name = snowflake_account_role.zone[each.key].name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = snowflake_file_format_parquet.reconcile.fully_qualified_name
  }
}

# --- One role, storage integration and stage per zone ---------------------------

resource "snowflake_account_role" "zone" {
  for_each = local.zones

  name    = "${local.sf}_${each.key}_EXTRACT"
  comment = "lakefreight: unloads the ${each.key} zone"
}

resource "snowflake_storage_integration_aws" "zone" {
  for_each = local.zones

  name                      = "${local.sf}_${each.key}_S3"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = local.snowflake_role_arn[each.key]
  storage_allowed_locations = ["s3://${local.buckets[each.value.bucket]}/${each.value.prefix}/"]
  comment                   = "lakefreight ${each.key} zone"
}

resource "snowflake_stage_external_s3" "zone" {
  for_each = local.zones

  database            = snowflake_schema.control.database
  schema              = snowflake_schema.control.name
  name                = "EGRESS_${each.key}_STAGE"
  url                 = "s3://${local.buckets[each.value.bucket]}/${each.value.prefix}/"
  storage_integration = snowflake_storage_integration_aws.zone[each.key].name

  encryption {
    aws_sse_kms {
      kms_key_id = aws_kms_key.zone[each.key].arn
    }
  }

  # The IAM role must exist before Snowflake first uses the stage.
  depends_on = [aws_iam_role_policy.snowflake]
}

# --- Grants ----------------------------------------------------------------------

locals {
  zone_lanes = merge([
    for z, v in local.zones : { for l in v.lanes : "${z}/${l}" => { zone = z, lane = l } }
  ]...)
  zone_sources = merge([
    for z, _ in local.zones : { for db in var.source_databases : "${z}/${db}" => { zone = z, db = db } }
  ]...)
}

resource "snowflake_grant_privileges_to_account_role" "warehouse" {
  for_each = local.zone_lanes

  account_role_name = snowflake_account_role.zone[each.value.zone].name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.lane[each.value.lane].name
  }
}

resource "snowflake_grant_privileges_to_account_role" "control_database" {
  for_each = local.zones

  account_role_name = snowflake_account_role.zone[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.control.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "control_schema" {
  for_each = local.zones

  account_role_name = snowflake_account_role.zone[each.key].name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.control.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "control_tables" {
  for_each = local.zones

  account_role_name = snowflake_account_role.zone[each.key].name
  privileges        = ["SELECT", "INSERT", "UPDATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.control.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "control_views" {
  for_each = local.zones

  account_role_name = snowflake_account_role.zone[each.key].name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = snowflake_schema.control.fully_qualified_name
    }
  }
}

# Each zone role can use only its own stage.
resource "snowflake_grant_privileges_to_account_role" "stage" {
  for_each = local.zones

  account_role_name = snowflake_account_role.zone[each.key].name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage_external_s3.zone[each.key].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "source_database" {
  for_each = local.zone_sources

  account_role_name = snowflake_account_role.zone[each.value.zone].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = each.value.db
  }
}

resource "snowflake_grant_privileges_to_account_role" "source_schemas" {
  for_each = local.zone_sources

  account_role_name = snowflake_account_role.zone[each.value.zone].name
  privileges        = ["USAGE"]
  on_schema {
    all_schemas_in_database = each.value.db
  }
}

resource "snowflake_grant_privileges_to_account_role" "source_future_schemas" {
  for_each = local.zone_sources

  account_role_name = snowflake_account_role.zone[each.value.zone].name
  privileges        = ["USAGE"]
  on_schema {
    future_schemas_in_database = each.value.db
  }
}

resource "snowflake_grant_privileges_to_account_role" "source_tables" {
  for_each = local.zone_sources

  account_role_name = snowflake_account_role.zone[each.value.zone].name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_database        = each.value.db
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "source_future_tables" {
  for_each = local.zone_sources

  account_role_name = snowflake_account_role.zone[each.value.zone].name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = each.value.db
    }
  }
}

# --- Token cross-reference: RESTRICTED role only ---------------------------------

locals {
  xref_parts = var.xref_table == "" ? [] : split(".", var.xref_table)
  xref       = var.xref_table == "" ? {} : { RESTRICTED = local.xref_parts }
}

resource "snowflake_grant_privileges_to_account_role" "xref_database" {
  for_each = local.xref

  account_role_name = snowflake_account_role.zone["RESTRICTED"].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = each.value[0]
  }

  lifecycle {
    precondition {
      condition     = !contains(var.source_databases, each.value[0])
      error_message = "xref_table is in a source database, so every zone role could read it. Move it to a database that is not migrated."
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "xref_schema" {
  for_each = local.xref

  account_role_name = snowflake_account_role.zone["RESTRICTED"].name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${each.value[0]}\".\"${each.value[1]}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "xref_table" {
  for_each = local.xref

  account_role_name = snowflake_account_role.zone["RESTRICTED"].name
  privileges        = ["SELECT"]
  on_schema_object {
    object_type = "TABLE"
    object_name = "\"${each.value[0]}\".\"${each.value[1]}\".\"${each.value[2]}\""
  }
}

# --- Service user for the Lambda functions --------------------------------------

resource "snowflake_service_user" "pipeline" {
  name              = "${local.sf}_PIPELINE"
  comment           = "lakefreight Step Functions pipeline"
  rsa_public_key    = var.service_user_public_key
  default_role      = snowflake_account_role.zone["INTERNAL"].name
  default_warehouse = snowflake_warehouse.lane["SMALL"].name

  # The user holds all three zone roles. Snowflake's default activates them all
  # as secondary roles, which would let an INTERNAL session read PII and
  # RESTRICTED stages; each session must hold only its own zone's role.
  default_secondary_roles_option = "NONE"

  # The pipeline starts each COPY asynchronously and polls it from a later
  # Lambda call; the query must keep running when the starting session ends.
  abort_detached_query = false
}

resource "snowflake_grant_account_role" "pipeline" {
  for_each = local.zones

  role_name = snowflake_account_role.zone[each.key].name
  user_name = snowflake_service_user.pipeline.name
}
