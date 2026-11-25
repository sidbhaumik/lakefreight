variable "name" {
  description = "Prefix for every resource name. Lower case letters, digits and hyphens."
  type        = string
  default     = "lakefreight"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,30}$", var.name))
    error_message = "name must be 2-31 characters of lower case letters, digits and hyphens, starting with a letter."
  }
}

variable "aws_region" {
  description = "AWS region for the egress buckets and the pipeline. Use the region of your Snowflake account to avoid cross-region unload charges."
  type        = string
}

variable "tags" {
  description = "Extra tags for every AWS resource."
  type        = map(string)
  default     = {}
}

# --- Snowflake -----------------------------------------------------------------

variable "source_databases" {
  description = "Snowflake databases to migrate. The extract roles get read access to every table in them."
  type        = list(string)

  validation {
    condition     = length(var.source_databases) > 0
    error_message = "List at least one source database."
  }
}

variable "control_database" {
  description = "Snowflake database to create for lakefreight's control tables and stages."
  type        = string
  default     = "LAKEFREIGHT"
}

variable "control_schema" {
  description = "Schema to create in the control database."
  type        = string
  default     = "CONTROL"
}

variable "xref_table" {
  description = "Fully qualified token cross-reference table (DB.SCHEMA.TABLE) for restricted columns. Leave empty if you have none. Keep it outside the source databases so only the RESTRICTED role can read it."
  type        = string
  default     = ""

  validation {
    condition     = var.xref_table == "" || can(regex("^[A-Za-z0-9_$]+\\.[A-Za-z0-9_$]+\\.[A-Za-z0-9_$]+$", var.xref_table))
    error_message = "xref_table must be DATABASE.SCHEMA.TABLE, or empty."
  }
}

variable "xref_token_column" {
  description = "Column of the cross-reference table holding the token."
  type        = string
  default     = "TOKEN_VALUE"
}

variable "xref_clear_column" {
  description = "Column of the cross-reference table holding the clear value."
  type        = string
  default     = "CLEAR_VALUE"
}

variable "lanes" {
  description = "Warehouse size and Step Functions concurrency for each lane."
  type = map(object({
    warehouse_size  = string
    max_concurrency = number
  }))
  default = {
    LARGE      = { warehouse_size = "XLARGE", max_concurrency = 8 }
    SMALL      = { warehouse_size = "MEDIUM", max_concurrency = 32 }
    RESTRICTED = { warehouse_size = "LARGE", max_concurrency = 4 }
  }

  validation {
    condition     = toset(keys(var.lanes)) == toset(["LARGE", "SMALL", "RESTRICTED"])
    error_message = "lanes must define exactly LARGE, SMALL and RESTRICTED."
  }
}

variable "service_user_public_key" {
  description = "RSA public key for the pipeline's Snowflake service user, on one line without the BEGIN/END lines. Put the matching private key in the Secrets Manager secret this module creates."
  type        = string
}

# --- Extraction settings written into the pipeline's lakefreight.toml ----------

variable "full_threshold_bytes" {
  description = "Tables at or below this size are unloaded in one COPY."
  type        = number
  default     = 107374182400
}

variable "target_chunk_bytes" {
  description = "Target size of one date chunk for larger tables."
  type        = number
  default     = 80530636800
}

variable "unmatched_token_threshold" {
  description = "Most rows per chunk allowed to have a token with no cross-reference match."
  type        = number
  default     = 0
}

# --- AWS -----------------------------------------------------------------------

variable "lambda_zip_path" {
  description = "Path to the Lambda package built by infra/build_lambda.sh."
  type        = string
  default     = "../../build/lakefreight-lambda.zip"
}

variable "lambda_architecture" {
  description = "Lambda architecture; must match the one the package was built for."
  type        = string
  default     = "x86_64"

  validation {
    condition     = contains(["x86_64", "arm64"], var.lambda_architecture)
    error_message = "lambda_architecture must be x86_64 or arm64."
  }
}

variable "egress_noncurrent_days" {
  description = "Days to keep overwritten or deleted object versions in the egress buckets."
  type        = number
  default     = 7
}

variable "replication" {
  description = <<-EOT
    Optional cross-account replication of each egress bucket to a bucket owned by
    the receiving account. Keys are "egress" (INTERNAL and PII zones) and
    "restricted". The receiving account must allow this module's replication role
    in its bucket policy and key policy.
  EOT
  type = map(object({
    destination_bucket_arn  = string
    destination_account_id  = string
    destination_kms_key_arn = string
  }))
  default = {}

  validation {
    condition     = alltrue([for k in keys(var.replication) : contains(["egress", "restricted"], k)])
    error_message = "replication keys must be \"egress\" or \"restricted\"."
  }
}
