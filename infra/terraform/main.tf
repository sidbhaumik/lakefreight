data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  # Snowflake identifiers: upper case, underscores.
  sf = upper(replace(var.name, "-", "_"))

  # The egress bucket holds the INTERNAL and PII zones under separate prefixes and
  # keys; RESTRICTED gets a bucket of its own.
  buckets = {
    egress     = "${var.name}-${local.account_id}-egress"
    restricted = "${var.name}-${local.account_id}-restricted"
  }
  control_bucket = "${var.name}-${local.account_id}-control"
  bucket_arn     = { for k, b in local.buckets : k => "arn:${local.partition}:s3:::${b}" }
  control_arn    = "arn:${local.partition}:s3:::${local.control_bucket}"

  zones = {
    INTERNAL   = { bucket = "egress", prefix = "internal", lanes = ["LARGE", "SMALL"] }
    PII        = { bucket = "egress", prefix = "pii", lanes = ["LARGE", "SMALL"] }
    RESTRICTED = { bucket = "restricted", prefix = "restricted", lanes = ["RESTRICTED"] }
  }

  # The key each bucket uses by default; files written through a stage use their zone's key.
  bucket_default_zone = { egress = "INTERNAL", restricted = "RESTRICTED" }

  snowflake_role_name = { for z, _ in local.zones : z => "${var.name}-snowflake-${lower(z)}" }
  snowflake_role_arn = {
    for z, n in local.snowflake_role_name : z => "arn:${local.partition}:iam::${local.account_id}:role/${n}"
  }

  control_fqn = "${var.control_database}.${var.control_schema}"
}
