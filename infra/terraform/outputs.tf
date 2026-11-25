locals {
  lakefreight_toml = templatefile("${path.module}/lakefreight.toml.tftpl", {
    control_fqn               = local.control_fqn
    full_threshold_bytes      = var.full_threshold_bytes
    target_chunk_bytes        = var.target_chunk_bytes
    unmatched_token_threshold = var.unmatched_token_threshold
    xref_table                = var.xref_table == "" ? "NONE.NONE.NONE" : var.xref_table
    xref_token_column         = var.xref_token_column
    xref_clear_column         = var.xref_clear_column
    zones = {
      for z, v in local.zones : z => {
        stage      = "${local.control_fqn}.EGRESS_${z}_STAGE"
        bucket     = local.buckets[v.bucket]
        prefix     = v.prefix
        role       = "${local.sf}_${z}_EXTRACT"
        kms_key_id = aws_kms_key.zone[z].arn
      }
    }
    lanes = {
      for l, v in var.lanes : l => {
        warehouse       = "${local.sf}_${l}_WH"
        max_concurrency = v.max_concurrency
      }
    }
  })
}

output "lakefreight_toml" {
  description = "Settings matching this deployment. Save with: terraform output -raw lakefreight_toml > lakefreight.toml"
  value       = local.lakefreight_toml
}

output "state_machine_arn" {
  description = "Start one execution per lane with input {\"lane\": \"LARGE\"}, {\"lane\": \"SMALL\"} or {\"lane\": \"RESTRICTED\"}."
  value       = aws_sfn_state_machine.extract_lane.arn
}

output "snowflake_secret_arn" {
  description = "Put the service user's credentials here as JSON: {\"account\", \"user\", \"private_key\"}."
  value       = aws_secretsmanager_secret.snowflake.arn
}

output "snowflake_service_user" {
  value = snowflake_service_user.pipeline.name
}

output "egress_buckets" {
  value = { for k, b in aws_s3_bucket.egress : k => b.id }
}

output "zone_kms_keys" {
  value = { for z, k in aws_kms_key.zone : z => k.arn }
}

output "replication_role_arn" {
  description = "Allow this role in the receiving account's bucket and key policies."
  value       = length(aws_iam_role.replication) > 0 ? aws_iam_role.replication[0].arn : null
}
