# IAM roles Snowflake assumes to write into each zone. Each role can reach only
# its own prefix and key, so a routing mistake fails with an access error.
#
# The policies are built with jsonencode (not aws_iam_policy_document) so they
# are known at plan time and covered by tests/plan.tftest.hcl.

locals {
  snowflake_zone_policy = {
    for z, v in local.zones : z => {
      Version = "2012-10-17"
      Statement = [
        {
          Sid    = "ZoneObjects"
          Effect = "Allow"
          Action = [
            "s3:PutObject",
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:DeleteObject",
            "s3:DeleteObjectVersion",
          ]
          Resource = "${local.bucket_arn[v.bucket]}/${v.prefix}/*"
        },
        {
          Sid       = "ZoneListing"
          Effect    = "Allow"
          Action    = "s3:ListBucket"
          Resource  = local.bucket_arn[v.bucket]
          Condition = { StringLike = { "s3:prefix" = ["${v.prefix}/*"] } }
        },
        {
          Sid      = "BucketLocation"
          Effect   = "Allow"
          Action   = "s3:GetBucketLocation"
          Resource = local.bucket_arn[v.bucket]
        },
        {
          Sid    = "ZoneKey"
          Effect = "Allow"
          Action = [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:DescribeKey",
          ]
          Resource = aws_kms_key.zone[z].arn
        },
      ]
    }
  }

  snowflake_trust_policy = {
    for z, _ in local.zones : z => {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Action    = "sts:AssumeRole"
        Principal = { AWS = snowflake_storage_integration_aws.zone[z].describe_output[0].iam_user_arn }
        Condition = {
          StringEquals = { "sts:ExternalId" = snowflake_storage_integration_aws.zone[z].describe_output[0].external_id }
        }
      }]
    }
  }
}

resource "aws_iam_role" "snowflake" {
  for_each = local.zones

  name               = local.snowflake_role_name[each.key]
  description        = "Snowflake unloads into the ${each.key} zone"
  assume_role_policy = jsonencode(local.snowflake_trust_policy[each.key])
}

resource "aws_iam_role_policy" "snowflake" {
  for_each = local.zones

  name   = "zone-${lower(each.key)}"
  role   = aws_iam_role.snowflake[each.key].id
  policy = jsonencode(local.snowflake_zone_policy[each.key])
}
