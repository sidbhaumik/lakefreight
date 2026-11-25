# Optional: replicate each egress bucket to a bucket in the receiving account.
# The receiving account must, in its own configuration:
#   - allow this module's replication role (output replication_role_arn) to
#     replicate into its bucket, in the bucket policy
#   - allow that role to encrypt with its KMS key, in the key policy

data "aws_iam_policy_document" "replication_trust" {
  count = length(var.replication) > 0 ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "replication" {
  count = length(var.replication) > 0 ? 1 : 0

  statement {
    sid       = "ReadSourceConfig"
    actions   = ["s3:GetReplicationConfiguration", "s3:ListBucket"]
    resources = [for k, _ in var.replication : local.bucket_arn[k]]
  }

  statement {
    sid = "ReadSourceObjects"
    actions = [
      "s3:GetObjectVersionForReplication",
      "s3:GetObjectVersionAcl",
      "s3:GetObjectVersionTagging",
    ]
    resources = [for k, _ in var.replication : "${local.bucket_arn[k]}/*"]
  }

  statement {
    sid = "WriteDestination"
    actions = [
      "s3:ReplicateObject",
      "s3:ReplicateDelete",
      "s3:ReplicateTags",
      "s3:ObjectOwnerOverrideToBucketOwner",
    ]
    resources = [for _, r in var.replication : "${r.destination_bucket_arn}/*"]
  }

  statement {
    sid       = "DecryptSource"
    actions   = ["kms:Decrypt"]
    resources = [for k in aws_kms_key.zone : k.arn]
  }

  statement {
    sid       = "EncryptDestination"
    actions   = ["kms:Encrypt", "kms:GenerateDataKey"]
    resources = [for _, r in var.replication : r.destination_kms_key_arn]
  }
}

resource "aws_iam_role" "replication" {
  count = length(var.replication) > 0 ? 1 : 0

  name               = "${var.name}-replication"
  assume_role_policy = data.aws_iam_policy_document.replication_trust[0].json
}

resource "aws_iam_role_policy" "replication" {
  count = length(var.replication) > 0 ? 1 : 0

  name   = "replicate-egress"
  role   = aws_iam_role.replication[0].id
  policy = data.aws_iam_policy_document.replication[0].json
}

resource "aws_s3_bucket_replication_configuration" "egress" {
  for_each = var.replication

  bucket = aws_s3_bucket.egress[each.key].id
  role   = aws_iam_role.replication[0].arn

  rule {
    id     = "to-receiving-account"
    status = "Enabled"
    filter {}

    delete_marker_replication {
      status = "Disabled"
    }

    source_selection_criteria {
      sse_kms_encrypted_objects {
        status = "Enabled"
      }
    }

    destination {
      bucket  = each.value.destination_bucket_arn
      account = each.value.destination_account_id

      access_control_translation {
        owner = "Destination"
      }

      encryption_configuration {
        replica_kms_key_id = each.value.destination_kms_key_arn
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.egress]
}
