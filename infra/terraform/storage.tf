# --- Keys: one per zone --------------------------------------------------------

resource "aws_kms_key" "zone" {
  for_each = local.zones

  description             = "${var.name} egress zone ${each.key}"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "zone" {
  for_each = local.zones

  name          = "alias/${var.name}-${lower(each.key)}"
  target_key_id = aws_kms_key.zone[each.key].key_id
}

# --- Egress buckets ------------------------------------------------------------

resource "aws_s3_bucket" "egress" {
  for_each = local.buckets

  bucket = each.value
}

resource "aws_s3_bucket_ownership_controls" "egress" {
  for_each = local.buckets

  bucket = aws_s3_bucket.egress[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "egress" {
  for_each = local.buckets

  bucket                  = aws_s3_bucket.egress[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Replication needs versioning; it also protects against accidental overwrites.
resource "aws_s3_bucket_versioning" "egress" {
  for_each = local.buckets

  bucket = aws_s3_bucket.egress[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "egress" {
  for_each = local.buckets

  bucket = aws_s3_bucket.egress[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.zone[local.bucket_default_zone[each.key]].arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "egress" {
  for_each = local.buckets

  bucket = aws_s3_bucket.egress[each.key].id

  rule {
    id     = "tidy"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
    noncurrent_version_expiration {
      noncurrent_days = var.egress_noncurrent_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.egress]
}

data "aws_iam_policy_document" "tls_only" {
  for_each = local.buckets

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      local.bucket_arn[each.key],
      "${local.bucket_arn[each.key]}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "egress" {
  for_each = local.buckets

  bucket = aws_s3_bucket.egress[each.key].id
  policy = data.aws_iam_policy_document.tls_only[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.egress]
}

# --- Control bucket: work-item lists for the Step Functions Distributed Map ----

resource "aws_s3_bucket" "control" {
  bucket = local.control_bucket
}

resource "aws_s3_bucket_public_access_block" "control" {
  bucket                  = aws_s3_bucket.control.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "control" {
  bucket = aws_s3_bucket.control.id

  rule {
    id     = "expire-run-lists"
    status = "Enabled"
    filter {
      prefix = "runs/"
    }
    expiration {
      days = 30
    }
  }
}
