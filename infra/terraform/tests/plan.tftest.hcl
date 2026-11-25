# Plans the whole module without cloud credentials. The AWS provider runs offline
# (IAM policy documents are rendered for real); Snowflake is mocked.

provider "aws" {
  region                      = "us-east-2"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
}

mock_provider "snowflake" {
  mock_resource "snowflake_storage_integration_aws" {
    defaults = {
      describe_output = [{
        iam_user_arn             = "arn:aws:iam::999999999999:user/snowflake"
        external_id              = "SF_EXTERNAL_ID"
        allowed_locations        = []
        blocked_locations        = []
        comment                  = ""
        enabled                  = true
        id                       = "mock"
        object_acl               = ""
        provider                 = "S3"
        role_arn                 = ""
        use_privatelink_endpoint = false
      }]
    }
  }
  mock_resource "snowflake_schema" {
    defaults = { fully_qualified_name = "\"LAKEFREIGHT\".\"CONTROL\"" }
  }
}

override_data {
  target = data.aws_caller_identity.current
  values = { account_id = "111111111111" }
}

# Values AWS only assigns on create, fixed so the plan can be checked.
override_resource {
  target = aws_kms_key.zone
  values = { arn = "arn:aws:kms:us-east-2:111111111111:key/zone" }
}

variables {
  aws_region              = "us-east-2"
  source_databases        = ["SALES", "HR"]
  xref_table              = "SECURE.PUBLIC.TOKEN_XREF"
  service_user_public_key = "MIIBIjANBgkqh"
  lambda_zip_path         = "tests/fixtures/lambda.zip"
}

run "plans" {
  command = plan

  assert {
    condition     = aws_s3_bucket.egress["egress"].bucket == "lakefreight-111111111111-egress"
    error_message = "unexpected egress bucket name"
  }

  # Each Snowflake role reaches only its own prefix and key.
  assert {
    condition     = local.snowflake_zone_policy["PII"].Statement[0].Resource == "arn:aws:s3:::lakefreight-111111111111-egress/pii/*"
    error_message = "PII role must be limited to the pii/ prefix"
  }
  assert {
    condition     = local.snowflake_zone_policy["RESTRICTED"].Statement[0].Resource == "arn:aws:s3:::lakefreight-111111111111-restricted/restricted/*"
    error_message = "RESTRICTED role must be limited to its own bucket"
  }

  assert {
    condition     = local.snowflake_zone_policy["PII"].Statement[3].Resource == aws_kms_key.zone["PII"].arn && local.snowflake_zone_policy["PII"].Statement[1].Condition.StringLike["s3:prefix"] == ["pii/*"]
    error_message = "PII role must be limited to the PII key and pii/ listing"
  }

  # Each zone's integration points at that zone's role and location only.
  assert {
    condition     = snowflake_storage_integration_aws.zone["PII"].storage_aws_role_arn == "arn:aws:iam::111111111111:role/lakefreight-snowflake-pii" && snowflake_storage_integration_aws.zone["PII"].storage_allowed_locations == toset(["s3://lakefreight-111111111111-egress/pii/"])
    error_message = "PII integration must use the PII role and prefix"
  }

  # The settings handed to the Lambdas parse and match the deployment.
  assert {
    condition     = strcontains(local.lakefreight_toml, "[zones.RESTRICTED]\nstage = \"LAKEFREIGHT.CONTROL.EGRESS_RESTRICTED_STAGE\"\nbucket = \"lakefreight-111111111111-restricted\"")
    error_message = "lakefreight.toml zones do not match the stages and buckets"
  }
  assert {
    condition     = aws_lambda_function.handler["reconcile"].handler == "lakefreight.handlers.reconcile" && aws_lambda_function.handler["reconcile"].timeout == 900
    error_message = "reconcile Lambda is misconfigured"
  }

  # Only the RESTRICTED role is granted the cross-reference table.
  assert {
    condition     = length(snowflake_grant_privileges_to_account_role.xref_table) == 1 && snowflake_grant_privileges_to_account_role.xref_table["RESTRICTED"].account_role_name == "LAKEFREIGHT_RESTRICTED_EXTRACT"
    error_message = "xref must be granted to the RESTRICTED role only"
  }

  # 3 zones x 2 source databases.
  assert {
    condition     = length(snowflake_grant_privileges_to_account_role.source_tables) == 6
    error_message = "every zone role needs read access to every source database"
  }

  assert {
    condition     = snowflake_service_user.pipeline.abort_detached_query == false
    error_message = "asynchronous COPY needs abort_detached_query = false"
  }

  assert {
    condition     = length(aws_s3_bucket_replication_configuration.egress) == 0
    error_message = "replication is off unless configured"
  }
}

run "xref_inside_a_source_database_is_refused" {
  command = plan

  variables {
    xref_table = "SALES.PUBLIC.TOKEN_XREF"
  }

  expect_failures = [snowflake_grant_privileges_to_account_role.xref_database]
}

run "replication" {
  command = plan

  variables {
    replication = {
      egress = {
        destination_bucket_arn  = "arn:aws:s3:::receiver-ingress"
        destination_account_id  = "222222222222"
        destination_kms_key_arn = "arn:aws:kms:us-east-1:222222222222:key/abc"
      }
    }
  }

  assert {
    condition     = aws_s3_bucket_replication_configuration.egress["egress"].rule[0].destination[0].access_control_translation[0].owner == "Destination"
    error_message = "replicas must belong to the receiving account"
  }
}

run "bad_name_is_refused" {
  command = plan

  variables {
    name = "Lake_Freight"
  }

  expect_failures = [var.name]
}
