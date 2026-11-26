terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 2.24, < 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge({ "managed-by" = "lakefreight" }, var.tags)
  }
}

# Authentication comes from the usual Snowflake provider settings: environment
# variables (SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER,
# SNOWFLAKE_AUTHENTICATOR, SNOWFLAKE_PRIVATE_KEY, ...) or ~/.snowflake/config. The role must be able to
# create integrations, roles, users and warehouses, typically ACCOUNTADMIN.
provider "snowflake" {}
