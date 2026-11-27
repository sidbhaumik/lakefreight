# Security policy

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it privately through GitHub instead: on the repository's **Security** tab, choose **Report a vulnerability**.

Include what you found, how to reproduce it, and the version of lakefreight you used. You should get a reply within a week. If the report is confirmed, a fix is released and the advisory is published with credit to you, unless you prefer otherwise.

## Supported versions

Only the latest release gets security fixes.

## What counts

lakefreight moves sensitive data, so these are in scope:

- A table or column reaching a less protected zone than its flags require, for example personal data written to the `INTERNAL` zone, or clear restricted values written anywhere but `RESTRICTED`
- Rows silently dropped, duplicated or changed without reconciliation catching it
- Files from a failed or partial attempt being picked up by `lakefreight register`
- An IAM, KMS or Snowflake grant in `infra/terraform` that is broader than documented
- Credentials or keys reaching logs, Terraform state, manifests or generated SQL
- SQL injection through metadata such as table, column or tag names

Problems in Snowflake, AWS or the libraries lakefreight depends on should go to their maintainers.
