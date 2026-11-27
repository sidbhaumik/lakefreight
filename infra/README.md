# Deploying the lakefreight pipeline

`infra/terraform` creates everything the Step Functions pipeline needs, on AWS and in Snowflake, from one configuration. It works with Terraform 1.6+ or OpenTofu.

**Status:** applied to a live AWS account and Snowflake trial and run end to end: Snowflake unloaded every table into S3 through the zone roles, each file encrypted with its zone's key, and the tables were registered and read back from S3. Cross-account replication is untested. Deploy to a test account first; see `docs/live-validation.md`.

## What it creates

| Where | Resource | Purpose |
|---|---|---|
| AWS | Two egress buckets: one for the `INTERNAL` and `PII` zones, one for `RESTRICTED` | Where Snowflake writes the Parquet files. Versioned, TLS-only, public access blocked, old versions expire. |
| AWS | One KMS key per zone | Each zone's files are encrypted with its own key. |
| AWS | One IAM role per zone, assumed by Snowflake | Can write only its zone's prefix and use only its zone's key. |
| AWS | Secrets Manager secret | The service user's private key. Created empty; you fill it in. |
| AWS | Five Lambda functions and a Step Functions state machine | Run work items: list, submit, poll, reconcile, then summarize the lane. |
| AWS | Control bucket | Work-item lists for each run. |
| AWS | Optional replication role and rules | Copy each egress bucket to the receiving account. |
| Snowflake | Control database and schema | Control tables and stages. |
| Snowflake | One warehouse per lane | `LARGE`, `SMALL` and `RESTRICTED` lanes run independently. |
| Snowflake | One role, storage integration and stage per zone | A role can use only its own stage. The service user has secondary roles turned off, so each session holds only its zone's role. |
| Snowflake | Grants | Read access to the source databases; the token cross-reference table for `RESTRICTED` only. |
| Snowflake | Key-pair service user | Used by the Lambda functions. Detached queries keep running, so asynchronous `COPY` works. |

## Before you start

- Terraform 1.6+ or OpenTofu, Python 3.11+, and the AWS CLI.
- AWS credentials allowed to create S3, KMS, IAM, Lambda, Step Functions and Secrets Manager resources.
- A Snowflake role allowed to create databases, warehouses, roles, users and storage integrations. `ACCOUNTADMIN` works; a dedicated admin role is better.
- Deploy in the AWS region of your Snowflake account, so unloads cross no region.
- An AWS account that allows access from other AWS accounts. Snowflake writes to S3 by assuming the zone roles from Snowflake's own AWS account. Accounts on AWS's free plan sit in an organization whose AWS-managed policies block principals from outside it, so every unload fails with `Error assuming AWS_ROLE ... not authorized to perform: sts:AssumeRole`. Upgrade the account and activate advanced features, or use an account outside the free plan.

## Steps

### 1. Make a key pair for the service user

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out lakefreight_key.p8 -nocrypt
```

```bash
openssl rsa -in lakefreight_key.p8 -pubout -out lakefreight_key.pub
```

Keep `lakefreight_key.p8` out of version control. The public key goes into Terraform; the private key goes only into Secrets Manager (step 6).

### 2. Build the Lambda package

```bash
infra/build_lambda.sh x86_64
```

This writes `build/lakefreight-lambda.zip`, about 26 MB. Use `arm64` instead to run on Graviton, and set `lambda_architecture = "arm64"` to match.

### 3. Set the variables

```bash
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
```

Edit `terraform.tfvars`:

- `aws_region`: the region of your Snowflake account
- `source_databases`: the databases to migrate
- `service_user_public_key`: the body of `lakefreight_key.pub`, on one line, without the `BEGIN` and `END` lines
- `xref_table`: only if you detokenize restricted columns. Keep it outside the source databases; the plan fails if it isn't.
- `replication`: only if you replicate to a receiving account (step 9)
- `snapshot` and `snapshot_window_hours`: which tables are read as of one snapshot through time travel (default: those with `STILL_WRITTEN` set), and how long their extract may run
- `allow_timestamp_precision_loss`: leave `false` to fail chunks whose timestamps would lose digits below a millisecond; set `true` if milliseconds are enough

`variables.tf` lists every option, including warehouse sizes and concurrency per lane.

### 4. Point the Snowflake provider at your account

The provider reads the usual environment variables or `~/.snowflake/config`, for example:

```bash
export SNOWFLAKE_ORGANIZATION_NAME=myorg SNOWFLAKE_ACCOUNT_NAME=myaccount SNOWFLAKE_USER=admin_user SNOWFLAKE_ROLE=ACCOUNTADMIN SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT SNOWFLAKE_PRIVATE_KEY="$(cat $HOME/.snowflake/admin_key.p8)"
```

### 5. Apply

```bash
cd infra/terraform && terraform init && terraform apply
```

Replace `terraform` with `tofu` if you use OpenTofu.

### 6. Store the service user's credentials

The secret is created empty, so the private key never enters Terraform state. Fill it in:

```bash
python3 -c 'import json,sys; print(json.dumps({"account": sys.argv[1], "user": sys.argv[2], "private_key": open(sys.argv[3]).read()}))' myorg-myaccount LAKEFREIGHT_PIPELINE lakefreight_key.p8 > secret.json
```

```bash
aws secretsmanager put-secret-value --secret-id "$(terraform output -raw snowflake_secret_arn)" --secret-string file://secret.json && rm secret.json
```

### 7. Create the control tables and seed them

In Snowflake, as the admin role:

```sql
USE SCHEMA LAKEFREIGHT.CONTROL;
-- then run sql/ddl/01_control_tables.sql
```

Run this after `apply`, so the extract roles' grants on future tables cover the new tables.

Then load the metadata and seed the control tables:

```bash
lakefreight discover --database SALES --database HR --table LAKEFREIGHT.CONTROL.SOURCE_METADATA
```

Run `sql/ddl/02_seed_config.sql` in the same schema, with `FULL_THRESHOLD_BYTES` at its top set to the `full_threshold_bytes` in your settings, and review the chunk columns it picked.

### 8. Create work items and run

Save the settings that match this deployment, then create work items:

```bash
terraform output -raw lakefreight_toml > lakefreight.toml
```

```bash
SNOWFLAKE_ACCOUNT=myorg-myaccount SNOWFLAKE_USER=LAKEFREIGHT_PIPELINE SNOWFLAKE_PRIVATE_KEY_FILE=lakefreight_key.p8 PLANNER_ROLE=LAKEFREIGHT_INTERNAL_EXTRACT PLANNER_WAREHOUSE=LAKEFREIGHT_SMALL_WH python -m lakefreight.planning
```

Start one execution per lane:

```bash
aws stepfunctions start-execution --state-machine-arn "$(terraform output -raw state_machine_arn)" --input '{"lane": "SMALL"}'
```

Repeat with `LARGE` and `RESTRICTED`. Every item in the lane gets its attempt; the execution then fails with `LaneHadFailures` if any item did not reconcile. A rerun picks up only pending and failed work items. Track progress with the `EXTRACT_PROGRESS` view.

### 9. Optional: replicate to the receiving account

Set `replication` in `terraform.tfvars` and apply again. Then the receiving account must:

- allow the role in `terraform output replication_role_arn` to replicate into its bucket, in the bucket policy
- allow that role to use its KMS key for encryption, in the key policy

Replicas belong to the receiving account. Only objects written after replication is turned on are copied; use S3 Batch Replication for anything older.

## Tearing down

Empty the egress buckets first, since they are versioned, then:

```bash
terraform destroy
```

KMS keys wait 30 days before deletion.

## Testing changes to the Terraform

```bash
cd infra/terraform && tofu init -backend=false && tofu validate && tofu test
```

`tofu test` plans the whole module with a mocked Snowflake provider and an offline AWS provider. It checks, among other things, that each zone's Snowflake role is limited to its own prefix and key, that only the `RESTRICTED` role can read the cross-reference table, and that the settings given to the Lambda functions match the stages and buckets. CI runs the same checks, and also imports the handlers from the built package on the AWS Lambda Python runtime image.
