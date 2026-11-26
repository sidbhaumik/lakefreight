# --- Snowflake credentials for the Lambda functions -----------------------------
# The secret is created empty. Put the value in yourself, so the private key never
# enters Terraform state:
#   {"account": "<org>-<account>", "user": "<service user>", "private_key": "<PEM>"}

resource "aws_secretsmanager_secret" "snowflake" {
  name        = "${var.name}/snowflake"
  description = "Key-pair credentials for the ${var.name} Snowflake service user"
}

# --- Lambda functions --------------------------------------------------------

locals {
  functions = {
    list_items = { timeout = 300, memory = 512 }
    submit     = { timeout = 120, memory = 512 }
    poll       = { timeout = 60, memory = 256 }
    reconcile  = { timeout = 900, memory = 512 }
    summarize  = { timeout = 120, memory = 256 }
  }
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda" {
  statement {
    sid       = "SnowflakeSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.snowflake.arn]
  }

  statement {
    sid       = "RunLists"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${local.control_arn}/runs/*"]
  }

  statement {
    sid     = "ManifestsAndSchemas"
    actions = ["s3:PutObject"]
    resources = [
      for z, v in local.zones : "${local.bucket_arn[v.bucket]}/${v.prefix}/*"
    ]
  }

  statement {
    sid       = "ZoneKeys"
    actions   = ["kms:GenerateDataKey", "kms:Encrypt", "kms:DescribeKey"]
    resources = [for k in aws_kms_key.zone : k.arn]
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy" "lambda" {
  name   = "pipeline"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "lambda" {
  for_each = local.functions

  name              = "/aws/lambda/${var.name}-${replace(each.key, "_", "-")}"
  retention_in_days = 30
}

resource "aws_lambda_function" "handler" {
  for_each = local.functions

  function_name    = "${var.name}-${replace(each.key, "_", "-")}"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.13"
  architectures    = [var.lambda_architecture]
  handler          = "lakefreight.handlers.${each.key}"
  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)
  timeout          = each.value.timeout
  memory_size      = each.value.memory

  environment {
    variables = {
      LAKEFREIGHT_CONFIG_TOML = local.lakefreight_toml
      ITEMS_BUCKET            = aws_s3_bucket.control.id
      SNOWFLAKE_SECRET_ARN    = aws_secretsmanager_secret.snowflake.arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy_attachment.lambda_logs]
}

# --- Step Functions --------------------------------------------------------------

locals {
  state_machine_name = "${var.name}-extract-lane"
  state_machine_arn  = "arn:${local.partition}:states:${var.aws_region}:${local.account_id}:stateMachine:${local.state_machine_name}"
}

data "aws_iam_policy_document" "sfn_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "sfn" {
  statement {
    sid     = "InvokeHandlers"
    actions = ["lambda:InvokeFunction"]
    resources = flatten([
      for f in aws_lambda_function.handler : [f.arn, "${f.arn}:*"]
    ])
  }

  statement {
    sid       = "ReadRunLists"
    actions   = ["s3:GetObject"]
    resources = ["${local.control_arn}/runs/*"]
  }

  # A Distributed Map runs each item as a child execution of this state machine.
  statement {
    sid       = "ChildExecutions"
    actions   = ["states:StartExecution"]
    resources = [local.state_machine_arn]
  }

  statement {
    sid     = "ManageChildExecutions"
    actions = ["states:DescribeExecution", "states:StopExecution"]
    resources = [
      "arn:${local.partition}:states:${var.aws_region}:${local.account_id}:execution:${local.state_machine_name}/*",
      "arn:${local.partition}:states:${var.aws_region}:${local.account_id}:execution:${local.state_machine_name}:*",
    ]
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.name}-step-functions"
  assume_role_policy = data.aws_iam_policy_document.sfn_trust.json
}

resource "aws_iam_role_policy" "sfn" {
  name   = "extract-lane"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_sfn_state_machine" "extract_lane" {
  name     = local.state_machine_name
  role_arn = aws_iam_role.sfn.arn
  definition = templatefile("${path.module}/../../stepfunctions/extract_lane.asl.json", {
    ListItemsFunctionArn = aws_lambda_function.handler["list_items"].arn
    SubmitFunctionArn    = aws_lambda_function.handler["submit"].arn
    PollFunctionArn      = aws_lambda_function.handler["poll"].arn
    ReconcileFunctionArn = aws_lambda_function.handler["reconcile"].arn
    SummarizeFunctionArn = aws_lambda_function.handler["summarize"].arn
  })
}
