# R14 (ADR-002): serverless batch legs.
# S3 landing events -> EventBridge -> SQS -> Lambda(validate|normalize|flag)
# -> Iceberg/Glue silver -> Athena; SNS alerts.
# Apply: terraform init && terraform plan   (requires AWS credentials)

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  default = "us-east-1"
}
variable "alert_email" {
  description = "Email subscribed to the alert topic"
  type        = string
}

resource "aws_s3_bucket" "lake" {
  bucket_prefix = "manifest-lake-"
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_notification" "landing" {
  bucket      = aws_s3_bucket.lake.id
  eventbridge = true
}

resource "aws_sns_topic" "alerts" {
  name = "manifest-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sqs_queue" "dlq" {
  name = "manifest-ingest-dlq"
}

resource "aws_sqs_queue" "ingest" {
  name                       = "manifest-ingest"
  visibility_timeout_seconds = 180
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_cloudwatch_event_rule" "landing_objects" {
  name = "manifest-landing-objects"
  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.lake.id] }
      object = { key = [{ prefix = "landing/" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "to_sqs" {
  rule = aws_cloudwatch_event_rule.landing_objects.name
  arn  = aws_sqs_queue.ingest.arn
}

resource "aws_sqs_queue_policy" "allow_eventbridge" {
  queue_url = aws_sqs_queue.ingest.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.ingest.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.landing_objects.arn } }
    }]
  })
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/build/manifest_lambdas.zip"
  source_dir  = "${path.module}/../.."
  excludes = [
    ".git", ".venv", ".venv-dbt", "data", "docs", "dbt", "deploy",
    "infra", "spark", "tests", "dags", "snowflake", "Makefile",
  ]
}

resource "aws_iam_role" "lambda" {
  name = "manifest-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.lake.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = aws_sqs_queue.ingest.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.alerts.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      },
    ]
  })
}

locals {
  handlers = {
    validate  = "lambdas.handler.validate"
    normalize = "lambdas.handler.normalize"
    flag      = "lambdas.handler.flag"
  }
}

resource "aws_lambda_function" "fn" {
  for_each         = local.handlers
  function_name    = "manifest-${each.key}"
  role             = aws_iam_role.lambda.arn
  handler          = each.value
  runtime          = "python3.12"
  timeout          = 120
  memory_size      = 512
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  environment {
    variables = {
      LAKE_BUCKET     = aws_s3_bucket.lake.id
      ALERT_TOPIC_ARN = aws_sns_topic.alerts.arn
    }
  }
}

resource "aws_lambda_event_source_mapping" "validate_from_sqs" {
  event_source_arn = aws_sqs_queue.ingest.arn
  function_name    = aws_lambda_function.fn["validate"].arn
  batch_size       = 1
}

# bronze/ objects -> normalize; silver/events/ objects -> flag (direct S3->Lambda)
resource "aws_lambda_permission" "s3_invoke" {
  for_each      = toset(["normalize", "flag"])
  statement_id  = "AllowS3-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fn[each.key].function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lake.arn
}

# Glue database over the silver prefix; Iceberg tables created by Athena DDL.
resource "aws_glue_catalog_database" "silver" {
  name = "manifest_silver"
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}
output "alert_topic" {
  value = aws_sns_topic.alerts.arn
}
