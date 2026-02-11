# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "fetcher" {
  name              = "/aws/lambda/${var.app_name}-${var.environment}-fetcher"
  retention_in_days = 14
}

# IAM Role for Lambda
resource "aws_iam_role" "fetcher" {
  name = "${var.app_name}-${var.environment}-fetcher-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "fetcher" {
  name = "${var.app_name}-${var.environment}-fetcher-policy"
  role = aws_iam_role.fetcher.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.fetcher.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "${var.s3_bucket_arn}/*"
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "fetcher" {
  function_name = "${var.app_name}-${var.environment}-fetcher"
  role          = aws_iam_role.fetcher.arn
  handler       = "lambda_fetcher.handler"
  runtime       = "python3.11"
  timeout       = 600
  memory_size   = 1024

  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)

  environment {
    variables = {
      S3_BUCKET = var.s3_bucket_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.fetcher]
}
