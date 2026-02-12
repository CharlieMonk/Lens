# Step Functions State Machine
# Optimized for Lambda concurrency limit of 10
# - Titles ordered: large first (40,26,42,45,48), then medium, then small
# - MaxConcurrency=10 to match Lambda limit and avoid throttling
# - Worker pool pattern: as each title completes, next one starts
resource "aws_sfn_state_machine" "main" {
  name     = "${var.app_name}-${var.environment}-orchestrator"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "eCFR data fetching orchestrator - optimized for Lambda concurrency"
    StartAt = "GenerateMatrix"
    States = {
      GenerateMatrix = {
        Type = "Pass"
        Result = {
          # Titles ordered by expected duration: large first, then medium, then small
          # Large (slowest): 40, 26, 42, 45, 48
          # Medium: 7, 12, 14, 17, 21, 29, 49
          # Small: remaining titles
          # Reserved (35) excluded - no content
          titles = [
            40, 26, 42, 45, 48,           # Large titles first (slowest)
            7, 12, 14, 17, 21, 29, 49,    # Medium titles
            1, 2, 3, 4, 5, 6, 8, 9, 10,   # Small titles
            11, 13, 15, 16, 18, 19, 20,
            22, 23, 24, 25, 27, 28,
            30, 31, 32, 33, 34, 36, 37, 38, 39,
            41, 43, 44, 46, 47, 50
          ]
          years = var.historical_years
        }
        Next = "FetchCurrentTitles"
      }
      FetchCurrentTitles = {
        Type       = "Map"
        ItemsPath  = "$.titles"
        MaxConcurrency = var.max_concurrency
        ToleratedFailurePercentage = 10
        Parameters = {
          "title.$" = "$$.Map.Item.Value"
          "year"    = 0
        }
        ItemProcessor = {
          ProcessorConfig = {
            Mode = "INLINE"
          }
          StartAt = "FetchTitle"
          States = {
            FetchTitle = {
              Type     = "Task"
              Resource = var.fetcher_lambda_arn
              Retry = [{
                ErrorEquals     = ["States.TaskFailed", "Lambda.ServiceException", "Lambda.TooManyRequestsException"]
                IntervalSeconds = 10
                MaxAttempts     = 3
                BackoffRate     = 2
              }]
              End = true
            }
          }
        }
        ResultPath = "$.currentResults"
        Next       = "FetchHistoricalYears"
      }
      FetchHistoricalYears = {
        Type       = "Map"
        ItemsPath  = "$.years"
        MaxConcurrency = 1  # Process one year at a time to avoid overwhelming resources
        Parameters = {
          "year.$"   = "$$.Map.Item.Value"
          "titles.$" = "$.titles"
        }
        ItemProcessor = {
          ProcessorConfig = {
            Mode = "INLINE"
          }
          StartAt = "FetchYearTitles"
          States = {
            FetchYearTitles = {
              Type       = "Map"
              ItemsPath  = "$.titles"
              MaxConcurrency = var.max_concurrency
              ToleratedFailurePercentage = 10
              Parameters = {
                "title.$" = "$$.Map.Item.Value"
                "year.$"  = "$.year"
              }
              ItemProcessor = {
                ProcessorConfig = {
                  Mode = "INLINE"
                }
                StartAt = "FetchHistoricalTitle"
                States = {
                  FetchHistoricalTitle = {
                    Type     = "Task"
                    Resource = var.fetcher_lambda_arn
                    Retry = [{
                      ErrorEquals     = ["States.TaskFailed", "Lambda.ServiceException", "Lambda.TooManyRequestsException"]
                      IntervalSeconds = 10
                      MaxAttempts     = 3
                      BackoffRate     = 2
                    }]
                    End = true
                  }
                }
              }
              End = true
            }
          }
        }
        ResultPath = "$.historicalResults"
        Next       = "RunConsolidator"
      }
      RunConsolidator = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = {
          Cluster        = var.ecs_cluster_arn
          TaskDefinition = var.consolidator_task_arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = var.subnet_ids
              SecurityGroups = [var.consolidator_security_group]
              AssignPublicIp = "ENABLED"
            }
          }
        }
        End = true
      }
    }
  })
}

# IAM Role for Step Functions
resource "aws_iam_role" "step_functions" {
  name = "${var.app_name}-${var.environment}-step-functions"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${var.app_name}-${var.environment}-step-functions-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = var.fetcher_lambda_arn
      },
      {
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "*"
        Condition = {
          StringLike = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

# EventBridge Rule for weekly schedule
resource "aws_cloudwatch_event_rule" "weekly" {
  name                = "${var.app_name}-${var.environment}-weekly"
  description         = "Trigger eCFR data fetch weekly"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "step_functions" {
  rule      = aws_cloudwatch_event_rule.weekly.name
  target_id = "step-functions"
  arn       = aws_sfn_state_machine.main.arn
  role_arn  = aws_iam_role.eventbridge.arn
}

# IAM Role for EventBridge
resource "aws_iam_role" "eventbridge" {
  name = "${var.app_name}-${var.environment}-eventbridge"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "events.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge" {
  name = "${var.app_name}-${var.environment}-eventbridge-policy"
  role = aws_iam_role.eventbridge.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.main.arn
    }]
  })
}

# CloudWatch Log Group for Step Functions
resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/states/${var.app_name}-${var.environment}-orchestrator"
  retention_in_days = 14
}
