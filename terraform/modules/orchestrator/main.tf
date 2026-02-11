# Step Functions State Machine
resource "aws_sfn_state_machine" "main" {
  name     = "${var.app_name}-${var.environment}-orchestrator"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "eCFR data fetching orchestrator"
    StartAt = "GenerateMatrix"
    States = {
      GenerateMatrix = {
        Type = "Pass"
        Result = {
          titles = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 36, 37, 38, 39,
                    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]
          years = var.historical_years
        }
        Next = "BuildItems"
      }
      BuildItems = {
        Type = "Pass"
        Parameters = {
          "items.$" = "States.Array(States.ArrayRange(1, 50, 1))"
        }
        ResultPath = "$.matrix"
        Next       = "FetchCurrentTitles"
      }
      FetchCurrentTitles = {
        Type = "Map"
        ItemsPath = "$.titles"
        MaxConcurrency = var.max_concurrency
        Parameters = {
          "title.$" = "$$.Map.Item.Value"
          "year"    = 0
        }
        Iterator = {
          StartAt = "FetchTitle"
          States = {
            FetchTitle = {
              Type     = "Task"
              Resource = var.fetcher_lambda_arn
              Retry = [{
                ErrorEquals     = ["States.TaskFailed", "Lambda.ServiceException"]
                IntervalSeconds = 30
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
        Type      = "Map"
        ItemsPath = "$.years"
        MaxConcurrency = 1
        Parameters = {
          "year.$"   = "$$.Map.Item.Value"
          "titles.$" = "$.titles"
        }
        Iterator = {
          StartAt = "FetchYearTitles"
          States = {
            FetchYearTitles = {
              Type      = "Map"
              ItemsPath = "$.titles"
              MaxConcurrency = var.max_concurrency
              Parameters = {
                "title.$" = "$$.Map.Item.Value"
                "year.$"  = "$.year"
              }
              Iterator = {
                StartAt = "FetchHistoricalTitle"
                States = {
                  FetchHistoricalTitle = {
                    Type     = "Task"
                    Resource = var.fetcher_lambda_arn
                    Retry = [{
                      ErrorEquals     = ["States.TaskFailed", "Lambda.ServiceException"]
                      IntervalSeconds = 30
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
