data "aws_caller_identity" "current" {}

# ECR Repository
resource "aws_ecr_repository" "fetcher" {
  name                 = "${var.app_name}-${var.environment}-fetcher"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-fetcher"
  }
}

resource "aws_ecr_lifecycle_policy" "fetcher" {
  repository = aws_ecr_repository.fetcher.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# ECS Cluster
resource "aws_ecs_cluster" "fetcher" {
  name = "${var.app_name}-${var.environment}-fetcher"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-fetcher"
  }
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "fetcher" {
  name              = "/ecs/${var.app_name}-${var.environment}-fetcher"
  retention_in_days = 30

  tags = {
    Name = "${var.app_name}-${var.environment}-fetcher"
  }
}

# IAM Role: Task Execution Role
resource "aws_iam_role" "task_execution" {
  name = "${var.app_name}-${var.environment}-fetcher-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.app_name}-${var.environment}-fetcher-execution"
  }
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM Role: Task Role (for accessing EFS)
resource "aws_iam_role" "task" {
  name = "${var.app_name}-${var.environment}-fetcher-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.app_name}-${var.environment}-fetcher-task"
  }
}

resource "aws_iam_role_policy" "task_efs" {
  name = "${var.app_name}-${var.environment}-fetcher-efs"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:ClientRootAccess"
        ]
        Resource = "arn:aws:elasticfilesystem:${var.aws_region}:${data.aws_caller_identity.current.account_id}:file-system/${var.efs_file_system_id}"
      }
    ]
  })
}

# ECS Task Definition
resource "aws_ecs_task_definition" "fetcher" {
  family                   = "${var.app_name}-${var.environment}-fetcher"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.fetcher_cpu
  memory                   = var.fetcher_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name  = "fetcher"
      image = "${aws_ecr_repository.fetcher.repository_url}:latest"

      essential = true

      environment = [
        {
          name  = "ECFR_DATABASE_PATH"
          value = "/data/ecfr.db"
        },
        {
          name  = "ECFR_OUTPUT_DIR"
          value = "/data"
        }
      ]

      mountPoints = [
        {
          sourceVolume  = "efs-data"
          containerPath = "/data"
          readOnly      = false
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.fetcher.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  volume {
    name = "efs-data"

    efs_volume_configuration {
      file_system_id     = var.efs_file_system_id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = var.efs_access_point_id
        iam             = "ENABLED"
      }
    }
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-fetcher"
  }
}

# IAM Role: EventBridge to ECS
resource "aws_iam_role" "eventbridge_ecs" {
  name = "${var.app_name}-${var.environment}-eventbridge-ecs"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.app_name}-${var.environment}-eventbridge-ecs"
  }
}

resource "aws_iam_role_policy" "eventbridge_ecs" {
  name = "${var.app_name}-${var.environment}-eventbridge-ecs"
  role = aws_iam_role.eventbridge_ecs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = aws_ecs_task_definition.fetcher.arn
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = [
          aws_iam_role.task_execution.arn,
          aws_iam_role.task.arn
        ]
      }
    ]
  })
}

# EventBridge Rule: Weekly Schedule
resource "aws_cloudwatch_event_rule" "fetcher" {
  name                = "${var.app_name}-${var.environment}-fetcher-weekly"
  description         = "Run eCFR fetcher weekly"
  schedule_expression = var.fetcher_schedule

  tags = {
    Name = "${var.app_name}-${var.environment}-fetcher-weekly"
  }
}

# EventBridge Target: ECS Task
resource "aws_cloudwatch_event_target" "fetcher" {
  rule      = aws_cloudwatch_event_rule.fetcher.name
  target_id = "${var.app_name}-${var.environment}-fetcher"
  arn       = aws_ecs_cluster.fetcher.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.fetcher.arn
    launch_type         = "FARGATE"
    platform_version    = "LATEST"

    network_configuration {
      subnets          = var.subnet_ids
      security_groups  = [var.fetcher_security_group]
      assign_public_ip = true
    }
  }
}
