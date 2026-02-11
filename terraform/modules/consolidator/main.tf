# ECS Cluster (shared with webapp)
resource "aws_ecs_cluster" "main" {
  name = "${var.app_name}-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# ECR Repository for consolidator
resource "aws_ecr_repository" "consolidator" {
  name                 = "${var.app_name}-${var.environment}-consolidator"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "consolidator" {
  repository = aws_ecr_repository.consolidator.name

  policy = jsonencode({
    rules = [{
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
    }]
  })
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "consolidator" {
  name              = "/ecs/${var.app_name}-${var.environment}/consolidator"
  retention_in_days = 14
}

# IAM Role: Task Execution
resource "aws_iam_role" "execution" {
  name = "${var.app_name}-${var.environment}-consolidator-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM Role: Task Role
resource "aws_iam_role" "task" {
  name = "${var.app_name}-${var.environment}-consolidator-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "task" {
  name = "${var.app_name}-${var.environment}-consolidator-policy"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.s3_bucket_arn,
          "${var.s3_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:ClientRootAccess"
        ]
        Resource = var.efs_file_system_arn
      }
    ]
  })
}

# ECS Task Definition
resource "aws_ecs_task_definition" "consolidator" {
  family                   = "${var.app_name}-${var.environment}-consolidator"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name  = "consolidator"
    image = "${aws_ecr_repository.consolidator.repository_url}:latest"

    essential = true

    environment = [
      {
        name  = "S3_BUCKET"
        value = var.s3_bucket_name
      },
      {
        name  = "ECFR_DATABASE_PATH"
        value = "/data/ecfr.db"
      },
      {
        name  = "ECFR_OUTPUT_DIR"
        value = "/data"
      }
    ]

    mountPoints = [{
      sourceVolume  = "efs-data"
      containerPath = "/data"
      readOnly      = false
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consolidator.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "consolidator"
      }
    }
  }])

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
}
