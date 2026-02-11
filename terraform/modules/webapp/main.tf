# ECR Repository for webapp
resource "aws_ecr_repository" "webapp" {
  name                 = "${var.app_name}-${var.environment}-webapp"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "webapp" {
  repository = aws_ecr_repository.webapp.name

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
resource "aws_cloudwatch_log_group" "webapp" {
  name              = "/ecs/${var.app_name}-${var.environment}/webapp"
  retention_in_days = 14
}

# IAM Role: Task Execution
resource "aws_iam_role" "execution" {
  name = "${var.app_name}-${var.environment}-webapp-execution"

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
  name = "${var.app_name}-${var.environment}-webapp-task"

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
  name = "${var.app_name}-${var.environment}-webapp-policy"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "elasticfilesystem:ClientMount",
        "elasticfilesystem:ClientRead"
      ]
      Resource = var.efs_file_system_arn
    }]
  })
}

# ECS Task Definition
resource "aws_ecs_task_definition" "webapp" {
  family                   = "${var.app_name}-${var.environment}-webapp"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name  = "webapp"
    image = "${aws_ecr_repository.webapp.repository_url}:latest"

    essential = true

    portMappings = [{
      containerPort = 5000
      protocol      = "tcp"
    }]

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

    mountPoints = [{
      sourceVolume  = "efs-data"
      containerPath = "/data"
      readOnly      = true
    }]

    healthCheck = {
      command     = ["CMD-SHELL", "curl -f http://localhost:5000/health || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 120
    }

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.webapp.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "webapp"
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

# Cloud Map Namespace
resource "aws_service_discovery_private_dns_namespace" "main" {
  name = "${var.app_name}-${var.environment}.local"
  vpc  = var.vpc_id
}

# Cloud Map Service
resource "aws_service_discovery_service" "webapp" {
  name = "webapp"

  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.main.id
    routing_policy = "MULTIVALUE"

    dns_records {
      type = "A"
      ttl  = 10
    }

    dns_records {
      type = "SRV"
      ttl  = 10
    }
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

# ECS Service
resource "aws_ecs_service" "webapp" {
  name            = "webapp"
  cluster         = var.ecs_cluster_arn
  task_definition = aws_ecs_task_definition.webapp.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [var.webapp_security_group]
    assign_public_ip = true
  }

  service_registries {
    registry_arn   = aws_service_discovery_service.webapp.arn
    container_name = "webapp"
    container_port = 5000
  }

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 100
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}

# Auto Scaling
resource "aws_appautoscaling_target" "webapp" {
  max_capacity       = var.max_count
  min_capacity       = var.min_count
  resource_id        = "service/${var.ecs_cluster_name}/${aws_ecs_service.webapp.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "webapp_cpu" {
  name               = "${var.app_name}-${var.environment}-webapp-cpu"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.webapp.resource_id
  scalable_dimension = aws_appautoscaling_target.webapp.scalable_dimension
  service_namespace  = aws_appautoscaling_target.webapp.service_namespace

  target_tracking_scaling_policy_configuration {
    target_value       = 70
    scale_in_cooldown  = 300
    scale_out_cooldown = 60

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}

# API Gateway HTTP API
resource "aws_apigatewayv2_api" "main" {
  name          = "${var.app_name}-${var.environment}"
  protocol_type = "HTTP"
}

# VPC Link
resource "aws_apigatewayv2_vpc_link" "main" {
  name               = "${var.app_name}-${var.environment}"
  subnet_ids         = var.subnet_ids
  security_group_ids = [var.vpc_link_security_group]
}

# API Gateway Integration
resource "aws_apigatewayv2_integration" "webapp" {
  api_id             = aws_apigatewayv2_api.main.id
  integration_type   = "HTTP_PROXY"
  integration_method = "ANY"
  integration_uri    = aws_service_discovery_service.webapp.arn
  connection_type    = "VPC_LINK"
  connection_id      = aws_apigatewayv2_vpc_link.main.id
}

# API Gateway Route
resource "aws_apigatewayv2_route" "default" {
  api_id    = aws_apigatewayv2_api.main.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.webapp.id}"
}

# API Gateway Stage
resource "aws_apigatewayv2_stage" "main" {
  api_id      = aws_apigatewayv2_api.main.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gateway.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      ip             = "$context.identity.sourceIp"
      requestTime    = "$context.requestTime"
      httpMethod     = "$context.httpMethod"
      path           = "$context.path"
      status         = "$context.status"
      responseLength = "$context.responseLength"
      latency        = "$context.integrationLatency"
    })
  }
}

# CloudWatch Log Group for API Gateway
resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/${var.app_name}-${var.environment}"
  retention_in_days = 14
}
