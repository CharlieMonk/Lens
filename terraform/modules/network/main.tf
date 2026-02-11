# VPC
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.app_name}-${var.environment}-vpc"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.app_name}-${var.environment}-igw"
  }
}

# Public Subnets
resource "aws_subnet" "public" {
  count = length(var.public_subnet_cidrs)

  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.app_name}-${var.environment}-public-${count.index + 1}"
  }
}

# Route Table for Public Subnets
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Security Group: VPC Link (API Gateway)
resource "aws_security_group" "vpc_link" {
  name        = "${var.app_name}-${var.environment}-vpc-link-sg"
  description = "Security group for API Gateway VPC Link"
  vpc_id      = aws_vpc.main.id

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-vpc-link-sg"
  }
}

# Security Group: Web App (Fargate)
resource "aws_security_group" "webapp" {
  name        = "${var.app_name}-${var.environment}-webapp-sg"
  description = "Security group for Fargate web app"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "HTTP from VPC Link"
    from_port       = 5000
    to_port         = 5000
    protocol        = "tcp"
    security_groups = [aws_security_group.vpc_link.id]
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-webapp-sg"
  }
}

# Security Group: Consolidator (Fargate)
resource "aws_security_group" "consolidator" {
  name        = "${var.app_name}-${var.environment}-consolidator-sg"
  description = "Security group for Fargate consolidator task"
  vpc_id      = aws_vpc.main.id

  # No inbound rules - consolidator only makes outbound requests

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-consolidator-sg"
  }
}

# Security Group: EFS
resource "aws_security_group" "efs" {
  name        = "${var.app_name}-${var.environment}-efs-sg"
  description = "Security group for EFS filesystem"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "NFS from webapp"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.webapp.id]
  }

  ingress {
    description     = "NFS from consolidator"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.consolidator.id]
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.app_name}-${var.environment}-efs-sg"
  }
}
