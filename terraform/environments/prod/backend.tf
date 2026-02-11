terraform {
  required_version = ">= 1.0"

  # Uncomment and configure to use S3 backend for state storage
  # backend "s3" {
  #   bucket         = "ecfr-terraform-state"
  #   key            = "prod/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "ecfr-terraform-locks"
  # }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(var.tags, {
      Application = var.app_name
      Environment = var.environment
      ManagedBy   = "terraform"
    })
  }
}
