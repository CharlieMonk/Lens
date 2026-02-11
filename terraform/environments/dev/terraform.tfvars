aws_region          = "us-east-1"
environment         = "dev"
app_name            = "ecfr"
vpc_cidr            = "10.0.0.0/16"
availability_zones  = ["us-east-1a", "us-east-1b"]
public_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]

# Fetcher Lambda
fetcher_lambda_zip_path = "../../../lambda/fetcher.zip"
fetcher_max_concurrency = 25

# Consolidator (Fargate)
consolidator_cpu    = 2048
consolidator_memory = 8192

# Webapp (Fargate Spot)
webapp_cpu           = 512
webapp_memory        = 1024
webapp_desired_count = 1
webapp_min_count     = 1
webapp_max_count     = 4

# Schedule
schedule_expression = "cron(0 6 ? * SUN *)"
historical_years    = [2025, 2020, 2015, 2010, 2005, 2000]

tags = {
  Project     = "eCFR"
  Environment = "dev"
}
