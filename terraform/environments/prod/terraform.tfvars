aws_region          = "us-east-1"
environment         = "prod"
app_name            = "ecfr"
vpc_cidr            = "10.0.0.0/16"
availability_zones  = ["us-east-1a", "us-east-1b"]
public_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
eb_instance_type    = "t3.medium"
eb_min_instances    = 1
eb_max_instances    = 4
fetcher_cpu         = 2048
fetcher_memory      = 4096
fetcher_schedule    = "cron(0 6 ? * SUN *)"  # Weekly on Sunday at 6 AM UTC

tags = {
  Project = "eCFR"
  Owner   = "cfr-team"
}
