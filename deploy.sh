#!/bin/bash
set -e

# Configuration
KEY_PATH="$HOME/.ssh/cfr-viewer-key.pem"
TARGET_GROUP_ARN="arn:aws:elasticloadbalancing:us-east-1:852981122009:targetgroup/cfr-viewer-tg/15ce19f429f52aa5"
AMI_ID="ami-0532be01f26a3de55"
INSTANCE_TYPE="m7i-flex.large"
SUBNET_ID="subnet-0d4f8441759e911e0"
SECURITY_GROUP_ID="sg-06ff14c0ab9af5462"
KEY_NAME="cfr-viewer-key"

echo "=== Deploying CFR Viewer ==="

# Get current instances in target group
echo "Finding instances in target group..."
OLD_INSTANCES=$(aws elbv2 describe-target-health --target-group-arn "$TARGET_GROUP_ARN" \
    --query 'TargetHealthDescriptions[*].Target.Id' --output text)

# Terminate old instances and delete their EBS volumes
if [ -n "$OLD_INSTANCES" ]; then
    echo "Getting EBS volumes for old instances..."
    OLD_VOLUMES=$(aws ec2 describe-instances --instance-ids $OLD_INSTANCES \
        --query 'Reservations[*].Instances[*].BlockDeviceMappings[*].Ebs.VolumeId' --output text)

    echo "Terminating old instances: $OLD_INSTANCES"
    aws ec2 terminate-instances --instance-ids $OLD_INSTANCES > /dev/null

    if [ -n "$OLD_VOLUMES" ]; then
        echo "Waiting for instances to terminate..."
        aws ec2 wait instance-terminated --instance-ids $OLD_INSTANCES

        echo "Deleting EBS volumes: $OLD_VOLUMES"
        for vol in $OLD_VOLUMES; do
            aws ec2 delete-volume --volume-id "$vol" 2>/dev/null || echo "  Volume $vol already deleted or in use"
        done
    fi
fi

# Launch new instance
echo "Launching new $INSTANCE_TYPE instance..."
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP_ID" \
    --subnet-id "$SUBNET_ID" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=cfr-viewer}]" \
    --query 'Instances[0].InstanceId' --output text)
echo "  Instance ID: $INSTANCE_ID"

# Wait for instance to be running
echo "Waiting for instance to be running..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"

# Get public IP
EC2_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "  Public IP: $EC2_IP"

# Register with target group
echo "Registering instance with target group..."
aws elbv2 register-targets --target-group-arn "$TARGET_GROUP_ARN" \
    --targets "Id=$INSTANCE_ID,Port=5000"

# Wait for SSH to be available
echo "Waiting for SSH to be ready..."
for i in {1..30}; do
    if ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$KEY_PATH" ec2-user@$EC2_IP "echo 'SSH ready'" 2>/dev/null; then
        break
    fi
    echo "  Attempt $i/30..."
    sleep 10
done

# Create tarball using git archive (respects .gitignore)
echo "Creating deployment package..."
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
git archive --format=tar.gz --prefix=cfr-viewer/ HEAD > /tmp/cfr-viewer.tar.gz
echo "  Package size: $(du -h /tmp/cfr-viewer.tar.gz | cut -f1)"

# Upload to EC2
echo "Uploading to EC2..."
scp -i "$KEY_PATH" -o StrictHostKeyChecking=no /tmp/cfr-viewer.tar.gz ec2-user@$EC2_IP:/tmp/

# Extract and set up on EC2
echo "Setting up on EC2..."
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no ec2-user@$EC2_IP << 'REMOTE'
set -e

# Install Python 3.11
sudo dnf install -y python3.11 python3.11-pip

sudo mkdir -p /opt/cfr-viewer
cd /opt
sudo rm -rf cfr-viewer
sudo tar -xzf /tmp/cfr-viewer.tar.gz
sudo chown -R ec2-user:ec2-user /opt/cfr-viewer
cd /opt/cfr-viewer

# Set up virtual environment and install dependencies
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -e .
pip install gunicorn

# Install systemd service
sudo tee /etc/systemd/system/cfr-viewer.service > /dev/null << 'SERVICE'
[Unit]
Description=CFR Viewer Flask Application
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/opt
Environment="PATH=/opt/cfr-viewer/venv/bin"
ExecStart=/opt/cfr-viewer/venv/bin/gunicorn --workers 2 --bind 0.0.0.0:5000 'cfr_viewer:create_app()'
Restart=always

[Install]
WantedBy=multi-user.target
SERVICE

# Run the fetcher to populate the database
echo "Running fetcher to populate database..."
python -m ecfr.fetcher

# Start the service
sudo systemctl daemon-reload
sudo systemctl enable cfr-viewer
sudo systemctl restart cfr-viewer
sudo systemctl status cfr-viewer --no-pager
REMOTE

echo ""
echo "=== Deployment Complete ==="
echo "Instance ID: $INSTANCE_ID"
echo "Direct EC2 URL: http://$EC2_IP:5000"
