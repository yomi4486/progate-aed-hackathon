#!/bin/bash
set -o xtrace

# Bootstrap script for EKS nodes with optimizations for crawler workloads

# Install SSM agent for better debugging capabilities
yum install -y amazon-ssm-agent
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# Optimize system for crawler workloads
# Increase file descriptor limits
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf

# Increase network buffer sizes for better HTTP performance
echo "net.core.rmem_max = 16777216" >> /etc/sysctl.conf
echo "net.core.wmem_max = 16777216" >> /etc/sysctl.conf
echo "net.ipv4.tcp_rmem = 4096 32768 16777216" >> /etc/sysctl.conf
echo "net.ipv4.tcp_wmem = 4096 32768 16777216" >> /etc/sysctl.conf
echo "net.ipv4.tcp_congestion_control = bbr" >> /etc/sysctl.conf

# Apply sysctl settings
sysctl -p

# Bootstrap the node to join the cluster
/etc/eks/bootstrap.sh ${cluster_name} \
  --apiserver-endpoint ${endpoint} \
  --b64-cluster-ca ${ca_data} \
  --container-runtime containerd \
  --kubelet-extra-args '--max-pods=110 --node-labels=workload-type=crawler'

# Install additional monitoring tools
yum install -y htop iotop

# Install Docker CLI for debugging (containerd is the runtime)
yum install -y docker
usermod -aG docker ec2-user

# Create directories for persistent volumes
mkdir -p /mnt/crawler-data
chmod 755 /mnt/crawler-data

# Enable CloudWatch agent for enhanced monitoring
yum install -y amazon-cloudwatch-agent

# Create CloudWatch agent config for container insights
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'EOL'
{
  "agent": {
    "metrics_collection_interval": 60,
    "run_as_user": "cwagent"
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/messages",
            "log_group_name": "/aws/eks/${cluster_name}/system",
            "log_stream_name": "{instance_id}/messages"
          },
          {
            "file_path": "/var/log/secure",
            "log_group_name": "/aws/eks/${cluster_name}/system",
            "log_stream_name": "{instance_id}/secure"
          }
        ]
      }
    }
  },
  "metrics": {
    "namespace": "CWAgent",
    "metrics_collected": {
      "cpu": {
        "measurement": ["cpu_usage_idle", "cpu_usage_iowait", "cpu_usage_user", "cpu_usage_system"],
        "metrics_collection_interval": 60
      },
      "disk": {
        "measurement": ["used_percent"],
        "metrics_collection_interval": 60,
        "resources": ["*"]
      },
      "mem": {
        "measurement": ["mem_used_percent"],
        "metrics_collection_interval": 60
      },
      "netstat": {
        "measurement": ["tcp_established", "tcp_time_wait"],
        "metrics_collection_interval": 60
      }
    }
  }
}
EOL

# Start CloudWatch agent
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# Signal that the instance is ready
/opt/aws/bin/cfn-signal -e $? --stack ${cluster_name} --resource NodeGroup --region $(curl -s http://169.254.169.254/latest/meta-data/placement/region) || true
