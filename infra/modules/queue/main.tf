variable "name_prefix" { type = string }

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name_prefix}-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "url_queue" {
  name                       = "${var.name_prefix}-url"
  visibility_timeout_seconds = 60
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "index_queue" {
  name                       = "${var.name_prefix}-index"
  visibility_timeout_seconds = 120
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "discovery_queue" {
  name                       = "${var.name_prefix}-discovery"
  visibility_timeout_seconds = 60
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

output "url_queue_url" { value = aws_sqs_queue.url_queue.id }
output "index_queue_url" { value = aws_sqs_queue.index_queue.id }
output "discovery_queue_url" { value = aws_sqs_queue.discovery_queue.id }
output "dlq_url" { value = aws_sqs_queue.dlq.id }

output "url_queue_arn" { value = aws_sqs_queue.url_queue.arn }
output "index_queue_arn" { value = aws_sqs_queue.index_queue.arn }
output "discovery_queue_arn" { value = aws_sqs_queue.discovery_queue.arn }
output "dlq_arn" { value = aws_sqs_queue.dlq.arn }
