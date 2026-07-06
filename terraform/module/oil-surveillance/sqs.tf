resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-dlq-${var.environment}"
  message_retention_seconds = 1209600

  tags = {
    Name        = "${var.project_name}-dlq-${var.environment}"
    Environment = var.environment
  }
}
