variable "environment" {
  description = "Environment name to be used as dev, test, prod"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "oil-surveillance"
}

variable "alert_email" {
  description = "Email for SNS alert notifications"
  type        = string
}
