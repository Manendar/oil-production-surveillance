variable "environment" {
  type    = string
  default = "test"
}

variable "aws_region" {
  type    = string
  default = "eu-west-2"
}

variable "alert_email" {
  description = "Email for SNS alert notifications"
  type        = string
}
