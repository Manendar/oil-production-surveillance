terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "oil-surveillance-tf-state"
    key            = "prod/terraform.tfstate"
    dynamodb_table = "oil-surveillance-tf-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "oil-production-surveillance"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
