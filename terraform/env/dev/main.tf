module "oil_surveillance" {
  source      = "../../module/oil-surveillance"
  environment = var.environment
  aws_region  = var.aws_region
  alert_email = var.alert_email
}
