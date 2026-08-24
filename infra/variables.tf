variable "s3_endpoint" {
  description = "S3 endpoint (MinIO locally; e.g. s3.amazonaws.com on AWS)"
  type        = string
  default     = "localhost:9000"
}

variable "s3_access_key" {
  type    = string
  default = "minio"
}

variable "s3_secret_key" {
  type      = string
  sensitive = true
  default   = "minio123"
}

variable "layers" {
  description = "Medallion buckets + the Iceberg warehouse"
  type        = list(string)
  default     = ["bronze", "silver", "gold", "warehouse"]
}

variable "cold_data_retention_days" {
  description = "Cost guardrail: archive/expire cold raw bronze data after N days"
  type        = number
  default     = 30
}

# -------------------------------------------------------------------------------------------------
# Glossary
#   variable {}    Declares an input parameter; override via -var, *.tfvars, or TF_VAR_ env vars.
#   description    Human-readable docs shown in plans and `terraform console`.
#   type           Value constraint: string / number / bool / list(...) / map(...) / object(...).
#   default        Value used when the caller doesn't supply one (makes the var optional).
#   sensitive      Marks a value (e.g. secret key) so Terraform redacts it from CLI/log output.
#   list(string)   An ordered collection of strings; here the medallion + warehouse bucket names.
#   s3_endpoint    Swap point: localhost:9000 (MinIO) vs. s3.amazonaws.com (real AWS S3).
# -------------------------------------------------------------------------------------------------
