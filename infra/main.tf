# Tight IaC: the lakehouse's physical footprint as code.
# Local target = MinIO (S3 API). Flip `s3_endpoint`/creds to point at real AWS S3.
terraform {
  required_providers {
    minio = { source = "aminueza/minio", version = "~> 2.0" }
  }
}

provider "minio" {
  minio_server   = var.s3_endpoint
  minio_user     = var.s3_access_key
  minio_password = var.s3_secret_key
  minio_ssl      = false
}

# Medallion layers + Iceberg warehouse, one bucket each = clear cost/lifecycle boundaries.
resource "minio_s3_bucket" "layer" {
  for_each = toset(var.layers)
  bucket   = each.key
}

# Cost guardrail: expire cold raw data in bronze after N days.
# On real S3 this is a lifecycle transition to Glacier; here we expire to keep the demo tight.
resource "minio_ilm_policy" "bronze_tiering" {
  bucket = minio_s3_bucket.layer["bronze"].bucket
  rule {
    id         = "archive-cold-raw"
    expiration = "${var.cold_data_retention_days}d"
  }
}

# -------------------------------------------------------------------------------------------------
# Glossary
#   terraform {}              Root settings block; here it pins the required providers.
#   required_providers        Declares provider plugins + version constraints Terraform must download.
#     aminueza/minio          Community Terraform provider that manages MinIO (S3) resources.
#     ~> 2.0                  "Pessimistic" version constraint: >=2.0, <3.0.
#   provider "minio" {}       Configures how Terraform authenticates to the MinIO/S3 endpoint.
#   resource                  A managed piece of infrastructure Terraform creates/updates/destroys.
#   minio_s3_bucket           Resource type: an S3 bucket (maps to an AWS S3 bucket on real AWS).
#   for_each / toset(...)     Creates one resource instance per element of a set (bronze/silver/...).
#   each.key                  The current element inside a for_each loop.
#   minio_ilm_policy          Information Lifecycle Management = S3 lifecycle rules (expire/tier objects).
#   expiration                Delete objects after N days (on AWS this maps to expire or Glacier transition).
#   ${var.x}                  Interpolates an input variable defined in variables.tf.
#   IaC                       Infrastructure as Code; infra defined in versioned files, not clicks.
# -------------------------------------------------------------------------------------------------
