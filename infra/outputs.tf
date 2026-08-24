output "layers" {
  description = "Provisioned medallion + warehouse buckets"
  value       = [for b in minio_s3_bucket.layer : b.bucket]
}

output "warehouse_uri" {
  description = "Iceberg warehouse root the catalog writes to"
  value       = "s3://warehouse/"
}

# -------------------------------------------------------------------------------------------------
# Glossary
#   output {}          Exposes a value after apply (shown in CLI, queryable by other tooling/modules).
#   value              The expression to export (a literal or a reference to a resource attribute).
#   for b in ... : ... A comprehension building a list from the for_each'd bucket resources.
#   s3://warehouse/    URI where Iceberg writes table data/metadata; matches Nessie's warehouse config.
#   Why outputs matter  They document the contract other components (Nessie, Trino, Spark) rely on.
# -------------------------------------------------------------------------------------------------
