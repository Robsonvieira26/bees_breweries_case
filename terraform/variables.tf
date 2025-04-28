
variable "minio_user" {
  description = "MinIO access key"
  type        = string
  sensitive   = true
}

variable "minio_password" {
  description = "MinIO secret key"
  type        = string
  sensitive   = true
}

variable "buckets" {
  description = "List of buckets to create"
  type        = list(string)
  default     = ["raw-data", "processed-data", "backup"]
}