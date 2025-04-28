terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "~> 1.15.3" 
    }
  }
}

provider "minio" {
  minio_server   = "minio:9000"  # Formato HOST:PORT
  minio_user     = var.minio_user  # Argumento correto
  minio_password = var.minio_password  # Argumento correto
  minio_ssl      = false
}

resource "minio_s3_bucket" "buckets" {
  for_each = toset(var.buckets) 
  
  bucket = each.key
  acl    = "private"
}