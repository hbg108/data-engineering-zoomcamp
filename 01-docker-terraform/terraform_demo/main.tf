terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = "de-zc-hbg"
  region  = "asia-northeast2"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "de-zc-hbg-terraform-bucket"
  location      = "ASIA-NORTHEAST2"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}