terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  # usando variables
  project  = var.gcp_project_id
  region   = var.gcp_region
}
