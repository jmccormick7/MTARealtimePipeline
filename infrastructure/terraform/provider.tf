terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0" # or whatever you're using
    }
    helm = {
      source  = "hashicorp/helm"
      version = "2.11.0"
    }
  }
}

provider "google" {
  credentials = file("key.json")
  project     = var.project_id
  region      = var.region
}

provider "helm" {
  kubernetes {
    host                   = google_container_cluster.primary.endpoint
    token                  = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth[0].cluster_ca_certificate)
  }
}
