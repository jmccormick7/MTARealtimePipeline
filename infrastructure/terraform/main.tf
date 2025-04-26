resource "google_container_cluster" "primary" {
  name     = "mta-cluster"
  location = var.region

  initial_node_count = 2

  deletion_protection = false

  node_config {
    machine_type = "e2-standard-4"
  }

}

data "google_client_config" "default" {}

resource "helm_release" "redis" {
  name       = "redis"
  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "redis"
  ## version    = "20.13.1"
  namespace = "default"
  timeout   = 600
  wait      = false

  set {
    name  = "architecture"
    value = "standalone"
  }

  set {
    name  = "auth.enabled"
    value = "true"
  }

  set {
    name  = "auth.password"
    value = var.redis_password
  }

  set {
    name  = "persistence.enabled"
    value = "true"
  }

  set {
    name  = "persistence.storageClass"
    value = "standard-rwo"
  }

  set {
    name  = "persistence.size"
    value = "1Gi" # Adjust size as needed
  }
}

# resource "helm_release" "kafka" {
#   name       = "kafka"
#   repository = "oci://registry-1.docker.io/bitnamicharts"
#   chart      = "kafka"
#   version    = "32.2.1"
#   namespace  = "default"
#   wait       = false

#   set {
#     name  = "replicaCount"
#     value = "1" # Adjust to your liking
#   }

#   set {
#     name  = "zookeeper.enabled"
#     value = "true"
#   }

#   set {
#     name  = "persistence.enabled"
#     value = "false"
#   }

#   set {
#     name  = "persistence.size"
#     value = "1Gi" # Lower the size if appropriate
#   }

#   values = [
#       <<-EOF
#       # This block is pure YAML, so Helm sees createTopics as one string
#       createTopics: "feed-ace-alert:3:1,feed-ace-vehicle-position:3:1,feed-ace-trip-update:3:1,feed-bdfm-alert:3:1,feed-bdfm-vehicle-position:3:1,feed-bdfm-trip-update:3:1,feed-g-alert:3:1,feed-g-vehicle-position:3:1,feed-g-trip-update:3:1,feed-irt-alert:3:1,feed-irt-vehicle-position:3:1,feed-irt-trip-update:3:1,feed-jz-alert:3:1,feed-jz-vehicle-position:3:1,feed-jz-trip-update:3:1,feed-l-alert:3:1,feed-l-vehicle-position:3:1,feed-l-trip-update:3:1,feed-nrqw-alert:3:1,feed-nrqw-vehicle-position:3:1,feed-nrqw-trip-update:3:1,feed-sir-alert:3:1,feed-sir-vehicle-position:3:1,feed-sir-trip-update:3:1"
#       EOF
#     ]
# }
