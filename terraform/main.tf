
resource "random_string" "db_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

#           -- Google Cloud SQL ---
resource "google_sql_database_instance" "mysql_instance" {
  name             = "guayerd-instance-db"
  region           = var.gcp_region
  database_version = "MYSQL_8_0"

  settings {
    tier = var.db_instance_tier

    ip_configuration {
      ipv4_enabled = true

      dynamic "authorized_networks" {
        for_each = var.authorized_networks
        content {
          name = authorized_networks.value.name
          value = authorized_networks.value.value
        }
      }
    }
  }
}

#             --- DB dentro del Cloud SQL service ---
resource "google_sql_database" "guayerd_db" {
  # configs from .env
  name     = "db_fundacion_final"
  instance = google_sql_database_instance.mysql_instance.name
  project  = var.gcp_project_id
}

#             --- DB user ---
resource "google_sql_user" "db_user" {
  name     = var.db_username
  instance = google_sql_database_instance.mysql_instance.name
  project  = var.gcp_project_id
  password = random_string.db_password.result
}
