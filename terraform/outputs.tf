
output "instance_public_ip_address" {
  description = "Public ip of the Cloud SQL instance itself"
  value       = google_sql_database_instance.mysql_instance.ip_address
}

output "instance_connection_name" {
  description = "Connection name of the CLoud SQL instance (used for Proxy)"
  value       = google_sql_database_instance.mysql_instance.connection_name
}

output "database_name" {
  description = "The name of the database within the Cloud SQL instance"
  value       = google_sql_database.guayerd_db.name
}

output "database_user_name" {
  description = "The name of the user created for the DB"
  value       = google_sql_user.db_user.name
}

output "database_user_password" {
  description = "The generated password of the user created"
  value       = random_string.db_password.result
  sensitive   = true                              # marks as sensitive in the tf logs
}

