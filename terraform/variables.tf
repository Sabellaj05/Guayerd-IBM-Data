
variable "gcp_project_id"  {
  type        = string
  description =  "Project id where everything will be created"
}

variable "gcp_region" {
  type        = string
  description = "The GCP region for deployment"
  default = "cheapest"
}

variable "db_instance_tier" {
  type        = string
  description = "The tier of the instance, no necesito mucho"
  default     = "db-f1-micro"
}

variable "db_username" {
  type        = string
  description = "The name of the db user"
  default     = "pepito"
}

variable "authorized_networks" {
  type = list(object({
    name  = string
    value = string
  }))
  description = "Lista de ip's autorizadas para conectarse a Cloud SQL, looker-ip y mi ip"
  default = []

}
