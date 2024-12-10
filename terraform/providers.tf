# Proveedores necesarios, por ejemplo
# 'hashicorp/azurerm'  
# 'random' para nombres de mascotas al azar

terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "~>3.0"    # cualquier version 3.x
    }
    random = {
      source = "hashicorp/random"  
      version = "~>3.0"
    }
  }
}

# esto configura el azure resource maanger
provider "azurerm" {
  features {}
}
