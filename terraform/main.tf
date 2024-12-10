# `random_et` es de terraform 

resource "random_pet" "prefix" {
  length = 1
}

# creamos el resource group con un prefijo al nombre
        # type of resource       # name
resource "azurerm_resource_group" "rg" {
  name             = "${random_pet.prefix.id}-rg-data-pipeline"
  # location = "useast"    --> mirar bien los nombres xd
  location = "eastus"
}

# para sacar data de azure
data "azurerm_client_config" "current" {}

# creamos Azure key vault
resource "azurerm_key_vault" "kv" {
  name                = "${random_pet.prefix.id}-kv-data-pipeline"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tenant_id           = data.azurerm_client_config.current.tenant_id  # from data
  sku_name            = "standard"
}

# creamos (Storage Account) Azure Data Lake Storage (ADLS)
#  
resource "azurerm_storage_account" "adls" {
  name                     = "${random_pet.prefix.id}adlsdatapipeline"  # sin "-"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"       # Local redundant zone
  # para que pueda ser usado como data lake
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

# Los diferentes containers dentro del ADLS
resource "azurerm_storage_container" "containers" {
  for_each              = toset(["bronze", "silver", "gold"])
  name                  = "${random_pet.prefix.id}-${each.value}" # iterar los 3 nombres
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# creamos Azure Data Factory (ADF)
resource "azurerm_data_factory" "adf" {
  name                = "${random_pet.prefix.id}-adf-data-pipeline"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

# Agregamos un file system al data lake es requiredio despues por Synapse Analytics
resource "azurerm_storage_data_lake_gen2_filesystem" "adls-fs" {
  name               = "${random_pet.prefix.id}-adls-fs"
  storage_account_id = azurerm_storage_account.adls.id

  properties = {
    hello = "aGVsbG8="
  }
}

# creamos Databricks
resource "azurerm_databricks_workspace" "databricks" {
  name                = "${random_pet.prefix.id}-databricks-data-pipeline"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "standard"
}

# creamos Azure Synapse Analytics
resource "azurerm_synapse_workspace" "synapse" {
  name                = "${random_pet.prefix.id}-synapse-data-pipeline"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.adls-fs.id
  sql_administrator_login          = "synapseadmin"
  sql_administrator_login_password = "SuperPassword123!"
  
  identity {
    type = "SystemAssigned"
  }
}

