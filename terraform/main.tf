terraform {
  backend "azurerm" {
    storage_account_name = "bd201staccaparkbasics"
    container_name = "m06sparkbasics"
    key = "m06sparkbasics"
    resource_group_name = "bd201stacc-resource-group"
  }
}

provider "azurerm" {
//  version = "~> 2.65"
  features {
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bdcc" {
  name = "rg-${var.ENV}-${var.LOCATION}"
  location = var.LOCATION

//  lifecycle {
//    prevent_destroy = true
//  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc]

  name = "st${var.ENV}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  account_tier = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled = "true"

  network_rules {
    default_action = "Allow"
    ip_rules = values(var.IP_RULES)
  }

//  lifecycle {
//    prevent_destroy = true
//  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

//resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
//  depends_on = [
//    azurerm_storage_account.bdcc]
//
//  name = "data"
//  storage_account_id = azurerm_storage_account.bdcc.id
//
////  lifecycle {
////    prevent_destroy = true
////  }
//}


resource "azurerm_kubernetes_cluster" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc]

  name                = "aks-${var.ENV}-${var.LOCATION}"
  location            = azurerm_resource_group.bdcc.location
  resource_group_name = azurerm_resource_group.bdcc.name
  dns_prefix          = "bdcc${var.ENV}"

  default_node_pool {
    name       = "default"
    node_count = 1
    vm_size    = "Standard_D2_v2"
  }

  identity {
    type = "SystemAssigned"
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

//resource "azurerm_key_vault" "bdcc" {
//  name = "st${var.ENV}${var.LOCATION}"
//  location                    = var.LOCATION
//  resource_group_name = azurerm_resource_group.bdcc.name
//  enabled_for_disk_encryption = false
//  tenant_id                   = data.azurerm_client_config.current.tenant_id
//  soft_delete_enabled         = true
//  soft_delete_retention_days  = 1
//  purge_protection_enabled    = false
//
//  sku_name = "standard"
//
//  access_policy {
//    tenant_id = data.azurerm_client_config.current.tenant_id
//    object_id = data.azurerm_client_config.current.object_id
//
//    key_permissions = [
//      "get",
//    ]
//
//    secret_permissions = [
//      "get",
//      "list",
////      "set",
//      "delete"
//    ]
//
//    storage_permissions = [
//      "get",
//    ]
//  }
//}
//
//resource "azurerm_key_vault_secret" "azure-auth" {
//  name = "client-endpoint"
//  value = var.STORAGE_ACCOUNT_CLIENT_ENDPOINT
//  key_vault_id = azurerm_key_vault.bdcc.id
//}

//resource "null_resource" "example1" {
//  provisioner "local-exec" {
//    command = "spark-submit --master k8s://https://kubernetes.docker.internal:6443 --deploy-mode cluster --name sparkbasics --conf spark.kubernetes.container.image=irinasharnikovaepam/sparkbasics  --conf fs.azure.account.auth.type.${var.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=OAuth  --conf fs.azure.account.oauth.provider.type.${var.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider  --conf fs.azure.account.oauth2.client.id.${var.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=${var.OAUTH2_CLIENT_ID}  --conf fs.azure.account.oauth2.client.secret.${var.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=${var.OAUTH2_CLIENT_SECRET}  --conf fs.azure.account.oauth2.client.endpoint.${var.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=${var.OAUTH2_CLIENT_ENDPOINT}  --class SparkApp --executor-memory 1g local://data_engineer_course/scala/m06_sparkbasics_jvm_azure/target/sparkbasics-1.0.0.jar"
//    interpreter = ["PowerShell", "-Command"]
//  }
//}
