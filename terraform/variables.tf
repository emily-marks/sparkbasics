variable "ENV" {
  type = string
  description = "The prefix which should be used for all resources in this environment. Make it unique, like ksultanau."
}

variable "LOCATION" {
  type = string
  description = "The Azure Region in which all resources in this example should be created."
  default = "westeurope"
}

variable "BDCC_REGION" {
  type = string
  description = "The BDCC Region for billing."
  default = "global"
}

variable "STORAGE_ACCOUNT_REPLICATION_TYPE" {
  type = string
  description = "Storage Account replication type."
  default = "LRS"
}

variable "OPEN_CAGE_KEY" {
  type = string
  description = "Key for accessing Open Cage API."
  sensitive = true
}

variable "CONTAINER_NAME" {
  type = string
  description = "Container name."
  sensitive = true
}

variable "OAUTH2_CLIENT_SECRET" {
  type = string
  description = "Storage Account secret."
  sensitive = true
}

variable "STORAGE_ACCOUNT_NAME" {
  type = string
  description = "Storage Account name."
  sensitive = true
}

variable "OAUTH2_CLIENT_ID" {
  type = string
  description = "Storage Account client id."
  sensitive = true
}

variable "OAUTH2_CLIENT_ENDPOINT" {
  type = string
  description = "Storage Account fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net property."
  sensitive = true
}

variable "IP_RULES" {
  type = map(string)
  description = "Map of IP addresses permitted to access"
  default = {
    "epam-vpn-ru-0" = "185.44.13.36"
    "epam-vpn-eu-0" = "195.56.119.209"
    "epam-vpn-by-0" = "213.184.231.20"
    "epam-vpn-by-1" = "86.57.255.94"
  }
}
