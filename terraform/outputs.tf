output "client_certificate" {
  value = azurerm_kubernetes_cluster.bdcc.kube_config.0.client_certificate
  sensitive = true
}

output "kube_config" {
  value = azurerm_kubernetes_cluster.bdcc.kube_config_raw
  sensitive = true
}

output "storage-account-name" {
  value = var.STORAGE_ACCOUNT_NAME
  sensitive = true
}

output "oauth2-client-id" {
  value = var.OAUTH2_CLIENT_ID
  sensitive = true
}

output "oauth2-client-secret" {
  value = var.OAUTH2_CLIENT_SECRET
  sensitive = true
}

output "oauth2-client-endpoint" {
  value = var.OAUTH2_CLIENT_ENDPOINT
  sensitive = true
}
