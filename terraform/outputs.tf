output "client_certificate" {
  value = azurerm_kubernetes_cluster.bdcc.kube_config.0.client_certificate
  sensitive = true
}

output "kube_config" {
  value = azurerm_kubernetes_cluster.bdcc.kube_config_raw
  sensitive = true
}

output "open_cage_key" {
  value = var.OPEN_CAGE_KEY
  sensitive = true
}