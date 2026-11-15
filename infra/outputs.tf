output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}