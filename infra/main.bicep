// =============================================================================
// Kraken Accelerator - top-level deployment (subscription scope)
//
// Creates a resource group and stamps the full app footprint inside it:
//   - VNet + delegated subnets + private DNS zones
//   - App Service Plan (Linux, P1v3) + App Service (container)
//   - Private Endpoints for an existing Azure SQL server and Storage account
//   - Role assignments so the App Service Managed Identity can read Blob
//
// Azure SQL AAD user/role grant is NOT done here (T-SQL only) - see runbook.
// =============================================================================

targetScope = 'subscription'

@description('Short client/environment name, e.g. eyDev, clientA. Used in resource names + tags.')
param clientName string

@description('Azure region for all resources.')
param location string = 'australiaeast'

@description('Container image reference, e.g. krknacr.azurecr.io/kraken-backend:0.1.0')
param containerImage string

@description('Resource ID of the ACR that hosts containerImage. App Service MI will be granted AcrPull.')
param acrResourceId string

@description('Resource ID of the existing Azure SQL Server (parent of the database).')
param sqlServerResourceId string

@description('Name of the existing Azure SQL Database.')
param sqlDatabaseName string

@description('Resource ID of the existing Storage Account holding the blob containers.')
param storageAccountResourceId string

@description('VNet address space, e.g. 10.50.0.0/22')
param vnetAddressPrefix string = '10.50.0.0/22'

@description('Subnet for App Service VNet integration (delegated to Microsoft.Web/serverFarms).')
param appSubnetPrefix string = '10.50.0.0/27'

@description('Subnet for Private Endpoints.')
param peSubnetPrefix string = '10.50.0.32/27'

@description('Common tags applied to every resource.')
param tags object = {
  workload: 'kraken-accelerator'
  managedBy: 'bicep'
  client: clientName
}

var rgName = 'rg-kraken-${clientName}'

resource rg 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: rgName
  location: location
  tags: tags
}

module network 'modules/network.bicep' = {
  scope: rg
  name: 'network'
  params: {
    location: location
    clientName: clientName
    vnetAddressPrefix: vnetAddressPrefix
    appSubnetPrefix: appSubnetPrefix
    peSubnetPrefix: peSubnetPrefix
    tags: tags
  }
}

module privateEndpoints 'modules/privateEndpoints.bicep' = {
  scope: rg
  name: 'privateEndpoints'
  params: {
    location: location
    clientName: clientName
    peSubnetId: network.outputs.peSubnetId
    sqlPrivateDnsZoneId: network.outputs.sqlPrivateDnsZoneId
    blobPrivateDnsZoneId: network.outputs.blobPrivateDnsZoneId
    sqlServerResourceId: sqlServerResourceId
    storageAccountResourceId: storageAccountResourceId
    tags: tags
  }
}

module appService 'modules/appService.bicep' = {
  scope: rg
  name: 'appService'
  params: {
    location: location
    clientName: clientName
    containerImage: containerImage
    acrResourceId: acrResourceId
    appSubnetId: network.outputs.appSubnetId
    sqlServerFqdn: '${last(split(sqlServerResourceId, '/'))}${environment().suffixes.sqlServerHostname}'
    sqlDatabaseName: sqlDatabaseName
    storageAccountName: last(split(storageAccountResourceId, '/'))
    storageAccountResourceId: storageAccountResourceId
    tags: tags
  }
}

output appServiceName string = appService.outputs.appServiceName
output appServiceDefaultHostname string = appService.outputs.defaultHostname
output appServicePrincipalId string = appService.outputs.principalId
output resourceGroupName string = rg.name
