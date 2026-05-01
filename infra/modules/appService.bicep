// App Service Plan + Linux container Web App + Managed Identity role assignments.
targetScope = 'resourceGroup'

param location string
param clientName string
param containerImage string
param acrResourceId string
param appSubnetId string
param sqlServerFqdn string
param sqlDatabaseName string
param storageAccountName string
param storageAccountResourceId string
param tags object

@description('Pricing SKU for the App Service Plan. P1v3 is the smallest tier supporting Premium features (private endpoints, scale).')
param skuName string = 'P1v3'

var planName = 'plan-kraken-${clientName}'
var siteName = 'app-kraken-${clientName}'

resource plan 'Microsoft.Web/serverfarms@2024-04-01' = {
  name: planName
  location: location
  tags: tags
  sku: {
    name: skuName
    tier: 'PremiumV3'
  }
  kind: 'linux'
  properties: {
    reserved: true
  }
}

resource site 'Microsoft.Web/sites@2024-04-01' = {
  name: siteName
  location: location
  tags: tags
  kind: 'app,linux,container'
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    virtualNetworkSubnetId: appSubnetId
    vnetRouteAllEnabled: true
    publicNetworkAccess: 'Enabled'
    siteConfig: {
      linuxFxVersion: 'DOCKER|${containerImage}'
      acrUseManagedIdentityCreds: true
      alwaysOn: true
      ftpsState: 'Disabled'
      http20Enabled: true
      minTlsVersion: '1.2'
      healthCheckPath: '/healthz'
      appSettings: [
        { name: 'WEBSITES_PORT', value: '8000' }
        { name: 'WEBSITE_VNET_ROUTE_ALL', value: '1' }
        { name: 'AZURE_SQL_SERVER', value: sqlServerFqdn }
        { name: 'AZURE_SQL_DATABASE', value: sqlDatabaseName }
        { name: 'AZURE_STORAGE_ACCOUNT', value: storageAccountName }
        { name: 'DOCKER_ENABLE_CI', value: 'true' }
      ]
    }
  }
}

// Built-in role IDs
var roleAcrPull = '7f951dda-4ed3-4680-a7ca-43fe172d538d'
var roleStorageBlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource acr 'Microsoft.ContainerRegistry/registries@2023-11-01-preview' existing = {
  scope: resourceGroup(split(acrResourceId, '/')[2], split(acrResourceId, '/')[4])
  name: last(split(acrResourceId, '/'))
}

resource acrPullAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: acr
  name: guid(acr.id, site.id, roleAcrPull)
  properties: {
    principalId: site.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleAcrPull)
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  scope: resourceGroup(split(storageAccountResourceId, '/')[2], split(storageAccountResourceId, '/')[4])
  name: storageAccountName
}

resource blobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: storageAccount
  name: guid(storageAccount.id, site.id, roleStorageBlobDataContributor)
  properties: {
    principalId: site.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleStorageBlobDataContributor)
  }
}

output appServiceName string = site.name
output defaultHostname string = site.properties.defaultHostName
output principalId string = site.identity.principalId
