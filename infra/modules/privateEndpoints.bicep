// Private endpoints for the existing Azure SQL server and Storage account,
// plus their private-DNS zone group registrations.
targetScope = 'resourceGroup'

param location string
param clientName string
param peSubnetId string
param sqlPrivateDnsZoneId string
param blobPrivateDnsZoneId string
param sqlServerResourceId string
param storageAccountResourceId string
param tags object

resource peSql 'Microsoft.Network/privateEndpoints@2024-01-01' = {
  name: 'pe-sql-${clientName}'
  location: location
  tags: tags
  properties: {
    subnet: { id: peSubnetId }
    privateLinkServiceConnections: [
      {
        name: 'sql'
        properties: {
          privateLinkServiceId: sqlServerResourceId
          groupIds: [ 'sqlServer' ]
        }
      }
    ]
  }
}

resource peSqlDns 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-01-01' = {
  parent: peSql
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'sql'
        properties: { privateDnsZoneId: sqlPrivateDnsZoneId }
      }
    ]
  }
}

resource peBlob 'Microsoft.Network/privateEndpoints@2024-01-01' = {
  name: 'pe-blob-${clientName}'
  location: location
  tags: tags
  properties: {
    subnet: { id: peSubnetId }
    privateLinkServiceConnections: [
      {
        name: 'blob'
        properties: {
          privateLinkServiceId: storageAccountResourceId
          groupIds: [ 'blob' ]
        }
      }
    ]
  }
}

resource peBlobDns 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-01-01' = {
  parent: peBlob
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'blob'
        properties: { privateDnsZoneId: blobPrivateDnsZoneId }
      }
    ]
  }
}
