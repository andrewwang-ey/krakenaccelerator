// Example parameter file. Copy to e.g. clientA.bicepparam and customise per client.
using './main.bicep'

param clientName = 'eyDev'
param location = 'australiaeast'

// Container image to run. Build + push first using infra/scripts/build-and-push.ps1
param containerImage = 'krknacr.azurecr.io/kraken-backend:0.1.0'

// Cross-subscription/cross-tenant ACR is supported as long as the App Service MI
// has AcrPull on this registry resource.
param acrResourceId = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-kraken-shared/providers/Microsoft.ContainerRegistry/registries/krknacr'

// Existing Azure SQL server + database (already provisioned in this client's sub).
param sqlServerResourceId = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-kraken-data/providers/Microsoft.Sql/servers/sql-krkn-dev-dh'
param sqlDatabaseName = 'kraken'

// Existing Storage Account (already provisioned with blob containers raw/staged/etc.).
param storageAccountResourceId = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-kraken-data/providers/Microsoft.Storage/storageAccounts/stkrkndevdh'

param vnetAddressPrefix = '10.50.0.0/22'
param appSubnetPrefix = '10.50.0.0/27'
param peSubnetPrefix = '10.50.0.32/27'

param tags = {
  workload: 'kraken-accelerator'
  managedBy: 'bicep'
  client: 'eyDev'
  costCentre: 'TBD'
}
