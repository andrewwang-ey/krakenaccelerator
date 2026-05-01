<#
.SYNOPSIS
  Deploys the Kraken Accelerator infra into a target Azure subscription.

.DESCRIPTION
  Subscription-scope Bicep deployment of main.bicep using a *.bicepparam file.
  After resources are stamped, prints the App Service principalId so you can
  grant Azure SQL access via T-SQL (see infra/sql/grant-app-mi.sql).

.EXAMPLE
  ./deploy.ps1 -SubscriptionId 00000000-... -ParameterFile ../clientA.bicepparam
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string] $SubscriptionId,
  [Parameter(Mandatory)] [string] $ParameterFile,
  [string] $Location = 'australiaeast',
  [string] $DeploymentName = "kraken-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))"
)

$ErrorActionPreference = 'Stop'
$infraRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$mainBicep = Join-Path $infraRoot 'main.bicep'
$paramPath = Resolve-Path $ParameterFile

Write-Host ">> Setting subscription $SubscriptionId"
az account set --subscription $SubscriptionId | Out-Null

Write-Host ">> what-if preview ($DeploymentName)"
az deployment sub what-if `
  --name $DeploymentName `
  --location $Location `
  --template-file $mainBicep `
  --parameters $paramPath

$confirm = Read-Host "Proceed with deployment? (y/N)"
if ($confirm -ne 'y' -and $confirm -ne 'Y') {
  Write-Host "Aborted."
  exit 1
}

Write-Host ">> Deploying"
$result = az deployment sub create `
  --name $DeploymentName `
  --location $Location `
  --template-file $mainBicep `
  --parameters $paramPath `
  --output json | ConvertFrom-Json

$outputs = $result.properties.outputs
Write-Host ""
Write-Host "Deployment complete."
Write-Host "  Resource group : $($outputs.resourceGroupName.value)"
Write-Host "  App Service    : $($outputs.appServiceName.value)"
Write-Host "  Hostname       : https://$($outputs.appServiceDefaultHostname.value)"
Write-Host "  MI principalId : $($outputs.appServicePrincipalId.value)"
Write-Host ""
Write-Host "Next step: grant the App Service MI access to Azure SQL."
Write-Host "  See infra/sql/grant-app-mi.sql - run it against the target database"
Write-Host "  while signed in as the SQL AAD admin, replacing <APP_NAME> with:"
Write-Host "    $($outputs.appServiceName.value)"
