<#
.SYNOPSIS
  Builds the Kraken backend container image and pushes it to Azure Container Registry.

.DESCRIPTION
  Uses ACR Tasks (`az acr build`) so no local Docker engine is required and the
  build runs in the same region as the registry. Creates the registry on first
  run if it does not exist.

.EXAMPLE
  ./build-and-push.ps1 -SubscriptionId 00000000-... -ResourceGroup rg-kraken-shared `
                       -RegistryName krknacr -Location australiaeast -ImageTag 0.1.0
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string] $SubscriptionId,
  [Parameter(Mandatory)] [string] $ResourceGroup,
  [Parameter(Mandatory)] [string] $RegistryName,
  [string] $Location = 'australiaeast',
  [string] $ImageName = 'kraken-backend',
  [Parameter(Mandatory)] [string] $ImageTag,
  [string] $Sku = 'Standard'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')
$backendCtx = Join-Path $repoRoot 'backend'

Write-Host ">> Setting subscription $SubscriptionId"
az account set --subscription $SubscriptionId | Out-Null

Write-Host ">> Ensuring resource group $ResourceGroup ($Location)"
az group create --name $ResourceGroup --location $Location --only-show-errors | Out-Null

$rgExists = az acr show --name $RegistryName --resource-group $ResourceGroup --only-show-errors --query name -o tsv 2>$null
if (-not $rgExists) {
  Write-Host ">> Creating ACR $RegistryName ($Sku)"
  az acr create `
    --name $RegistryName `
    --resource-group $ResourceGroup `
    --location $Location `
    --sku $Sku `
    --admin-enabled false `
    --only-show-errors | Out-Null
}

$fullImage = "{0}.azurecr.io/{1}:{2}" -f $RegistryName, $ImageName, $ImageTag
Write-Host ">> Building + pushing $fullImage from $backendCtx"
az acr build `
  --registry $RegistryName `
  --resource-group $ResourceGroup `
  --image "$ImageName`:$ImageTag" `
  --file (Join-Path $backendCtx 'Dockerfile') `
  $backendCtx

Write-Host ""
Write-Host "Image pushed: $fullImage"
$acrId = az acr show --name $RegistryName --resource-group $ResourceGroup --query id -o tsv
Write-Host "ACR resourceId: $acrId"
Write-Host ""
Write-Host "Use these values in your *.bicepparam:"
Write-Host "  param containerImage = '$fullImage'"
Write-Host "  param acrResourceId  = '$acrId'"
