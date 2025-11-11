<#
Remediate duplicate files based on a CSV exported from Databricks dashboards.

The CSV must contain at least two columns:
  - path: full path to the original file (UNC path, SharePoint location, etc.)
  - content_sha256: hash used to group duplicates. Empty hashes are permitted.

Usage:
    .\remediate_duplicates.ps1 -CsvPath path\to\results.csv -ArchiveRoot \\server\archive

For each row, the script moves the file into $ArchiveRoot\<hash>\filename
and replaces the original file with a text stub for end users.
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$CsvPath,

    [Parameter(Mandatory = $true)]
    [string]$ArchiveRoot,

    [string]$StubMessage = "This file has been removed due to records and retention policy. Contact your administrator for access."
)

if (-not (Test-Path -LiteralPath $CsvPath)) {
    Write-Error "CSV file not found: $CsvPath"
    exit 1
}

New-Item -ItemType Directory -Force -Path $ArchiveRoot | Out-Null

$rows = Import-Csv -LiteralPath $CsvPath
foreach ($row in $rows) {
    $source = $row.path
    if ([string]::IsNullOrWhiteSpace($source)) {
        continue
    }
    if (-not (Test-Path -LiteralPath $source)) {
        Write-Warning "Source path not found: $source"
        continue
    }

    $hash = $row.content_sha256
    if ([string]::IsNullOrWhiteSpace($hash)) {
        $hash = "unhashed"
    }

    $targetFolder = Join-Path -Path $ArchiveRoot -ChildPath $hash
    New-Item -ItemType Directory -Force -Path $targetFolder | Out-Null

    $fileName = Split-Path -Path $source -Leaf
    $archivePath = Join-Path -Path $targetFolder -ChildPath $fileName

    Move-Item -LiteralPath $source -Destination $archivePath -Force
    Set-Content -LiteralPath $source -Value $StubMessage -Force -Encoding UTF8
}
