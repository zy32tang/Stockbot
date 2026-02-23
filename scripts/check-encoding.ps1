param(
    [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$checker = Join-Path $PSScriptRoot "EncodingCheck.java"
if (-not (Test-Path $checker)) {
    Write-Error "Encoding checker not found: $checker"
    exit 2
}

& java $checker $Root
exit $LASTEXITCODE
