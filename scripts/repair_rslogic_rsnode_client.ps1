[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -Path $Path)) {
    throw "Target bootstrap script was not found: $Path"
}

$raw = Get-Content -Path $Path -Raw
$patched = $raw

$patched = $patched.Replace(
    'return "redis://:$escapedPassword@$cleanHost:$Port/$Database"',
    'return ("redis://:{0}@{1}:{2}/{3}" -f $escapedPassword, $cleanHost, $Port, $Database)'
)

$patched = [regex]::Replace(
    $patched,
    'function Resolve-Required\(\[string\]\$PromptText\)\s*\{[\s\S]*?param\(\[string\]\$Current\)',
    'function Resolve-Required {`r`n    param(`r`n        [string]$PromptText,`r`n        [string]$Current`r`n    )',
    [Text.RegularExpressions.RegexOptions]::Singleline
)

if ($patched -ne $raw) {
    Set-Content -Path $Path -Value $patched -Encoding UTF8
}
