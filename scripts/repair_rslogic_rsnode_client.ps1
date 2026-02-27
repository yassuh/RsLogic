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
$isChanged = $false

$patched = $patched.Replace(
    'return "redis://:$escapedPassword@$cleanHost:$Port/$Database"',
    'return ("redis://:{0}@{1}:{2}/{3}" -f $escapedPassword, $cleanHost, $Port, $Database)'
)
if ($patched -ne $raw) {
    $isChanged = $true
}

$expectedResolveFunction = @'
function Resolve-Required {
    param(
        [string]$PromptText,
        [string]$Current
    )
    if ($Current) {
        return $Current
    }
    if ($NoPromptForSecrets) {
        return ""
    }
    return (Read-Host $PromptText)
}
'@

$hasCanonicalResolveFunction = $patched -match 'function\s+Resolve-Required\s*\{\s*param\s*\(\s*\[string\]\$PromptText,\s*\[string\]\$Current\s*\)\s*\)'
if (-not $hasCanonicalResolveFunction) {
    $resolveStartPattern = '(?ms)^\s*function\s+Resolve-Required\b[\s\S]*?(?=^\s*function\s+[A-Za-z_][A-Za-z0-9_-]*\s*\()'
    $resolvePatched = [regex]::Replace(
        $patched,
        $resolveStartPattern,
        $expectedResolveFunction + "`r`n",
        [Text.RegularExpressions.RegexOptions]::Multiline
    )
    if ($resolvePatched -ne $patched) {
        $patched = $resolvePatched
        $isChanged = $true
    }
}

$tokens = $null
$errors = @()
[System.Management.Automation.Language.Parser]::ParseInput($patched, [ref]$tokens, [ref]$errors) | Out-Null
if ($errors -and $errors.Count -gt 0) {
    if ($isChanged) {
        throw "Failed to repair bootstrap script syntax: $($errors[0].Message)"
    }
    throw "Bootstrap script syntax is invalid in the current checkout: $($errors[0].Message)"
}

if ($isChanged) {
    Set-Content -Path $Path -Value $patched -Encoding UTF8
}
