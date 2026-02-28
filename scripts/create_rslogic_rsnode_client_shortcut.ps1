[CmdletBinding()]
param(
    [string]$ShortcutName = "RsLogic RSNode Client.lnk",
    [string]$ShortcutPath = "",
    [string]$WorkingDirectory = "",
    [switch]$RunAfterCreate
)

$ErrorActionPreference = "Stop"

$scriptRoot = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $PSCommandPath }
$startScript = Join-Path $scriptRoot "start_rslogic_rsnode_client.bat"

if (-not (Test-Path $startScript)) {
    throw "Missing start script at $startScript"
}

if (-not $ShortcutPath) {
    $shortcutFolder = Join-Path $env:USERPROFILE "Desktop"
    if (-not (Test-Path $shortcutFolder)) {
        $shortcutFolder = $env:USERPROFILE
    }
    $ShortcutPath = Join-Path $shortcutFolder $ShortcutName
}

if (-not $WorkingDirectory) {
    $WorkingDirectory = $scriptRoot
}

$workingDirectory = [Environment]::ExpandEnvironmentVariables($WorkingDirectory)
$wsh = New-Object -ComObject WScript.Shell
$shortcut = $wsh.CreateShortcut($ShortcutPath)
$shortcut.TargetPath = $startScript
$shortcut.WorkingDirectory = $workingDirectory
$shortcut.WindowStyle = 1
$shortcut.Description = "RsLogic RSNode Client launcher"
$shortcut.Save()

Write-Host "Created shortcut: $ShortcutPath"
Write-Host "Target: $startScript"
Write-Host "Working directory: $workingDirectory"

if ($RunAfterCreate) {
    Start-Process -FilePath $startScript -WorkingDirectory $workingDirectory
}
