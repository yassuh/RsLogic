@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=%~dp0"
if "%ROOT%"=="" set "ROOT=%CD%\"

echo [*] Stopping rslogic client processes

powershell -NoProfile -Command "$ErrorActionPreference='SilentlyContinue'; $client=Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^(python|pythonw)\.exe$' -and $_.CommandLine -like '*rslogic.client.rsnode_client*' }; if ($client) { $client | ForEach-Object { Write-Host ('[INFO] stopping client pid=' + $_.ProcessId); Stop-Process -Id $_.ProcessId -Force } } else { Write-Host '[INFO] no rslogic client process found' }; $node=Get-Process -Name RSNode -ErrorAction SilentlyContinue; if ($node) { $node | ForEach-Object { Write-Host ('[INFO] stopping RSNode pid=' + $_.Id); Stop-Process -Id $_.Id -Force } }"

echo [*] Stop command sent.
