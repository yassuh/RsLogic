@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "START_SCRIPT=%SCRIPT_DIR%start_rslogic_rsnode_client.bat"
set "SHORTCUT_PATH=%~1"
if not defined SHORTCUT_PATH set "SHORTCUT_PATH=%USERPROFILE%\Desktop\RsLogic RSNode Client.lnk"

if /I "%~2"=="--run" set "RUN_AFTER=1"
if /I "%~2"=="/run" set "RUN_AFTER=1"

if not exist "%START_SCRIPT%" (
    echo Missing launcher: %START_SCRIPT%
    echo Reinstall repository scripts and retry.
    pause
    exit /b 1
)

set "VBS=%TEMP%\_rslogic_rsnode_client_shortcut.vbs"
(
    echo Set shell = CreateObject("WScript.Shell")
    echo Set shortcut = shell.CreateShortcut("%SHORTCUT_PATH%")
    echo shortcut.TargetPath = "%START_SCRIPT%"
    echo shortcut.WorkingDirectory = "%SCRIPT_DIR%"
    echo shortcut.WindowStyle = 1
    echo shortcut.Description = "RsLogic RSNode Client launcher"
    echo shortcut.Save
) > "%VBS%"

cscript //NoLogo "%VBS%"
if errorlevel 1 (
    echo Failed to create shortcut.
    del "%VBS%" >nul 2>&1
    pause
    exit /b 1
)
del "%VBS%" >nul 2>&1

echo Created shortcut: %SHORTCUT_PATH%

if defined RUN_AFTER (
    start "" "%START_SCRIPT%"
)

endlocal
