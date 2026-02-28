param(
    [switch]$Rebase,
    [switch]$HardReset
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "Fetching origin/main from remote..."
git fetch origin main --prune --tags

$before = (git rev-parse --short HEAD).Trim()
Write-Host ("Local HEAD: {0}" -f $before)
$remoteHead = (git rev-parse --short origin/main).Trim()
Write-Host ("origin/main: {0}" -f $remoteHead)

$relation = (git rev-list --left-right --count HEAD...origin/main).Trim()
$parts = [regex]::Matches($relation, '\S+') | ForEach-Object { $_.Value }
if ($parts.Count -lt 2) {
    throw "Unable to parse git divergence output: '$relation'"
}

$behind = [int]$parts[0]
$ahead = [int]$parts[1]
Write-Host ("Divergence: behind={0}, ahead={1}" -f $behind, $ahead)

if ($behind -eq 0 -and $ahead -eq 0) {
    Write-Host "Repo is already in sync."
    exit 0
}

if ($behind -gt 0 -and $ahead -eq 0) {
    Write-Host "Remote has updates, fast-forwarding local branch..."
    git pull --ff-only origin main
    exit 0
}

if ($behind -gt 0 -and $ahead -gt 0) {
    if ($HardReset) {
        Write-Warning "Diverged branch detected; forcing hard reset to origin/main."
        git reset --hard origin/main
        exit 0
    }

    if ($Rebase) {
        Write-Host "Diverged branch detected; rebasing onto origin/main..."
        git rebase origin/main
        exit 0
    }

    Write-Host "Diverged branch detected."
    Write-Host "Recommended fixes:"
    Write-Host "  .\sync_rslogic_repo.ps1 -Rebase"
    Write-Host "  .\sync_rslogic_repo.ps1 -HardReset"
    throw "Branch is diverged. Re-run with -Rebase or -HardReset."
}

if ($behind -eq 0 -and $ahead -gt 0) {
    Write-Host "Local branch is ahead of origin/main. Review local commits before pushing."
    Write-Host "Run git log --oneline --decorate --max-count=20 to inspect."
    exit 0
}
