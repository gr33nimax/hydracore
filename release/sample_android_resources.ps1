param(
    [string]$PackageName = "com.etonify.meow_client",
    [ValidateRange(1, 1440)][int]$DurationMinutes = 30,
    [ValidateRange(5, 3600)][int]$IntervalSeconds = 30,
    [ValidateRange(0, 100000)][int]$SampleCount = 0,
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command adb -ErrorAction SilentlyContinue)) {
    throw "adb was not found in PATH"
}

$limits = @{}
Get-Content (Join-Path $PSScriptRoot "ETONIFY_PERFORMANCE_BASELINE") | ForEach-Object {
    if ($_ -match '^([A-Z0-9_]+)=(\d+)$') {
        $limits[$Matches[1]] = [int64]$Matches[2]
    }
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputPath = Join-Path (Get-Location) "android-resource-soak-$stamp.csv"
}
$OutputPath = [System.IO.Path]::GetFullPath($OutputPath)
$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

function Get-AppPid {
    $rawPid = (& adb shell pidof $PackageName 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($rawPid)) {
        return $null
    }
    return [int](($rawPid -split '\s+')[0])
}

function Read-Number([string]$Content, [string]$Pattern) {
    $match = [regex]::Match($Content, $Pattern)
    if (-not $match.Success) {
        return $null
    }
    return [int64]$match.Groups[1].Value
}

function Read-CpuPercent([int]$ProcessId) {
    $escapedPackage = [regex]::Escape($PackageName)
    $line = (& adb shell dumpsys cpuinfo 2>$null) |
        Where-Object { $_ -match "$ProcessId/$escapedPackage" } |
        Select-Object -First 1
    if ($line -and $line -match '^\s*([\d.,]+)%') {
        return [double]::Parse($Matches[1].Replace(',', '.'), [Globalization.CultureInfo]::InvariantCulture)
    }
    return 0.0
}

if ($SampleCount -eq 0) {
    $SampleCount = [math]::Ceiling(($DurationMinutes * 60) / $IntervalSeconds) + 1
}
$samples = [System.Collections.Generic.List[object]]::new()
$lastPid = $null
$restartCount = 0

for ($index = 0; $index -lt $SampleCount; $index++) {
    $processId = Get-AppPid
    if ($null -eq $processId) {
        if ($samples.Count -eq 0) {
            throw "$PackageName is not running"
        }
        Write-Warning "$PackageName is temporarily not running"
    } else {
        if ($null -ne $lastPid -and $processId -ne $lastPid) {
            $restartCount++
        }
        $lastPid = $processId
        $meminfo = (& adb shell dumpsys meminfo $processId | Out-String)
        $fdCommand = "ls /proc/$processId/fd 2>/dev/null | wc -l"
        $fdCount = [int64]((& adb shell sh -c $fdCommand | Out-String).Trim())
        $samples.Add([pscustomobject]@{
            Timestamp  = (Get-Date).ToString("o")
            Pid        = $processId
            PssKiB     = Read-Number $meminfo 'TOTAL PSS:\s+(\d+)'
            RssKiB     = Read-Number $meminfo 'TOTAL RSS:\s+(\d+)'
            SwapPssKiB = Read-Number $meminfo 'TOTAL SWAP PSS:\s+(\d+)'
            FdCount    = $fdCount
            CpuPercent = Read-CpuPercent $processId
        })
        $samples | Export-Csv -NoTypeInformation -Encoding utf8 -Path $OutputPath
        $latest = $samples[$samples.Count - 1]
        Write-Host ("{0} pid={1} pss={2:N1} MiB rss={3:N1} MiB fd={4} cpu={5:N1}%" -f `
            $latest.Timestamp, $latest.Pid, ($latest.PssKiB / 1024), ($latest.RssKiB / 1024), $latest.FdCount, $latest.CpuPercent)
    }
    if ($index + 1 -lt $SampleCount) {
        Start-Sleep -Seconds $IntervalSeconds
    }
}

$first = $samples[0]
$maxPssKiB = ($samples | Measure-Object PssKiB -Maximum).Maximum
$maxFd = ($samples | Measure-Object FdCount -Maximum).Maximum
$minFd = ($samples | Measure-Object FdCount -Minimum).Minimum
$pssGrowthKiB = $samples[$samples.Count - 1].PssKiB - $first.PssKiB
$averageCpu = ($samples | Measure-Object CpuPercent -Average).Average

Write-Host ("Saved {0} samples to {1}; restarts={2}; average CPU={3:N1}%" -f $samples.Count, $OutputPath, $restartCount, $averageCpu)

$failures = [System.Collections.Generic.List[string]]::new()
if ($maxPssKiB -gt ($limits.ANDROID_MAX_PSS_MIB * 1024)) {
    $failures.Add("maximum PSS exceeded $($limits.ANDROID_MAX_PSS_MIB) MiB")
}
if ($pssGrowthKiB -gt ($limits.ANDROID_MAX_PSS_GROWTH_MIB * 1024)) {
    $failures.Add("PSS growth exceeded $($limits.ANDROID_MAX_PSS_GROWTH_MIB) MiB")
}
if (($maxFd - $minFd) -gt $limits.ANDROID_MAX_FD_GROWTH) {
    $failures.Add("file descriptor spread exceeded $($limits.ANDROID_MAX_FD_GROWTH)")
}
if ($failures.Count -gt 0) {
    throw ($failures -join "; ")
}
