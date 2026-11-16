# Setup script for Windows PowerShell
$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Setup Environment: Chronic Disease Population Analytics" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Cek Java
Write-Host "`n[1/3] Memeriksa Instalasi Java..." -ForegroundColor Yellow
try {
    $javaVer = & java -version 2>&1
    Write-Host "Java terdeteksi:" -ForegroundColor Green
    $javaVer | ForEach-Object { Write-Host "   $_" }
} catch {
    Write-Host "[ERROR] Java tidak ditemukan di PATH! Diperlukan Java 8, 11, atau 17 untuk menjalankan Apache Spark." -ForegroundColor Red
    Write-Host "Silakan unduh dan pasang OpenJDK / Oracle JDK, lalu tambahkan ke PATH dan set JAVA_HOME." -ForegroundColor Red
    exit 1
}

# 2. Cek Python & Buat venv
Write-Host "`n[2/3] Memeriksa Python & Membuat Virtual Environment..." -ForegroundColor Yellow
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvDir = Join-Path $scriptDir ".venv"

if (-not (Test-Path $venvDir)) {
    Write-Host "Membuat venv di: $venvDir"
    if (Get-Command py -ErrorAction SilentlyContinue) {
        py -3.9 -m venv $venvDir
    } else {
        python -m venv $venvDir
    }
} else {
    Write-Host "Virtual environment sudah ada di $venvDir" -ForegroundColor Green
}

$venvPython = Join-Path $venvDir "Scripts\python.exe"
$venvPip = Join-Path $venvDir "Scripts\pip.exe"

# 3. Install Requirements
Write-Host "`n[3/3] Memasang Dependencies dari requirements.txt..." -ForegroundColor Yellow
$reqFile = Join-Path $scriptDir "requirements.txt"
& $venvPython -m pip install --upgrade pip
& $venvPip install -r $reqFile

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "  Setup Berhasil Selesai!" -ForegroundColor Green
Write-Host "  Jalankan modul dengan: & '$venvPython' analytical/run_all.py" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
