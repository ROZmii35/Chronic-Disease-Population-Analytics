#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "  Setup Environment: Chronic Disease Population Analytics"
echo "=========================================================="

# 1. Cek Java
echo -e "\n[1/3] Memeriksa Instalasi Java..."
if command -v java >/dev/null 2>&1; then
    java -version
else
    echo "[ERROR] Java tidak ditemukan di PATH! Diperlukan Java 8, 11, atau 17."
    exit 1
fi

# 2. Buat venv
echo -e "\n[2/3] Memeriksa Python & Membuat Virtual Environment..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# 3. Install requirements
echo -e "\n[3/3] Memasang Dependencies..."
pip install --upgrade pip
pip install -r "$SCRIPT_DIR/requirements.txt"

echo -e "\n=========================================================="
echo "  Setup Berhasil! Jalankan dengan: python analytical/run_all.py"
echo "=========================================================="
