@echo off
setlocal enabledelayedexpansion

echo ========================================================
echo  Sincrona Bridge - Enterprise Nuitka Compiler
echo ========================================================

cd /d "%~dp0"

echo [1/3] Curatare directoare anterioare...
if exist "dist" rmdir /s /q "dist"

echo [2/3] Compilare Nuitka C++ Native Executable...
python -m nuitka ^
  --standalone ^
  --windows-console-mode=disable ^
  --assume-yes-for-downloads ^
  --enable-plugin=anti-bloat ^
  --include-package=watchdog ^
  --include-package=websockets ^
  --include-package=requests ^
  --output-dir=dist ^
  --output-filename=SincronaBridge.exe ^
  erpnet_fp_bridge.py

if %ERRORLEVEL% NEQ 0 (
  echo [EROARE] Compilarea a esuat cu codul %ERRORLEVEL%.
  exit /b %ERRORLEVEL%
)

echo [3/3] Copiere dependinte si nssm.exe in pachet...
if exist "nssm.exe" (
  copy /y "nssm.exe" "dist\erpnet_fp_bridge.dist\"
)

echo ========================================================
echo  COMPILARE NUITKA FINALIZATA CU SUCCES!
echo  Director: dist\erpnet_fp_bridge.dist\
echo ========================================================
