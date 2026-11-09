# Sincrona Universal Fiscal Bridge (BFBridge)
> Enterprise IoT Edge Agent pentru conectarea caselor de marcat fiscale Windows la platforma SaaS Sincrona Auto.

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Platform](https://img.shields.io/badge/Platform-Windows%2010%20%7C%2011%20%7C%20Server-lightgrey.svg)]()
[![Architecture](https://img.shields.io/badge/Architecture-C%2B%2B%20Native%20(Nuitka)-orange.svg)]()
[![Service](https://img.shields.io/badge/Service-NSSM%20Background%20Service-green.svg)]()

---

## 📌 Prezentare Generală

**BFBridge** este un agent middleware de nivel enterprise care rulează pe stațiile Windows din service-urile auto. Acesta înlocuiește vechile binare împachetate cu PyInstaller printr-un executabil compilat nativ în C++ folosind **Nuitka**, eliminând definitiv alarmele false din Windows Defender și SmartScreen.

Aplicația rulează complet invizibil ca **Windows Background Service** gestionat prin **NSSM**, asigurând repornire automată la boot și recuperare în caz de eroare.

---

## 🚀 Piloni Arhitecturali

### 1. Router Fiscal Universal
Bridge-ul expune o interfață locală de configurare la `http://localhost:8002/` și menține o conexiune deschisă bidirecțională prin **WebSockets** cu `https://sincrona.ro` (cu heartbeat ping/pong și fallback pe HTTP Polling).

Pachetele JSON universale primite din cloud sunt rutate nativ către:
- **ErpNet.FP**: HTTP REST pe `localhost:8001` (bonuri, rapoarte X/Z, gestiune idempotență `E109`).
- **Datecs DUDE & FPrinter**: Generează fișiere `.inp` în folderul `IN` cu encodare Windows-1250 (`cp1250`) și detectează asincron răspunsul prin `watchdog` pe folderul `OUT`.
- **Tremol ZFPServer**: Generează fișiere `bon.<id>.txt` în folderul `Bonuri` (`cp1250`) și citește rezultatul din `raspuns.txt`.

### 2. Compilare Nativă Nuitka (Zero False Positives)
Codul Python este transpus în cod C++ și compilat nativ (`--standalone`, `--windows-console-mode=disable`). Deoarece fișierele nu mai sunt dezarhivate silențios în directorul `%TEMP%` ca la PyInstaller, antivirusul nu le mai blochează.

### 3. Serviciu Windows Invizibil (NSSM)
Bridge-ul este înregistrat în Windows Service Control Manager folosind **NSSM (Non-Sucking Service Manager)**:
- Autostart la pornirea sistemului (`SERVICE_AUTO_START`).
- Auto-restart pe crash (delay configurat la 5s).
- Rotație automată a fișierelor de log stdout/stderr la fiecare 5MB.

### 4. Installer Automat Inno Setup
Scriptul `SincronaSetup.iss` generează un pachet de instalare enterprise:
- Solicită drepturi de Administrator (UAC).
- Execută în mod silențios un script PowerShell care adaugă directorul `C:\SincronaBridge` la **Excepțiile Windows Defender** înainte de extragerea binarelor.
- Configurează și pornește serviciul Windows în fundal.

---

## 🛠️ Ghid de Compilare și Build

### Cerințe preliminare
- Python 3.10+ (x64)
- Dependințe: `pip install -r requirements.txt`

### 1. Compilare Binar Nuitka
Rulați din terminal:
```cmd
build.bat
```
sau cu Python:
```bash
python build.py
```
Rezultatul va fi generat în `dist\erpnet_fp_bridge.dist\`.

### 2. Generare Installer Inno Setup
Deschideți `SincronaSetup.iss` în Inno Setup Compiler și compilați, sau rulați din consolă:
```cmd
iscc SincronaSetup.iss
```
Pachetul final `SincronaSetup.exe` va fi generat în `installer_output\`.

---

## 🔒 Semnare Digitală (Code Signing OV / EV)

Pentru o reputație impecabilă în Windows SmartScreen:
```cmd
signtool.exe sign /v /tr http://timestamp.digicert.com /td sha256 /fd sha256 /a "installer_output\SincronaSetup.exe"
```

---

## 📄 Licență & Suport
Proprietar: **Sincrona Auto** (https://sincrona.ro)
