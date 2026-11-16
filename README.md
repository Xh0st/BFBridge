# Sincrona Universal Fiscal Bridge (BFBridge)

**Sincrona Universal Fiscal Bridge** este agentul local de comunicație (IoT Edge Agent) care leagă platforma Cloud [Sincrona](https://sincrona.ro) de casele de marcat fiscale din România.

Rulează pe calculatorul la care este conectată casa de marcat, funcționând ca serviciu Windows sau aplicație de fundal.

---

## 🚀 Caracteristici principale

- **Multi-Driver Fiscal:**
  - **Datecs DUDE / FPrinter:** Protocol nativ fișiere `.inp` cu watchdog pe folderul `OUT`, comenzi native fiscale (48, 49, 53, 56).
  - **Tremol ZFPServer:** Protocol nativ `ServerTremol.exe` (fișiere `bon.*.txt` / `raspuns.txt`, codare Windows-1250 / CP1250).
  - **ErpNet.FP:** REST API HTTP pe `http://127.0.0.1:8001` (driver universal).
- **Descărcare & Export Automat Pachete ANAF (A4200 / XML / p7b):**
  - **Datecs:** Extragere automată direct prin serverul COM ActiveX Datecs DUDE (`dude.CFD_DUDE`), fără a mai necesita deschiderea manuală a aplicației `DudeFrontApplication.exe`.
  - **Tremol:** Declanșare automată prin comanda nativă `L|DDMMYY|DDMMYY|1` executată prin `ServerTremol.exe` / `ZekaFP`.
  - **Planificare lunară automată:** Puntea verifică periodic și descarcă automat extrasele lunii precedente în primele zile ale fiecărei luni (`anaf_monthly_downloads.json`).
  - **Preluare și atașare automată:** Componenta `AnafXmlWatcher` monitorizează folderele (`C:\Datecs\ANAF`, `C:\ServerTremol\ANAF`, `C:\ServerTremol\RapoarteTXT`, `done`) și transmite automat pachetele în Sincrona, atașându-le la Rapoartele Z corespunzătoare.
- **Declanșare la cerere din Cloud și Local:**
  - Direct din aplicația Sincrona: butonul **„Descarcă fișiere ANAF din casă”** din `https://sincrona.ro/invoices?tab=accounting`.
  - Din Panoul Web Admin local (`http://127.0.0.1:8002/`).
- **Conexiune Duală Rezilientă:**
  - Conexiune în timp real prin **WebSocket securizat** (`wss://`).
  - Fallback automat pe **HTTP Polling** la deconectare sau rețele instabile.
- **Mod Offline (Fără Internet):**
  - Profil fiscal salvat local (`offline_profile.json`).
  - Permite vânzarea și emiterea bonurilor din Sincrona locală chiar și în cazul căderii conexiunii la internet (`http://127.0.0.1:8002/local/receipt`).
  - Bonurile emise offline se stochează în `offline_fiscal_journal.json` și sunt sincronizate automat în Cloud la revenirea internetului.
- **Idempotență și Siguranță (USN Journal):**
  - Jurnal local USN (`file_usn_journal.json`): un bon retrimis cu același `uniqueSaleNumber` nu se va retipări pe casa de marcat, prevenind dublarea bonurilor fiscale.
- **Panou de Control Web Local:**
  - Accesibil pe `http://127.0.0.1:8002/` pentru diagnostic, testare driver, testare cloud, schimbare setări și declanșare manuală ANAF.

---

## 📦 Cerințe de sistem

- Windows 10 sau Windows 11 (32-bit sau 64-bit).
- Python 3.10+ (sau executabilul de sine stătător compilat).
- Pentru **Datecs**: Aplicația Datecs DUDE instalată (pentru înregistrarea ActiveX COM `dude.CFD_DUDE` și directoarele `C:\Datecs\In`, `C:\Datecs\Out`, `C:\Datecs\ANAF`).
- Pentru **Tremol**: `ServerTremol.exe` configurat în `C:\ServerTremol` pe portul serial respectiv (ex: `COM9`, `115200`).

---

## ⚙️ Configurare rapidă

Fișierul de configurare `config.json` se generează automat la prima pornire sau se configurează direct din panoul web `http://127.0.0.1:8002/`:

```json
{
  "API_URL": "https://api.sincrona.ro",
  "WS_URL": "",
  "DEVICE_TOKEN": "fp_xxxxxx...",
  "FISCAL_DRIVER": "datecs",
  "DATECS_IN_DIR": "C:\\Datecs\\In",
  "DATECS_OUT_DIR": "C:\\Datecs\\Out",
  "DATECS_FORMAT": "dude",
  "DATECS_COM_PORT": 6,
  "DATECS_BAUDRATE": 115200,
  "DATECS_ANAF_DIR": "C:\\Datecs\\ANAF",
  "ANAF_AUTO_MONTHLY_DOWNLOAD": true,
  "TREMOL_IN_DIR": "C:\\ServerTremol\\Bonuri",
  "TREMOL_OUT_DIR": "C:\\ServerTremol",
  "ANAF_XML_DIRS": [
    "C:\\Datecs\\ANAF",
    "C:\\ServerTremol\\ANAF"
  ],
  "TIMEOUT_SECONDS": 45
}
```

> **Important (Securitate):** Nu urcați niciodată `config.json` sau jurnalele locale pe GitHub! Acestea sunt deja incluse în `.gitignore`.

---

## 🛠️ Rulare & Dezvoltare

### 1. Instalare dependențe
```bash
pip install -r requirements.txt
```

### 2. Pornire manuală
```bash
python erpnet_fp_bridge.py
```
Panoul Web de diagnostic va fi disponibil la: **`http://127.0.0.1:8002/`**.

### 3. Simulator fiscal (pentru testare fără casă fizică)
```bash
python simulator.py
```
Simulatorul emulează o casă de marcat pe `http://127.0.0.1:8001` și generează rapoarte Z și XML-uri de test în folderul `anaf_xml/`.

---

## 🧾 Fluxul Rapoartelor Lunare ANAF (A4200)

1. **Automat:** La fiecare început de lună, puntea calculează intervalul lunii calendaristice precedente și declanșează descărcarea (Datecs via COM ActiveX, Tremol via fișier de comandă `L`).
2. **Manual din Sincrona Cloud:** În `Facturi › Contabilitate › Export SAGA & Rapoarte Z` (`https://sincrona.ro/invoices?tab=accounting`), apăsați butonul **„Descarcă fișiere ANAF din casă”**.
3. **Manual din Puntea Locală:** Pe `http://127.0.0.1:8002/`, selectați datele dorite și apăsați **„⚡ Descarcă Pachete ANAF Acum”**.
4. Fișierele sunt preluate automat de `AnafXmlWatcher` și trimise securizat către Sincrona, fiind atașate automat la Rapoartele Z corespunzătoare.

---

## 📦 Compilare & Creare Instalator Windows

După testarea pe casele de marcat fizice, noul installer Windows poate fi generat cu scriptul inclus:

```bash
# 1. Compilare executabil
python build.py

# sau folosind batch-ul automat:
build.bat

# 2. Generare installer Inno Setup
# Se compilează scriptul SincronaSetup.iss cu Inno Setup Compiler
```

---

## 📄 Licență
Proprietar: **Sincrona Enterprise IoT**  
Toate drepturile rezervate.
