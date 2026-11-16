# Punte locală ErpNet.FP (Sincrona)

Rulează pe stația fiscală, lângă ErpNet.FP (sau simulatorul compatibil).

1. Pornește ErpNet.FP **sau** simulatorul: `python simulator.py` — ascultă pe `http://127.0.0.1:8001`
   (același API ca ErpNet.FP: `POST /printers/{id}/receipt`, `GET /printers/taskinfo?id=`).
2. În Sincrona → Setări → Casă de marcat (pentru filiala selectată): server local `http://127.0.0.1:8001`,
   ID imprimantă, mod (test/live), maparea cote TVA → grupe fiscale. Generează tokenul de dispozitiv.
3. Configurează puntea în `config.json` (lângă script) **sau** din panoul `http://127.0.0.1:8002`:

```json
{ "API_URL": "https://api.sincrona.ro", "DEVICE_TOKEN": "fp_..." }
```

   Alternativ, variabile de mediu: `SINCRONA_API_URL`, `FP_DEVICE_TOKEN`.
4. Pornește: `python erpnet_fp_bridge.py`.

Puntea preia doar joburile filialei/imprimantei asociate tokenului. La refuz, codul și mesajul casei
(ex. `PAPER_OUT`) sunt trimise înapoi în Sincrona și apar la bonul în așteptare.

**Securitate:** tokenul nu este scris în cod. Nu urca `config.json` și fișierele `*.jsonl` / `*.log`
pe GitHub (sunt în `.gitignore`). Dacă un token a ajuns în cod sau în git, revocă-l din Sincrona și generează altul.

## Rapoarte casă de marcat

Din Sincrona → Casă → „Rapoarte casă de marcat”: **Raport X** (informativ), **Raport Z** (închidere zilnică,
obligatoriu) și **Raport lunar memorie fiscală** (`periodreport`, scurt). Puntea apelează
`POST /printers/{id}/xreport | zreport | periodreport` din ErpNet.FP; simulatorul le suportă și el.
Tipul plății pe bon: numerar → `cash`, POS/card → `card`, transfer → `bank` (câmpul `payments[].paymentType`).

## Idempotență (bon tipărit o singură dată)

- Fiecare bon are `taskId` = `uniqueSaleNumber` = `pa-<id încasare>`.
- Dacă ErpNet.FP întoarce `E109` (taskId deja folosit) la o reîncercare, puntea nu retrimite bonul, ci citește
  rezultatul taskului inițial.
- Driverul românesc (fork ErpNet.FP v1.1.0-ro) ține jurnalul `usn-journal`: un bon retrimis cu același
  `uniqueSaleNumber` nu se mai tipărește (întoarce rezultatul salvat). `E409` = bon întrerupt, posibil tipărit —
  verificați ultimul bon pe casă.
- Puntea așteaptă confirmarea casei 80 s (170 s la raportul de memorie fiscală); la depășire raportează
  `TASK_TIMEOUT`, iar „Reîncearcă” din Sincrona este sigur.

## Fișiere XML ANAF ale rapoartelor Z (preluare automată)

Casa de marcat generează, la fiecare raport Z, fișierul XML cerut de OPANAF 146/2018 (semnat de aparat).
Puntea nu îl citește direct din aparat: îl preia din folderul în care îl salvează programul producătorului.

1. Exportați fișierele ANAF din programul producătorului într-un folder fix, de exemplu:
   - Datecs: DUDE (*Datecs Applications\DUDE*), funcția de citire / export a fișierelor ANAF (XML) din casa de marcat → `C:\Datecs\ANAF`;
   - Tremol: utilitarul Tremol pentru fișierele ANAF / jurnalul electronic → de ex. `C:\ServerTremol\ANAF`.
2. În panoul punții (http://127.0.0.1:8002) → „Fișiere XML ANAF” → „Foldere urmărite”, treceți folderul (câte unul pe rând) și salvați.
   (În `config.json`: `"ANAF_XML_DIRS": ["C:\\Datecs\\ANAF"]`.)
3. La fiecare minut puntea trimite fișierele noi (`.xml`, `.p7m`, `.p7s`, `.zip`); Sincrona le potrivește după NUI, numărul Z
   și dată și le atașează la Raportul Z al filialei. Dacă Raportul Z nu e încă în Sincrona, puntea reîncearcă la 10 minute.
   Evidența fișierelor trimise: `anaf_xml_sent.json`.

Simulator: la fiecare raport Z scrie un XML de TEST (nesemnat) în `anaf_xml\` lângă simulator
(`"simulator": {"anaf_xml_dir": "..."}` în config.json; gol = dezactivat). Treceți acel folder la „Foldere urmărite”.

Manual: Facturi › Contabilitate › Rapoarte Z › „Import fișiere ANAF (XML / ZIP)” — se pot selecta mai multe fișiere sau o arhivă.

## Fără internet (mod offline)

Cât e internet, puntea descarcă din Sincrona setările casei (imprimanta, mod test/live, cotele TVA → grupe fiscale)
în `offline_profile.json`. Fără internet, aplicația Sincrona deschisă **pe acest calculator** (pagina „Sincronizare”)
trimite bonul fiscal sau raportul X / Z direct la punte (`http://127.0.0.1:8002/local/*`), care îl tipărește pe casă,
îl notează în `offline_records.json` și îl trimite singură în Sincrona când revine internetul
(`POST /api/device/fiscal-printer/offline-records`). Același bon cerut de două ori nu se tipărește a doua oară.

- Doar paginile `https://*.sincrona.ro` (și `localhost` pentru dezvoltare) pot trimite bonuri la punte; setările punții
  (`/service/*`) se schimbă doar din panoul ei. Panoul ascultă doar pe `127.0.0.1` (`"ADMIN_BIND": "0.0.0.0"` în
  `config.json` pentru acces din rețea).
- Bonurile emise offline apar în Sincrona pe pagina „Sincronizare” și trebuie trecute la încasări cu numărul bonului;
  raportul Z offline se înregistrează automat la Rapoartele Z.
- `offline_profile.json` și `offline_records.json` nu se urcă pe GitHub.
