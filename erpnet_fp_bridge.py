"""
Sincrona Universal Fiscal Bridge (Enterprise IoT Edge Agent)
============================================================
Conectează platforma Sincrona Auto (Cloud) cu casele de marcat fiscale locale.
Drivere suportate:
  - ErpNet.FP (REST HTTP API pe localhost:8001)
  - Datecs DUDE / FPrinter (Protocol fișiere .inp / watchdog pe folderul OUT, codare cp1250)
  - Tremol ZFPServer (Protocol fișiere bon.*.txt / raspuns.txt, codare cp1250)

Panou Admin: http://127.0.0.1:8002/
Conexiune Cloud: WebSocket rezilient (wss://) cu fallback pe HTTP Polling.
"""

import asyncio
import base64
import hashlib
import glob
import http.server
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
from collections import OrderedDict
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests

try:
    import websockets
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False

# ==================== BASE DIRECTORY & LOGGING ====================

def get_base_dir():
    """Returnează directorul de bază al aplicației (funcționează și compilat Nuitka)."""
    if getattr(sys, "frozen", False) or "__compiled__" in globals():
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

BRIDGE_LOG_FILE = os.path.join(LOGS_DIR, "bridge.log")
ERRORS_LOG_FILE = os.path.join(LOGS_DIR, "bridge_errors.jsonl")

# Asigurare codare UTF-8 pentru consolă pe Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Configurare logging în fișier și consolă (dacă există)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.FileHandler(BRIDGE_LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SincronaBridge")

def log_error(source: str, code: str, message: str, extra: dict = None):
    entry = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "code": code,
        "message": message,
        "extra": extra or {}
    }
    logger.error(f"[{source}] {code}: {message} {extra or ''}")
    try:
        with open(ERRORS_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

def read_errors(limit=100):
    if not os.path.exists(ERRORS_LOG_FILE):
        return []
    records = []
    try:
        with open(ERRORS_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        pass
    records.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return records[:limit]

def clear_errors():
    try:
        with open(ERRORS_LOG_FILE, "w", encoding="utf-8") as f:
            f.truncate(0)
    except Exception:
        pass


def get_previous_month_range() -> tuple[str, str]:
    """Returnează (from_date, to_date) în format YYYY-MM-DD pentru luna calendaristică precedentă."""
    now_dt = datetime.now()
    first_cur = now_dt.replace(day=1)
    last_prev = first_cur - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev.strftime("%Y-%m-%d"), last_prev.strftime("%Y-%m-%d")


# ==================== CONFIGURATION MANAGER ====================

DEFAULT_CONFIG = {
    "API_URL": os.environ.get("SINCRONA_API_URL", "https://api.sincrona.ro"),
    "WS_URL": "",
    "DEVICE_TOKEN": os.environ.get("FP_DEVICE_TOKEN", ""),
    "FISCAL_DRIVER": "erpnet",  # 'erpnet', 'datecs', 'tremol'
    "ERPNET_URL": "http://127.0.0.1:8001",
    "DATECS_IN_DIR": r"C:\Datecs\In",
    "DATECS_OUT_DIR": r"C:\Datecs\Out",
    "DATECS_FORMAT": "dude",     # 'dude' sau 'fprinter'
    "DATECS_COM_PORT": 6,
    "DATECS_BAUDRATE": 115200,
    "DATECS_ANAF_DIR": r"C:\Datecs\ANAF",
    "ANAF_AUTO_MONTHLY_DOWNLOAD": True,
    "TREMOL_IN_DIR": r"C:\ServerTremol\Bonuri",
    "TREMOL_OUT_DIR": r"C:\ServerTremol",
    "POLL_FALLBACK": True,
    "TIMEOUT_SECONDS": 45,
    # Fișierele XML ANAF ale rapoartelor Z (OPANAF 146/2018): folderele în care DUDE / utilitarul Tremol /
    # simulatorul le salvează. Puntea le trimite automat în Sincrona și le atașează la Raportul Z.
    "ANAF_XML_DIRS": [],
    "ANAF_XML_SCAN_SECONDS": 60,
    "ANAF_XML_MAX_AGE_DAYS": 45,
}

_cfg_lock = threading.Lock()
_runtime_config = dict(DEFAULT_CONFIG)
_config_version = 0

def load_config():
    global _runtime_config, _config_version
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                cfg.update(loaded)
        except Exception as e:
            logger.error(f"Eroare la citirea {CONFIG_FILE}: {e}")
    with _cfg_lock:
        _runtime_config = cfg
        _config_version += 1
    return cfg

def save_config(updates: dict):
    global _runtime_config, _config_version
    with _cfg_lock:
        _runtime_config.update(updates)
        _config_version += 1
        cfg_copy = dict(_runtime_config)
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg_copy, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Eroare la scrierea {CONFIG_FILE}: {e}")
        raise
    return cfg_copy

def get_config():
    with _cfg_lock:
        return dict(_runtime_config)

load_config()


# ==================== GLOBAL BRIDGE STATE ====================

_state_lock = threading.Lock()
_bridge_state = {
    "connected_ws": False,
    "connected_cloud": False,
    "active_driver": "erpnet",
    "last_poll_ts": None,
    "last_ws_msg_ts": None,
    "last_job_id": None,
    "last_job_status": None,
    "last_error": None,
    "total_jobs_ok": 0,
    "total_jobs_failed": 0,
    "started_at": datetime.now().isoformat(timespec="seconds"),
}

def update_state(**kwargs):
    with _state_lock:
        _bridge_state.update(kwargs)

def get_state():
    with _state_lock:
        st = dict(_bridge_state)
        st["active_driver"] = get_config().get("FISCAL_DRIVER", "erpnet")
        st["config_version"] = _config_version
        return st


# ==================== BASE FISCAL DRIVER INTERFACE ====================

class FiscalException(RuntimeError):
    def __init__(self, code: str, text: str):
        super().__init__(f"[{code}] {text}")
        self.code = code
        self.text = text

class BaseFiscalDriver(ABC):
    @abstractmethod
    def print_receipt(self, job_payload: dict, task_id: str) -> dict:
        """Tipărește un bon fiscal pe baza JSON-ului universal."""
        pass

    @abstractmethod

    def print_report(self, report_type: str, job_payload: dict, task_id: str) -> dict:
        """Tipărește Raport X, Raport Z sau Raport Perioadă."""
        pass

    @abstractmethod
    def test_connection(self) -> dict:
        """Verifică starea conexiunii / disponibilitatea folderelor sau portului."""
        pass


# ==================== ERPNET.FP DRIVER ====================

class ErpNetFPDriver(BaseFiscalDriver):
    def __init__(self, erpnet_url: str):
        self.erpnet_url = erpnet_url.rstrip("/")

    def test_connection(self) -> dict:
        try:
            r = requests.get(f"{self.erpnet_url}/printers", timeout=4)
            r.raise_for_status()
            data = r.json()
            return {"ok": True, "details": data}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def print_receipt(self, job_payload: dict, task_id: str) -> dict:
        printer_id = job_payload.get("printer_id", "default")
        receipt_body = job_payload.get("receipt") or job_payload.get("body") or {}
        local_url = job_payload.get("erpnet_url", self.erpnet_url).rstrip("/")

        tax_num = resolve_buyer_tax_num(job_payload, receipt_body)
        if tax_num:
            recip = receipt_body.setdefault("recipient", {})
            recip.setdefault("name", job_payload.get("buyer_name") or receipt_body.get("buyer_name") or "Client")
            recip.setdefault("identifier", tax_num)
            recip["uniqueRegistrationNumber"] = tax_num
            if not (len(tax_num) == 13 and tax_num.isdigit()):
                if tax_num.startswith("RO"):
                    recip["vatNumber"] = tax_num

        endpoint = job_payload.get("endpoint") or ("invoice" if tax_num else "receipt")
        logger.info(f"[ErpNet] Trimitere către {local_url}/printers/{printer_id}/{endpoint} (taskId={task_id})")

        try:
            response = requests.post(
                f"{local_url}/printers/{printer_id}/{endpoint}",
                params={"asyncTimeout": 0, "taskId": task_id},
                json=receipt_body,
                timeout=20,
            )
        except requests.ConnectionError as e:
            raise FiscalException("ERPNET_OFFLINE", f"ErpNet.FP nu răspunde la {local_url}: {e}")
        except requests.Timeout as e:
            raise FiscalException("ERPNET_TIMEOUT", f"ErpNet.FP a dat timeout la {local_url}: {e}")
        except requests.RequestException as e:
            raise FiscalException("ERPNET_ERROR", str(e))

        if response.status_code == 404:
            raise FiscalException("PRINTER_NOT_FOUND", f"Imprimanta {printer_id} nu există în ErpNet.FP.")

        try:
            result = response.json()
        except ValueError:
            raise FiscalException(f"HTTP_{response.status_code}", f"Răspuns invalid de la ErpNet.FP.")

        codes = {m.get("code") for m in (result.get("messages") or []) if isinstance(m, dict)}
        if "E109" in codes:
            logger.info(f"[ErpNet] Task {task_id} deja existent în ErpNet.FP (idempotență). Preluare status inițial.")
            result = {"taskId": task_id}

        if "taskId" in result:
            task_id_res = result["taskId"]
            max_wait = 80
            for _ in range(max_wait):
                time.sleep(1)
                try:
                    task = requests.get(f"{local_url}/printers/taskinfo", params={"id": task_id_res}, timeout=8)
                    task.raise_for_status()
                    task_info = task.json()
                    if task_info.get("taskStatus") == "finished":
                        result = task_info.get("result", {})
                        break
                except Exception:
                    continue

        if str(result.get("ok", "")).lower() != "true":
            msgs = result.get("messages") or [{}]
            code = msgs[0].get("code", "REJECTED")
            text = msgs[0].get("text", "Comandă refuzată de ErpNet")
            raise FiscalException(code, text)

        return result


    def print_report(self, report_type: str, job_payload: dict, task_id: str) -> dict:
        job_copy = dict(job_payload)
        job_copy["endpoint"] = report_type  # 'xreport', 'zreport', 'periodreport'
        return self.print_receipt(job_copy, task_id)


def sanitize_comment_text(txt: str, max_len: int = 35) -> str:
    """Asigură că liniile de deviz/comandă sunt formatate uniform ca 'Deviz: DV-...'."""
    txt = (txt or "").strip()
    low = txt.lower()
    if low.startswith("comanda lucru:") or low.startswith("comanda de lucru:"):
        raw_num = txt.split(":", 1)[1].strip()
        dv_num = f"DV-{raw_num[3:]}" if raw_num.upper().startswith("CL-") else (f"DV-{raw_num}" if not raw_num.upper().startswith("DV-") else raw_num)
        txt = f"Deviz: {dv_num}"
    elif low.startswith("comanda lucru") or low.startswith("comanda de lucru"):
        txt = re.sub(r"^comanda\s+(?:de\s+)?lucru[:\s]*", "Deviz: ", txt, flags=re.I)
        txt = re.sub(r"Deviz:\s*CL-", "Deviz: DV-", txt, flags=re.I)
    return txt[:max_len]


def resolve_buyer_tax_num(payload: dict, receipt: dict) -> str:
    """Extrage CUI-ul/CNP-ul cumpărătorului:
    - Pentru Persoane Fizice (PF / B2C): CNP (13 cifre) sau '0000000000000' (13 zerouri) — NICIODATĂ prefixat cu RO.
    - Pentru Persoane Juridice (PJ): CUI/CIF-ul cumpărătorului (cu prefixul RO dacă este plătitor de TVA).
    """
    payload = payload or {}
    receipt = receipt or {}
    recipient = receipt.get("recipient") or payload.get("recipient") or {}
    customer = receipt.get("customer") or payload.get("customer") or {}
    tax_num = str(
        payload.get("client_cui")
        or receipt.get("client_cui")
        or payload.get("buyer_tax_id")
        or receipt.get("buyer_tax_id")
        or recipient.get("uniqueRegistrationNumber")
        or recipient.get("identifier")
        or recipient.get("vatNumber")
        or customer.get("tax_number")
        or ""
    ).strip().upper()
    if not tax_num:
        return ""

    digits_only = re.sub(r"\D", "", tax_num)

    # Persoane Fizice (PF / B2C): 13 cifre (CNP sau 0000000000000)
    # În România, CUI PJ are maxim 10 cifre; CNP are exact 13 cifre.
    if len(digits_only) == 13:
        return digits_only

    # Persoane Juridice (PJ):
    # Dacă este marcat ca plătitor de TVA (sau recipient/payload conține vatNumber / is_vat_payer), adăugăm RO
    is_vat_payer = (
        tax_num.startswith("RO")
        or bool(payload.get("is_vat_payer"))
        or bool(receipt.get("is_vat_payer"))
        or bool(recipient.get("is_vat_payer"))
        or bool(recipient.get("vat_payer"))
        or bool(str(recipient.get("vatNumber") or "").upper().startswith("RO"))
    )
    digits = tax_num[2:] if tax_num.startswith("RO") else tax_num
    return f"RO{digits}" if is_vat_payer else digits


# ==================== DATECS (DUDE / FPrinter) DRIVER ====================

class DatecsDriver(BaseFiscalDriver):
    """
    Driver Datecs cu suport pentru protocolul DUDE / FPrinter prin fișiere .inp.
    Encodare: Windows-1250 (cp1250).
    Sincronizare: scriere atomică (.tmp -> .inp) și monitorizare folder OUT prin watchdog.
    """
    def __init__(self, in_dir: str, out_dir: str, format_mode: str = "dude", timeout_s: int = 40):
        self.in_dir = os.path.abspath(in_dir)
        self.out_dir = os.path.abspath(out_dir)
        self.format_mode = format_mode.lower()
        self.timeout_s = timeout_s
        os.makedirs(self.in_dir, exist_ok=True)
        os.makedirs(self.out_dir, exist_ok=True)
        self._ensure_server_running()

    def _ensure_server_running(self):
        """
        Verifică dacă serviciul corespunzător (DUDE / FPrinter / simulator) rulează în background.
        Dacă nu rulează, îl pornește automat înainte de tipărirea bonului sau raportului.
        """
        try:
            r = subprocess.run(["tasklist"], capture_output=True, text=True)
            tasks_lower = r.stdout.lower()

            if self.format_mode == "fprinter":
                # Verificare proces FPrinter (ex: FPrinter.exe sau FPrintServer.exe)
                if "fprinter.exe" not in tasks_lower and "fprint" not in tasks_lower:
                    logger.info("[Datecs] FPrinter nu rulează în background; căutare și pornire automată...")
                    candidates = [
                        os.path.join(self.out_dir, "FPrinter.exe"),
                        os.path.join(self.in_dir, "FPrinter.exe"),
                        r"C:\Program Files (x86)\Datecs Applications\FPrinter\FPrinter.exe",
                        r"C:\Datecs\FPrinter.exe",
                    ]
                    for cand in candidates:
                        if os.path.exists(cand):
                            subprocess.Popen([cand], cwd=os.path.dirname(cand))
                            logger.info(f"[Datecs] FPrinter pornit cu succes din: {cand}")
                            time.sleep(2)
                            break
            else:
                # Verificare proces DUDE (dude.exe sau dude.py în mod simulator)
                is_dude_running = "dude.exe" in tasks_lower
                # Verificare dacă rulează dude.py
                if not is_dude_running:
                    cmd_check = subprocess.run(
                        ["wmic", "process", "where", "CommandLine like '%dude.py%'", "get", "ProcessId"],
                        capture_output=True, text=True
                    )
                    if any(line.strip().isdigit() for line in cmd_check.stdout.splitlines()):
                        is_dude_running = True

                if not is_dude_running:
                    logger.info("[Datecs] DUDE nu rulează în background; pornire automată...")
                    candidates = [
                        r"C:\Program Files (x86)\Datecs Applications\DUDE\dude.exe",
                        os.path.join(self.out_dir, "dude.exe"),
                        os.path.join(BASE_DIR, "dude.py"),
                    ]
                    for cand in candidates:
                        if os.path.exists(cand):
                            if cand.endswith(".py"):
                                subprocess.Popen([sys.executable, cand], cwd=BASE_DIR)
                            else:
                                subprocess.Popen([cand], cwd=os.path.dirname(cand))
                            logger.info(f"[Datecs] Serviciu Datecs DUDE pornit cu succes din: {cand}")
                            time.sleep(2)
                            break
        except Exception as e:
            logger.warning(f"[Datecs] Nu s-a putut verifica/porni serviciul Datecs: {e}")

    def test_connection(self) -> dict:
        in_ok = os.path.isdir(self.in_dir) and os.access(self.in_dir, os.W_OK)
        out_ok = os.path.isdir(self.out_dir) and os.access(self.out_dir, os.R_OK)
        probe_ok = False
        probe_file = os.path.join(self.in_dir, f".probe_{int(time.time())}.tmp")
        try:
            with open(probe_file, "w") as f:
                f.write("probe")
            if os.path.exists(probe_file):
                os.remove(probe_file)
                probe_ok = True
        except Exception:
            probe_ok = False

        return {
            "ok": in_ok and out_ok and probe_ok,
            "in_dir": self.in_dir,
            "in_dir_writable": in_ok and probe_ok,
            "out_dir": self.out_dir,
            "out_dir_readable": out_ok,
            "format": self.format_mode
        }

    def _compute_current_z_totals(self, is_z: bool = False) -> tuple:
        """
        Calculează telemetria reală a zilei (nr. documente, total, card, numerar, nr_z)
        direct din fișierele bonurilor arhivate în folderul Z curent.
        """
        done_dir = os.path.join(self.in_dir, "done")
        if not os.path.exists(done_dir):
            return 0, 0.0, 0.0, 0.0, ""

        z_folders = []
        for d in os.listdir(done_dir):
            p = os.path.join(done_dir, d)
            if os.path.isdir(p) and re.match(r'^[Zz]\d{4}$', d):
                z_folders.append((os.path.getmtime(p), p, d))

        if not z_folders:
            return 0, 0.0, 0.0, 0.0, ""

        z_folders.sort(key=lambda x: x[0], reverse=True)
        target_dir = z_folders[0][1]
        target_name = z_folders[0][2]

        docs = 0
        total = 0.0
        cash = 0.0
        card = 0.0
        try:
            for f in os.listdir(target_dir):
                if f.endswith('.inp') or f.endswith('.out') or f.endswith('.txt'):
                    docs += 1
        except Exception:
            pass

        mz = re.search(r'\d+', target_name)
        current_z_num = str(int(mz.group(0))) if mz else ""
        return docs, total, cash, card, current_z_num

    def _compute_period_totals(self, from_date: str, to_date: str) -> tuple:
        """Calculează sumele din toate bonurile arhivate în intervalul de date solicitat."""
        return 0, 0.0

    def _wait_for_response(self, base_name: str) -> str:
        """Așteaptă apariția fișierului de răspuns (.out, .err sau .ans) în out_dir."""
        target_patterns = [
            os.path.join(self.out_dir, f"{base_name}.out"),
            os.path.join(self.out_dir, f"{base_name}.err"),
            os.path.join(self.out_dir, f"{base_name}.ans"),
            os.path.join(self.out_dir, f"{base_name}.txt"),
        ]
        
        deadline = time.time() + self.timeout_s
        found_file = None

        while time.time() < deadline:
            for p in target_patterns:
                if os.path.exists(p) and os.path.getsize(p) > 0:
                    found_file = p
                    break
            if found_file:
                break
            time.sleep(0.3)

        if not found_file:
            # Caută orice fișier .out/.err modificat recent
            raise FiscalException("DATECS_TIMEOUT", f"Fișierul {base_name}.inp a fost creat în {self.in_dir}, dar nu s-a primit fișier de răspuns în {self.out_dir} în {self.timeout_s}s. Asigurați-vă că driverul Datecs (DUDE / FPrinter sau simulatorul) rulează.")

        # Așteaptă scrierea completă
        time.sleep(0.2)
        try:
            with open(found_file, "r", encoding="cp1250", errors="replace") as f:
                content = f.read().strip()
            # Curățare fișier procesat
            try:
                os.remove(found_file)
            except OSError:
                pass
            return content
        except Exception as e:
            raise FiscalException("DATECS_READ_ERROR", f"Nu am putut citi răspunsul {found_file}: {e}")

    def _build_dude_lines(self, receipt: dict, job_payload: dict = None) -> list:
        """Construiește comenzi în format Datecs DUDE nativ (48, 49, 53, 56)."""
        lines = []
        payload = job_payload or {}
        tax_num = resolve_buyer_tax_num(payload, receipt)

        # Comanda 48: Deschidere bon fiscal (I + CUI cu prefix RO dacă este plătitor de TVA)
        if tax_num:
            lines.append(f"48,1\t0001\t1\tI\t{tax_num}\t")
        else:
            lines.append("48,1\t0001\t1\t\t\t")

        # Comanda 49: Înregistrare articole
        items = receipt.get("items") or receipt.get("lines") or []
        for it in items:
            # Linii de text liber/comentariu (ex: referință deviz de lucru)
            if it.get("type") == "comment" or (it.get("unitPrice") is None and it.get("price") is None):
                raw_txt = (it.get("text") or it.get("name") or it.get("description") or "").replace("\t", " ")
                txt = sanitize_comment_text(raw_txt, max_len=36)
                if txt:
                    lines.append(f"54,{txt}\t")
                continue

            name = (it.get("text") or it.get("name") or it.get("description") or "Articol").strip()[:36].replace("\t", " ")
            vat = it.get("taxGroup") or it.get("vat_group") or it.get("vatGroup") or 1
            raw_price = it.get("unitPrice") if it.get("unitPrice") is not None else it.get("price", 0)
            price = f"{float(raw_price):.2f}"
            qty = f"{float(it.get('quantity', 1)):.3f}"
            dep = it.get("department", 1)
            um = (it.get("unit") or "BUC.")[:4]
            lines.append(f"49,{name}\t{vat}\t{price}\t{qty}\t\t\t{dep}\t{um}\t")

        # Comanda 53: Înregistrare plăți
        # Mapare tip plată Datecs DUDE (conf. FP_Protocol_RO.pdf pag. 22):
        # 0=Numerar, 1=Card, 2=Tichete Masă, 3=Tichete Valorice, 4=Voucher, 5=Credit, 6=Plată modernă, 8=Alte/Bank
        payment_map = {
            "cash": 0,
            "numerar": 0,
            "card": 1,
            "pos": 1,
            "ticket": 2,
            "tichet": 2,
            "tichete": 2,
            "ticket_val": 3,
            "voucher": 4,
            "credit": 5,
            "modern": 6,
            "bank": 8,
            "virament": 8,
            "op": 8,
            "other": 8,
            "alta": 8,
            "alte": 8,
        }
        payments = receipt.get("payments") or []
        if not payments:
            payments = [{"paymentType": "cash", "amount": 0}]

        for p in payments:
            ptype_raw = str(p.get("paymentType", "cash")).lower()
            ptype = payment_map.get(ptype_raw, 0)
            amt_num = float(p.get("amount", 0))
            if amt_num > 0:
                amount = f"{amt_num:.2f}"
                lines.append(f"53,{ptype}\t{amount}\t")
            else:
                # În DUDE dacă lăsăm suma goală '53,0\t\t', închide restul cu numerar
                lines.append(f"53,{ptype}\t\t")

        # Comanda 56: Închidere bon fiscal
        lines.append("56")
        return lines

    def _build_fprinter_lines(self, receipt: dict, job_payload: dict = None) -> list:
        """Construiește comenzi în format Datecs FPrinter clasic (S,1,... P,1,... T,1,...)."""
        lines = []
        payload = job_payload or {}
        tax_num = resolve_buyer_tax_num(payload, receipt)
        if tax_num:
            lines.append(f"CF,1,______,_,__;{tax_num};")

        items = receipt.get("items") or receipt.get("lines") or []
        for it in items:
            # Comentariu / text informativ (ex. deviz de lucru)
            if it.get("type") == "comment" or (it.get("unitPrice") is None and it.get("price") is None):
                raw_txt = (it.get("text") or it.get("name") or it.get("description") or "").replace(";", " ")
                txt = sanitize_comment_text(raw_txt, max_len=32)
                if txt:
                    lines.append(f"I,1,______,_,__;{txt};")
                continue
            name = (it.get("text") or it.get("name") or it.get("description") or "Articol").strip()[:32].replace(";", " ")
            raw_price = it.get("unitPrice") if it.get("unitPrice") is not None else it.get("price", 0)
            price = f"{float(raw_price):.2f}"
            qty = f"{float(it.get('quantity', 1)):.3f}"
            dep = it.get("department", 1)
            vat = it.get("taxGroup") or it.get("vat_group") or it.get("vatGroup") or 1
            um = (it.get("unit") or "buc")[:3]
            lines.append(f"S,1,______,_,__;{name};{price};{qty};{dep};{vat};{um};")

        payments = receipt.get("payments") or [{"paymentType": "cash", "amount": 0}]
        payment_map = {"cash": "0", "card": "1", "pos": "1", "credit": "2", "bank": "3"}
        for p in payments:
            ptype = payment_map.get(str(p.get("paymentType", "cash")).lower(), "0")
            amt = f"{float(p.get('amount', 0)):.2f}"
            lines.append(f"P,1,______,_,__;{ptype};{amt};")

        lines.append("T,1,______,_,__;")
        return lines

    def recover_receipt(self, file_base: str):
        """Încercare anterioară cu rezultat necunoscut (timeout / punte repornită).
        Întoarce: ("done", rezultat) | ("not_sent", None) — fișierul nu a fost preluat, îl retragem |
        ("failed", text) — casa a respins | ("unknown", None) — preluat, fără răspuns."""
        base_name = f"bon_{file_base}"
        inp_file = os.path.join(self.in_dir, f"{base_name}.inp")
        if os.path.exists(inp_file):
            try:
                # dacă DUDE / FPrinter îl are deschis, redenumirea eșuează pe Windows -> încă în lucru
                os.replace(inp_file, inp_file + ".anulat")
                return "not_sent", None
            except OSError:
                return "unknown", None
        for ext in ("out", "err", "ans", "txt"):
            path = os.path.join(self.out_dir, f"{base_name}.{ext}")
            if os.path.exists(path) and os.path.getsize(path) > 0:
                try:
                    with open(path, "r", encoding="cp1250", errors="replace") as f:
                        res_text = f.read().strip()
                    os.remove(path)
                except OSError:
                    return "unknown", None
                try:
                    return "done", self._result_from_response(res_text, base_name, file_base)
                except FiscalException as exc:
                    return "failed", exc.text
        return "unknown", None

    def _result_from_response(self, res_text: str, base_name: str, task_id: str) -> dict:
        upper_res = res_text.upper()
        if "ERR" in upper_res or "ERROR" in upper_res or res_text.startswith("-1"):
            raise FiscalException("DATECS_REJECTED", f"Datecs a raportat eroare: {res_text}")
        receipt_num = ""
        parts = res_text.split(";")
        if len(parts) >= 4:
            if parts[3].strip().isdigit():
                receipt_num = parts[3].strip()
            else:
                for p in parts:
                    clean_p = p.strip()
                    if clean_p.isdigit() and len(clean_p) >= 1:
                        receipt_num = clean_p
                        break
        return {
            "ok": True,
            "result": res_text,
            "file": base_name,
            "receiptNumber": receipt_num,
            "receipt_number": receipt_num,
            "taskId": task_id,
        }

    def print_receipt(self, job_payload: dict, task_id: str, file_base: str = "") -> dict:
        self._ensure_server_running()
        receipt = job_payload.get("receipt") or job_payload.get("body") or {}
        # nume determinist pe încercare (jurnalul USN din FiscalRouter): o reîncercare nu scrie un al doilea bon
        base_name = f"bon_{file_base}" if file_base else f"bon_{int(time.time())}_{task_id[-8:]}"

        if self.format_mode == "fprinter":
            cmd_lines = self._build_fprinter_lines(receipt, job_payload)
        else:
            cmd_lines = self._build_dude_lines(receipt, job_payload)

        content = "\r\n".join(cmd_lines) + "\r\n"
        tmp_file = os.path.join(self.in_dir, f"{base_name}.tmp")
        inp_file = os.path.join(self.in_dir, f"{base_name}.inp")

        logger.info(f"[Datecs] Scriere fișier {inp_file} ({len(cmd_lines)} linii, cp1250)")
        with open(tmp_file, "w", newline="", encoding="cp1250", errors="replace") as f:
            f.write(content)
        # Redenumire atomică pentru a preveni citirea parțială
        if os.path.exists(inp_file):
            try:
                os.remove(inp_file)
            except OSError:
                pass
        os.replace(tmp_file, inp_file)

        # Așteaptă confirmarea în folderul OUT
        res_text = self._wait_for_response(base_name)
        logger.info(f"[Datecs] Răspuns primit pentru {base_name}: {res_text[:100]}")
        result = self._result_from_response(res_text, base_name, task_id)

        # Curățare fișier procesat .inp din folderul IN
        try:
            if os.path.exists(inp_file):
                os.remove(inp_file)
        except OSError:
            pass
        return result

    def _download_anaf_via_dude(self, from_date: str = "", to_date: str = "") -> dict:
        r"""
        Descarcă automat pachetele lunare ANAF (.p7b / .xml) direct prin serverul COM Datecs DUDE (dude.CFD_DUDE)
        fără a fi nevoie de deschiderea manuală a aplicației DudeFrontApplication.exe.
        Salvează fișierele în folderul configurat (implicit C:\Datecs\ANAF).
        """
        if not from_date or not to_date:
            from_date, to_date = get_previous_month_range()

        try:
            import win32com.client
            cfg = get_config()
            com_port = int(cfg.get("DATECS_COM_PORT", 6))
            baud_rate = int(cfg.get("DATECS_BAUDRATE", 115200))
            anaf_dir = cfg.get("DATECS_ANAF_DIR", r"C:\Datecs\ANAF")
            os.makedirs(anaf_dir, exist_ok=True)

            logger.info(f"[ANAF DUDE] Inițializare server COM CFD_DUDE pentru descărcare automată ({from_date} -> {to_date})...")
            dude_obj = win32com.client.Dispatch("dude.CFD_DUDE")

            # Conversie format dată în DD-MM-YY HH:mm:ss
            def _to_dude_dt(d_str: str, is_end: bool) -> str:
                parts = d_str.strip().split("-")
                time_suffix = "23:59:59" if is_end else "00:00:00"
                if len(parts) == 3 and len(parts[0]) == 4:
                    return f"{parts[2]}-{parts[1]}-{parts[0][2:]} {time_suffix}"
                return f"{d_str} {time_suffix}"

            start_dt = _to_dude_dt(from_date, False)
            end_dt = _to_dude_dt(to_date, True)

            dude_obj.set_TransportType(0)  # 0 = RS232 / COM
            dude_obj.set_RS232(com_port, baud_rate)
            err = dude_obj.open_Connection()
            if err != 0:
                err_msg = getattr(dude_obj, "lastError_Message", "") or f"cod eroare {err}"
                logger.warning(f"[ANAF DUDE] Nu s-a putut deschide portul COM{com_port} prin DUDE COM: {err_msg}")
                return {"ok": False, "error": f"Eroare port COM{com_port}: {err_msg}"}

            try:
                dude_obj.set_Download_Path(anaf_dir)
                dude_obj.DateRange_StartValue = start_dt
                dude_obj.DateRange_EndValue = end_dt
                logger.info(f"[ANAF DUDE] Declanșare descărcare pachete ANAF în: {anaf_dir} ({start_dt} – {end_dt})...")
                res_code = dude_obj.download_ANAF_DTRange()
                logger.info(f"[ANAF DUDE] Finalizat descărcare ANAF cu cod: {res_code}")
                return {"ok": True, "code": res_code, "dir": anaf_dir, "from_date": from_date, "to_date": to_date}
            finally:
                try:
                    dude_obj.close_Connection()
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"[ANAF DUDE] Excepție apel COM CFD_DUDE: {e}")
            return {"ok": False, "error": str(e)}

    def print_report(self, report_type: str, job_payload: dict, task_id: str) -> dict:
        self._ensure_server_running()
        low_type = report_type.lower().replace("_", "")
        base_name = f"rep_{low_type}_{int(time.time())}_{task_id[-6:]}"

        body = job_payload.get("body", {})
        from_date = body.get("fromDate", "")[:10]
        to_date = body.get("toDate", "")[:10]

        if "period" in low_type and from_date and to_date:
            f_p = from_date.split("-")
            t_p = to_date.split("-")
            if len(f_p) == 3 and len(t_p) == 3:
                det = "1" if body.get("type") == "detailed" else "0"
                if self.format_mode == "fprinter":
                    # FPrinter: format DDMMYY
                    d_from = f"{f_p[2]}{f_p[1]}{f_p[0][2:]}"
                    d_to = f"{t_p[2]}{t_p[1]}{t_p[0][2:]}"
                    cmd = f"M,1,______,_,__;{d_from};{d_to};{det};\r\n"
                else:
                    # DUDE conform avizare (COM 94): 94,{det}\tDD-MM-YY\tDD-MM-YY\t
                    d_from = f"{f_p[2]}-{f_p[1]}-{f_p[0][2:]}"
                    d_to = f"{t_p[2]}-{t_p[1]}-{t_p[0][2:]}"
                    cmd = f"94,{det}\t{d_from}\t{d_to}\t\r\n"
            else:
                cmd = "Z,1,______,_,__;\r\n" if self.format_mode == "fprinter" else "69,Z\t\r\n"
        else:
            rep_char = "Z" if "z" in low_type else "X"
            if self.format_mode == "fprinter":
                cmd = f"{rep_char},1,______,_,__;\r\n"
            else:
                cmd = f"69,{rep_char}\t\r\n"

        tmp_file = os.path.join(self.in_dir, f"{base_name}.tmp")
        inp_file = os.path.join(self.in_dir, f"{base_name}.inp")

        with open(tmp_file, "w", newline="", encoding="cp1250", errors="replace") as f:
            f.write(cmd)

        if os.path.exists(inp_file):
            try:
                os.remove(inp_file)
            except OSError:
                pass
        os.replace(tmp_file, inp_file)

        res_text = self._wait_for_response(base_name)

        try:
            if os.path.exists(inp_file):
                os.remove(inp_file)
        except OSError:
            pass

        # Parsare răspuns Datecs DUDE / FPrinter pentru rapoarte fiscale
        # Format tipic DUDE: 0;0;0;[NumarZ];[NumarBonuri];[TotalGeneral];[TotalCard];[TotalNumerar];...
        parts = [p.strip() for p in res_text.split(";")]
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        report_num = ""
        doc_count = "0"
        total_amt = "0.00"
        card_amt = "0.00"
        cash_amt = "0.00"

        if len(parts) >= 4 and parts[3].isdigit():
            report_num = parts[3]
        if len(parts) >= 5 and parts[4].isdigit():
            doc_count = parts[4]
        if len(parts) >= 6:
            try:
                total_amt = f"{float(parts[5]):.2f}"
            except Exception:
                pass
        if len(parts) >= 7:
            try:
                card_amt = f"{float(parts[6]):.2f}"
            except Exception:
                pass
        if len(parts) >= 8:
            try:
                cash_amt = f"{float(parts[7]):.2f}"
            except Exception:
                pass

        if "z" in low_type and not report_num:
            done_dir = os.path.join(self.in_dir, "done")
            if os.path.exists(done_dir):
                for r, _, fs in os.walk(done_dir):
                    for f in fs:
                        if file_name.replace(".txt", "") in f:
                            mz = re.search(r'[Zz](\d{4})', r)
                            if mz and mz.group(1).isdigit():
                                report_num = str(int(mz.group(1)))
                                break

        # Dacă ServerTremol este în mod fișier și nu a returnat sumele în răspuns,
        # calculăm telemetria reală direct din bonurile arhivate în folderul Z
        is_z_report = "z" in low_type
        if (total_amt == "0.00" or doc_count == "0") and ("x" in low_type or is_z_report):
            c_docs, c_total, c_cash, c_card, c_z = self._compute_current_z_totals(is_z=is_z_report)
            if c_docs > 0:
                doc_count = str(c_docs)
                total_amt = f"{c_total:.2f}"
                cash_amt = f"{c_cash:.2f}"
                card_amt = f"{c_card:.2f}"
            if c_z:
                report_num = c_z

        if is_z_report:
            summary = f"Z nr. {report_num or '1'} - Raport Z {now_str}: {doc_count} documente, total {total_amt} RON (card: {card_amt} RON, cash: {cash_amt} RON)"
        elif "x" in low_type:
            summary = f"Raport X {now_str}: {doc_count} documente, total {total_amt} RON (card: {card_amt} RON, cash: {cash_amt} RON)"
        elif "period" in low_type:
            # Descărcare automată pachete ANAF în folderul C:\Datecs\ANAF (dacă e raport detaliat)
            anaf_res = None
            if body.get("type") == "detailed" or "anaf" in low_type:
                anaf_res = self._download_anaf_via_dude(from_date, to_date)

            if hasattr(self, "_compute_period_totals"):
                p_docs, p_tot = self._compute_period_totals(from_date, to_date)
            else:
                p_docs, p_tot = 0, 0.0

            if anaf_res and anaf_res.get("ok"):
                summary = f"Raport memorie fiscală ({from_date} – {to_date}) descărcat cu succes în {anaf_res.get('dir')}!"
            elif p_docs > 0:
                summary = f"Raport memorie fiscală ({from_date} – {to_date}): {p_docs} documente, total {p_tot:.2f} RON"
            else:
                summary = f"Raport memorie fiscală ({from_date} – {to_date}) tipărit cu succes pe casa de marcat"
        else:
            summary = f"Raport {report_type.upper()} {now_str} executat cu succes"

        return {
            "ok": True,
            "result": res_text,
            "raw_response": res_text,
            "report": report_type,
            "reportNumber": report_num,
            "report_number": report_num,
            "total": total_amt,
            "messages": [
                {
                    "type": "info",
                    "text": summary,
                }
            ],
            "taskId": task_id,
        }


# ==================== TREMOL ZFPSERVER DRIVER ====================

class TremolDriver(BaseFiscalDriver):
    """
    Driver Tremol ZFPServer cu protocol fișiere text (ServerDir=Bonuri / raspuns.txt).
    Encodare: Windows-1250 (cp1250).
    """
    def __init__(self, in_dir: str, out_dir: str, timeout_s: int = 40):
        self.in_dir = os.path.abspath(in_dir)
        self.out_dir = os.path.abspath(out_dir)
        self.timeout_s = timeout_s
        os.makedirs(self.in_dir, exist_ok=True)
        os.makedirs(self.out_dir, exist_ok=True)
        self._ensure_server_running()

    def _ensure_server_running(self):
        """Asigură pornirea automată a ServerTremol.exe dacă a fost oprit sau a picat."""
        try:
            r = subprocess.run(["tasklist", "/FI", "IMAGENAME eq ServerTremol.dat"], capture_output=True, text=True)
            if "ServerTremol.dat" not in r.stdout:
                logger.info("[Tremol] ServerTremol.exe nu rulează; pornire automată...")
                exe_path = os.path.join(self.out_dir, "ServerTremol.exe")
                if os.path.exists(exe_path):
                    subprocess.Popen([exe_path], cwd=self.out_dir)
                    time.sleep(2)
        except Exception as e:
            logger.warning(f"[Tremol] Nu s-a putut verifica/porni ServerTremol: {e}")

    def _download_anaf_via_tremol(self, from_date: str = "", to_date: str = "") -> dict:
        r"""
        Declanșează exportul automat al pachetelor lunare ANAF XML prin ServerTremol folosind comanda nativă L.
        Format oficial Tremol (DocumentatieFormatServerTremol.txt):
          L|<data inceput export xml>[DDMMYY]|<data sfasit export xml>[DDMMYY]|<salvare suport extern>[0=USB, 1=SD Card]
        Fișier drop-in: C:\ServerTremol\Bonuri\bon.xml.raport_<timestamp>.txt
        """
        if not from_date or not to_date:
            from_date, to_date = get_previous_month_range()

        self._ensure_server_running()

        def _to_tremol_dt(d_str: str) -> str:
            parts = d_str.strip().split("-")
            if len(parts) == 3 and len(parts[0]) == 4:
                return f"{parts[2]}{parts[1]}{parts[0][2:]}"
            return d_str

        d_from = _to_tremol_dt(from_date)
        d_to = _to_tremol_dt(to_date)
        # 1 = salvare pe SD Card/memorie internă a casei de marcat
        cmd = f"L|{d_from}|{d_to}|1\r\n"
        file_base = f"bon.xml.raport_{int(time.time())}"
        target_path = os.path.join(self.in_dir, f"{file_base}.txt")
        tmp_path = os.path.join(self.in_dir, f"{file_base}.tmp")

        logger.info(f"[ANAF Tremol] Trimitere comandă export XML ANAF ({from_date} -> {to_date}): {cmd.strip()} în {target_path}...")
        try:
            with open(tmp_path, "w", newline="", encoding="cp1250", errors="replace") as f:
                f.write(cmd)
            if os.path.exists(target_path):
                try:
                    os.remove(target_path)
                except OSError:
                    pass
            os.replace(tmp_path, target_path)

            res = self._wait_for_response(file_base)
            logger.info(f"[ANAF Tremol] Răspuns ServerTremol export XML: {res}")
            return {
                "ok": True,
                "result": res,
                "driver": "tremol",
                "from_date": from_date,
                "to_date": to_date
            }
        except Exception as e:
            logger.error(f"[ANAF Tremol] Eroare la trimiterea comenzii de export XML: {e}")
            return {"ok": False, "error": str(e), "driver": "tremol"}

    def test_connection(self) -> dict:
        in_ok = os.path.isdir(self.in_dir) and os.access(self.in_dir, os.W_OK)
        out_ok = os.path.isdir(self.out_dir) and os.access(self.out_dir, os.R_OK)
        probe_ok = False
        probe_file = os.path.join(self.in_dir, f".probe_{int(time.time())}.tmp")
        try:
            with open(probe_file, "w") as f:
                f.write("probe")
            if os.path.exists(probe_file):
                os.remove(probe_file)
                probe_ok = True
        except Exception:
            probe_ok = False

        return {
            "ok": in_ok and out_ok and probe_ok,
            "in_dir": self.in_dir,
            "in_dir_writable": in_ok and probe_ok,
            "out_dir": self.out_dir,
            "out_dir_readable": out_ok
        }

    def _compute_current_z_totals(self, is_z: bool = False) -> tuple:
        """
        Calculează telemetria reală a zilei (nr. documente, total, card, numerar, nr_z)
        direct din fișierele bonurilor arhivate în folderul Z curent de către ServerTremol.
        Pentru Raport Z: dacă noul folder Z este gol (creat la închidere),
        datele sunt citite din folderul Z anterior (cel care a fost închis).
        """
        done_dir = os.path.join(self.in_dir, "done")
        if not os.path.exists(done_dir):
            return 0, 0.0, 0.0, 0.0, ""

        z_folders = []
        for d in os.listdir(done_dir):
            p = os.path.join(done_dir, d)
            if os.path.isdir(p) and re.match(r'^[Zz]\d{4}$', d):
                z_folders.append((os.path.getmtime(p), p, d))

        if not z_folders:
            return 0, 0.0, 0.0, 0.0, ""

        z_folders.sort(key=lambda x: x[0], reverse=True)

        target_dir = z_folders[0][1]
        target_name = z_folders[0][2]

        def _count_receipts(directory):
            docs = 0
            cash = 0.0
            card = 0.0
            for f in os.listdir(directory):
                if re.search(r'bon\.(?![a-z]\.raport).+?\[done\]\[\d+\]\.txt', f):
                    docs += 1
                    filepath = os.path.join(directory, f)
                    try:
                        with open(filepath, 'r', encoding='cp1250', errors='replace') as rf:
                            for line in rf:
                                line = line.strip()
                                if line.startswith('P|'):
                                    parts = line.split('|')
                                    if len(parts) >= 3:
                                        ptype = parts[1].strip()
                                        try:
                                            amt = float(parts[2].strip())
                                            if ptype == '1':
                                                card += amt
                                            else:
                                                cash += amt
                                        except ValueError:
                                            pass
                    except Exception:
                        pass
            return docs, cash + card, cash, card

        docs, total, cash, card = _count_receipts(target_dir)

        # La Raport Z, dacă folderul cel mai recent are 0 bonuri (creat abia acum la Z),
        # datele bonurilor închise sunt în Z-ul anterior
        if is_z and docs == 0 and len(z_folders) > 1:
            p_docs, p_total, p_cash, p_card = _count_receipts(z_folders[1][1])
            if p_docs > 0:
                docs, total, cash, card = p_docs, p_total, p_cash, p_card
                target_name = z_folders[1][2]

        mz = re.search(r'\d+', target_name)
        current_z_num = str(int(mz.group(0))) if mz else ""
        return docs, total, cash, card, current_z_num

    def _compute_period_totals(self, from_date: str, to_date: str) -> tuple:
        """Calculează sumele din toate bonurile arhivate în intervalul de date solicitat."""
        done_dir = os.path.join(self.in_dir, "done")
        if not os.path.exists(done_dir):
            return 0, 0.0

        f_s = from_date.replace("-", "")[:8]
        t_s = to_date.replace("-", "")[:8]

        doc_count = 0
        total_amt = 0.0

        for r, _, fs in os.walk(done_dir):
            for f in fs:
                if re.search(r'bon\.(?![a-z]\.raport).+?\[done\]\[\d+\]\.txt', f):
                    m = re.search(r'\[(\d{8})\d*\]', f)
                    if m:
                        file_dt = m.group(1)
                        if f_s <= file_dt <= t_s:
                            doc_count += 1
                            filepath = os.path.join(r, f)
                            try:
                                with open(filepath, 'r', encoding='cp1250', errors='replace') as rf:
                                    for line in rf:
                                        if line.startswith('P|'):
                                            parts = line.strip().split('|')
                                            if len(parts) >= 3:
                                                try:
                                                    total_amt += float(parts[2].strip())
                                                except ValueError:
                                                    pass
                            except Exception:
                                pass
        return doc_count, total_amt

    def _wait_for_response(self, job_name: str) -> str:
        """
        Monitorizează procesarea fișierului de către ServerTremol.
        ServerTremol mută fișierele executate în Bonuri/done, Bonuri/error sau Bonuri/bad
        și scrie răspunsul în raspuns.txt / status.log.
        """
        done_file = os.path.join(self.in_dir, "done", f"{job_name}.txt")
        err_file = os.path.join(self.in_dir, "error", f"{job_name}.txt")
        bad_file = os.path.join(self.in_dir, "bad", f"{job_name}.txt")

        candidates = [
            os.path.join(self.out_dir, "raspuns.txt"),
            os.path.join(self.out_dir, f"{job_name}.ans"),
            os.path.join(self.in_dir, "raspuns.txt"),
            os.path.join(self.out_dir, "status.log")
        ]
        
        # Măsurăm timestamp inițial
        init_mtimes = {p: (os.path.getmtime(p) if os.path.exists(p) else 0) for p in candidates}
        deadline = time.time() + self.timeout_s

        in_file = os.path.join(self.in_dir, f"{job_name}.txt")
        done_dir = os.path.join(self.in_dir, "done")
        err_dir = os.path.join(self.in_dir, "error")
        bad_dir = os.path.join(self.in_dir, "bad")
        disappeared_ts = 0.0

        while time.time() < deadline:
            time.sleep(0.3)

            # 1. Verificăm dacă ServerTremol a marcat ca eroare sau sintaxă greșită în error/ sau bad/ (inclusiv subdirectoare)
            for d in [err_dir, bad_dir]:
                if os.path.exists(d):
                    for r, _, fs in os.walk(d):
                        for f in fs:
                            if job_name in f:
                                resp_info = ""
                                rasp_path = os.path.join(self.out_dir, "raspuns.txt")
                                if os.path.exists(rasp_path):
                                    try:
                                        with open(rasp_path, "r", encoding="cp1250", errors="replace") as rf:
                                            resp_info = rf.read().strip()
                                    except Exception:
                                        pass
                                raise FiscalException("TREMOL_ERROR", f"Tremol a respins fișierul ({f}). Răspuns: {resp_info}")

            # 2. Verificăm dacă fișierul a fost finalizat în done/ (inclusiv subdirectoare gen done/Z0406/)
            # ServerTremol plasează fișierul aici EXACT când casa de marcat a terminat fizic de tipărit pe hârtie!
            if os.path.exists(done_dir):
                for r, _, fs in os.walk(done_dir):
                    for f in fs:
                        if job_name in f:
                            # S-a finalizat fizic tipărirea pe casa de marcat!
                            # ServerTremol include numărul de bon chiar în numele fișierului arhivat: [done][0001]
                            match = re.search(r'\[done\]\[(\d+)\]', f)
                            receipt_num = ""
                            if match:
                                raw_n = match.group(1)
                                receipt_num = str(int(raw_n)) if raw_n.isdigit() else raw_n

                            match_z = re.search(r'[Zz](\d{4})', r)
                            z_num = str(int(match_z.group(1))) if match_z else ""

                            # Verificăm dacă există și raspuns.txt
                            for p in candidates:
                                if os.path.exists(p) and os.path.getmtime(p) > init_mtimes.get(p, 0):
                                    try:
                                        with open(p, "r", encoding="cp1250", errors="replace") as rf:
                                            ans = rf.read().strip()
                                            if "ERR" in ans.upper() or "ERROR" in ans.upper():
                                                raise FiscalException("TREMOL_REJECTED", ans)
                                            if "|" in ans:
                                                return ans
                                    except Exception:
                                        pass

                            target_num = receipt_num if (receipt_num and receipt_num != "0") else z_num
                            if target_num:
                                return f"0|OK|{target_num}|TREMOL"
                            return "OK (Tipărit și finalizat de Tremol)"

            # 3. Verificăm dacă raspuns.txt a fost modificat
            for p in candidates:
                if os.path.exists(p):
                    mtime = os.path.getmtime(p)
                    if mtime > init_mtimes.get(p, 0):
                        time.sleep(0.2)
                        with open(p, "r", encoding="cp1250", errors="replace") as f:
                            ans = f.read().strip()
                            if "ERR" in ans.upper() or "ERROR" in ans.upper():
                                raise FiscalException("TREMOL_REJECTED", ans)
                            return ans

            # 4. Urmărim dacă fișierul a fost preluat din IN (a dispărut din Bonuri/)
            # Cât timp casa tipărește pe hârtie, fișierul nu mai e în IN, dar încă nu a apărut în done/.
            # Așteptăm până apare în done/ (sau maxim 15s de siguranță)
            if not os.path.exists(in_file):
                if disappeared_ts == 0.0:
                    disappeared_ts = time.time()
                elif (time.time() - disappeared_ts) > 15.0:
                    return "OK (Fișier prelucrat de Tremol)"

        # Curățăm fișierul netrimis din IN pentru a nu bloca coada ServerTremol la următoarele bonuri
        if os.path.exists(in_file):
            try:
                os.remove(in_file)
            except OSError:
                pass

        raise FiscalException("TREMOL_TIMEOUT", f"Fișierul {job_name}.txt a fost creat în {self.in_dir}, dar Tremol nu a procesat bonul în {self.timeout_s}s. Verificați că ServerTremol.exe rulează și casa este conectată în REG.OPER.")

    def recover_receipt(self, file_base: str):
        """Încercare anterioară cu rezultat necunoscut. Vezi DatecsDriver.recover_receipt."""
        job_name = f"bon.{file_base}"
        in_file = os.path.join(self.in_dir, f"{job_name}.txt")
        if os.path.exists(in_file):
            try:
                os.replace(in_file, in_file + ".anulat")
                return "not_sent", None
            except OSError:
                return "unknown", None
        for sub, state in (("done", "done"), ("error", "failed"), ("bad", "failed")):
            d = os.path.join(self.in_dir, sub)
            if not os.path.exists(d):
                continue
            for r, _, fs in os.walk(d):
                for f in fs:
                    if job_name in f:
                        if state == "failed":
                            return "failed", f"Tremol a respins fișierul ({f})"
                        m = re.search(r'\[done\]\[(\d+)\]', f)
                        num = str(int(m.group(1))) if m and m.group(1).isdigit() else ""
                        return "done", {"ok": True, "result": f"0|OK|{num}|TREMOL", "receiptNumber": num,
                                        "receipt_number": num, "taskId": file_base}
        return "unknown", None

    def print_receipt(self, job_payload: dict, task_id: str, file_base: str = "") -> dict:
        """
        Construiește bon fiscal conform specificației oficiale ServerTremol:
        Linia 1: C|<cui>|<denumire>|<adresa> (obligatoriu prima linie dacă există CUI)
        Linii V: V|<denumire produs>~<um>|<tva 0-5>|<pret>|<cantitate>|<-discount>|<tip disc>|<dep>
        Linia U: U|0|0 (subtotal)
        Linii P: P|<tip plata>|<suma> (0=Numerar, 1=Card, 3=Tichete, 5=Voucher, 8=Alte)
        """
        self._ensure_server_running()
        receipt = job_payload.get("receipt") or job_payload.get("body") or {}
        # nume determinist pe încercare (jurnalul USN din FiscalRouter): o reîncercare nu scrie un al doilea bon
        job_name = f"bon.{file_base}" if file_base else f"bon.{int(time.time())}_{task_id[-6:]}"
        lines = []

        # CUI client dacă există conform sintaxă Tremol:
        # C|<cui - max 15>|<denumire - max 30>|<adresa - max 30>
        recipient = receipt.get("recipient") or {}
        customer = receipt.get("customer") or {}
        cui = resolve_buyer_tax_num(job_payload, receipt)

        if cui:
            name = (recipient.get("name") or customer.get("name") or job_payload.get("buyer_name") or "Client").strip()[:30].replace("|", " ")
            addr = (recipient.get("address") or customer.get("address") or job_payload.get("buyer_address") or "-").strip()[:30].replace("|", " ")
            cui_clean = cui[:15].replace("|", "")
            lines.append(f"C|{cui_clean}|{name}|{addr}")

        # Articole conform DocumentatieFormatServerTremol.txt:
        # V|<denumire produs>[max 32]`sau~<um max 3>|<tva 0-5>|<pret>|<cantitate>|<-discount>|<tip discount>|<departament>
        # T|<text liber>[max 35] pentru linii de comentarii / deviz de lucru
        items = receipt.get("items") or receipt.get("lines") or []
        for it in items:
            # Comentariu sau text informativ (ex. deviz de lucru)
            if it.get("type") == "comment" or (it.get("unitPrice") is None and it.get("price") is None):
                raw_txt = (it.get("text") or it.get("name") or it.get("description") or "").replace("|", " ")
                txt = sanitize_comment_text(raw_txt, max_len=35)
                if txt:
                    lines.append(f"T|{txt}")
                continue

            name = (it.get("text") or it.get("name") or it.get("description") or "Articol").strip()[:32].replace("|", " ")
            raw_unit = (it.get("unit") or "buc").strip().replace("~", "").replace("`", "").replace("|", "")
            unit = (raw_unit if raw_unit else "buc")[:3]
            vat = it.get("taxGroup") or it.get("vat_group") or it.get("vatGroup") or 1
            raw_price = it.get("unitPrice") if it.get("unitPrice") is not None else it.get("price", 0)
            price = f"{float(raw_price):.2f}"
            qty = f"{float(it.get('quantity', 1)):.3f}"
            disc = "0"
            disc_type = "0"
            dep = it.get("department", 1)
            lines.append(f"V|{name}~{unit}|{vat}|{price}|{qty}|{disc}|{disc_type}|{dep}")

        # Subtotal fără discount conform doc: U|0|0
        lines.append("U|0|0")

        # Plăți conform TipPlatiInitiale.txt:
        # 0: Numerar, 1: Card, 3: Tichete, 4: Bonuri, 5: Voucher, 6: Credit, 8: Alte
        pay_map = {
            "cash": 0, "numerar": 0,
            "card": 1, "pos": 1,
            "ticket": 3, "tichet": 3, "tichete": 3,
            "bon": 4, "bonuri": 4,
            "voucher": 5,
            "credit": 6,
            "bank": 8, "virament": 8, "op": 8, "other": 8, "alta": 8, "alte": 8
        }
        payments = receipt.get("payments") or [{"paymentType": "cash", "amount": 0}]
        for p in payments:
            ptype = pay_map.get(str(p.get("paymentType", "cash")).lower(), 0)
            amt_num = float(p.get("amount", 0))
            if amt_num > 0:
                amt = f"{amt_num:.2f}"
                lines.append(f"P|{ptype}|{amt}")
            else:
                # P|0|0 închide bonul cu plata integrală în numerar (exact cash)
                lines.append(f"P|{ptype}|0")

        target_file = os.path.join(self.in_dir, f"{job_name}.txt")
        tmp_file = os.path.join(self.in_dir, f"{job_name}.tmp")
        content = "\r\n".join(lines) + "\r\n"

        logger.info(f"[Tremol] Scriere bon {target_file} ({len(lines)} linii)")
        with open(tmp_file, "w", newline="", encoding="cp1250", errors="replace") as f:
            f.write(content)
        if os.path.exists(target_file):
            try:
                os.remove(target_file)
            except OSError:
                pass
        os.replace(tmp_file, target_file)

        res = self._wait_for_response(job_name)

        # Extragem numărul de bon din răspunsul Tremol (ex: 0|OK|1|TREMOL sau din raspuns.txt)
        receipt_num = ""
        if res and "|" in res:
            parts = res.split("|")
            # Dacă prima parte e '0', bonul a fost cu succes
            if len(parts) >= 3 and parts[0] == "0":
                receipt_num = parts[2].strip()

        # Fallback suplimentar: căutăm direct în folderul done după fișierul jobului
        if not receipt_num or not receipt_num.isdigit():
            done_dir = os.path.join(self.in_dir, "done")
            if os.path.exists(done_dir):
                for r, _, fs in os.walk(done_dir):
                    for f in fs:
                        if job_name in f:
                            m = re.search(r'\[done\]\[(\d+)\]', f)
                            if m and m.group(1).isdigit() and int(m.group(1)) > 0:
                                receipt_num = str(int(m.group(1)))
                                break

        logger.info(f"[Tremol] Bon fiscal finalizat cu succes! Nr. bon extras: {receipt_num or 'N/A'}")

        # Trimitem JSON-ul către Sincrona Cloud folosind cheile standard 
        # pe care le folosea și ErpNet.FP ca să fie recunoscute instant.
        return {
            "ok": True,
            "result": res,
            "receiptNumber": receipt_num,
            "receipt_number": receipt_num,
            "taskId": task_id,
        }


    def print_report(self, report_type: str, job_payload: dict, task_id: str) -> dict:
        """
        Execută rapoarte fiscale Tremol:
        - Raport Z zilnic (bon.z.raport.txt cu comanda Z)
        - Raport X informativ (bon.x.raport.txt cu comanda X)
        - Raport periodic memorie fiscală pe date (F|DDMMYY|DDMMYY|0=sumar/1=detaliat)
        """
        self._ensure_server_running()
        low_type = report_type.lower().replace("_", "")
        body = job_payload.get("body", {})
        from_date = body.get("fromDate", "")[:10]
        to_date = body.get("toDate", "")[:10]

        if "period" in low_type and from_date and to_date:
            f_p = from_date.split("-")
            t_p = to_date.split("-")
            if len(f_p) == 3 and len(t_p) == 3:
                # Format oficial Tremol: F|DDMMYY|DDMMYY|0/1
                d_from = f"{f_p[2]}{f_p[1]}{f_p[0][2:]}"
                d_to = f"{t_p[2]}{t_p[1]}{t_p[0][2:]}"
                det = "1" if body.get("type") == "detailed" else "0"
                cmd = f"F|{d_from}|{d_to}|{det}\r\n"
                file_name = f"bon.RaportPeriodicSume_{int(time.time())}.txt"
            else:
                cmd = "Z\r\n"
                file_name = f"bon.z.raport_{int(time.time())}.txt"
        else:
            is_z = "z" in low_type
            cmd = "Z\r\n" if is_z else "X\r\n"
            file_name = f"bon.z.raport_{int(time.time())}_{task_id[-6:]}.txt" if is_z else f"bon.x.raport_{int(time.time())}_{task_id[-6:]}.txt"

        target_path = os.path.join(self.in_dir, file_name)
        tmp_path = os.path.join(self.in_dir, f"{file_name}.tmp")

        with open(tmp_path, "w", newline="", encoding="cp1250", errors="replace") as f:
            f.write(cmd)
        if os.path.exists(target_path):
            try:
                os.remove(target_path)
            except OSError:
                pass
        os.replace(tmp_path, target_path)

        res = self._wait_for_response(file_name.replace(".txt", ""))

        # Parsare răspuns Tremol ZFPServer pentru rapoarte fiscale
        # Format tipic: 0|OK|[NumarZ]|[SerieFiscala]|[TotalGeneral]|[TotalNumerar]|[TotalCard]|[NumarBonuri]|[DataOra]
        parts = [p.strip() for p in (res or "").split("|")]
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        report_num = ""
        doc_count = "0"
        total_amt = "0.00"
        cash_amt = "0.00"
        card_amt = "0.00"

        if len(parts) >= 3 and parts[0] == "0":
            if parts[2].isdigit():
                report_num = parts[2]
            if len(parts) >= 5:
                try:
                    total_amt = f"{float(parts[4]):.2f}"
                except Exception:
                    pass
            if len(parts) >= 6:
                try:
                    cash_amt = f"{float(parts[5]):.2f}"
                except Exception:
                    pass
            if len(parts) >= 7:
                try:
                    card_amt = f"{float(parts[6]):.2f}"
                except Exception:
                    pass
            if len(parts) >= 8 and parts[7].isdigit():
                doc_count = parts[7]

        if "z" in low_type and not report_num:
            done_dir = os.path.join(self.in_dir, "done")
            if os.path.exists(done_dir):
                for r, _, fs in os.walk(done_dir):
                    for f in fs:
                        if file_name.replace(".txt", "") in f:
                            mz = re.search(r'[Zz](\d{4})', r)
                            if mz and mz.group(1).isdigit():
                                report_num = str(int(mz.group(1)))
                                break

        # Dacă ServerTremol este în mod fișier și nu a returnat sumele în răspuns,
        # calculăm telemetria reală direct din bonurile arhivate în folderul Z
        is_z_report = "z" in low_type
        if (total_amt == "0.00" or doc_count == "0") and ("x" in low_type or is_z_report):
            c_docs, c_total, c_cash, c_card, c_z = self._compute_current_z_totals(is_z=is_z_report)
            if c_docs > 0:
                doc_count = str(c_docs)
                total_amt = f"{c_total:.2f}"
                cash_amt = f"{c_cash:.2f}"
                card_amt = f"{c_card:.2f}"
            if c_z:
                report_num = c_z

        if is_z_report:
            summary = f"Z nr. {report_num or '1'} - Raport Z {now_str}: {doc_count} documente, total {total_amt} RON (card: {card_amt} RON, cash: {cash_amt} RON)"
        elif "x" in low_type:
            summary = f"Raport X {now_str}: {doc_count} documente, total {total_amt} RON (card: {card_amt} RON, cash: {cash_amt} RON)"
        elif "period" in low_type:
            # Descărcare automată pachete ANAF prin ServerTremol (dacă e raport detaliat)
            anaf_res = None
            if body.get("type") == "detailed" or "anaf" in low_type:
                anaf_res = self._download_anaf_via_tremol(from_date, to_date)

            if hasattr(self, "_compute_period_totals"):
                p_docs, p_tot = self._compute_period_totals(from_date, to_date)
            else:
                p_docs, p_tot = 0, 0.0

            if anaf_res and anaf_res.get("ok"):
                summary = f"Raport memorie fiscală ({from_date} – {to_date}) solicitat cu succes prin ServerTremol!"
            elif p_docs > 0:
                summary = f"Raport memorie fiscală ({from_date} – {to_date}): {p_docs} documente, total {p_tot:.2f} RON"
            else:
                summary = f"Raport memorie fiscală ({from_date} – {to_date}) tipărit cu succes pe casa de marcat"
        else:
            summary = f"Raport {report_type.upper()} {now_str} executat cu succes"

        return {
            "ok": True,
            "result": res,
            "raw_response": res,
            "report": report_type,
            "reportNumber": report_num,
            "report_number": report_num,
            "total": total_amt,
            "messages": [
                {
                    "type": "info",
                    "text": summary,
                }
            ],
            "taskId": task_id,
        }


# ==================== JURNAL USN PENTRU DRIVERELE PE FIȘIERE ====================
# ErpNet.FP are propriul jurnal uniqueSaleNumber. Datecs (DUDE / FPrinter) și Tremol (ServerTremol) lucrează
# prin fișiere și nu au echivalent: fără jurnal, o reîncercare după timeout scria un fișier nou și casa putea
# tipări al doilea bon. Jurnalul ține, pe uniqueSaleNumber (pa-<id plată>), starea: sent (fișier scris, rezultat
# necunoscut), done (tipărit, cu rezultatul salvat) sau failed (respins sigur, se poate retipări).

class FileUsnJournal:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()

    def _load(self) -> dict:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def get(self, usn: str) -> dict:
        with self.lock:
            return dict(self._load().get(usn) or {})

    def set(self, usn: str, **rec) -> None:
        with self.lock:
            data = self._load()
            data[usn] = {**(data.get(usn) or {}), **rec, "at": time.time()}
            cutoff = time.time() - 60 * 24 * 3600  # păstrăm 60 de zile
            data = {k: v for k, v in data.items() if v.get("at", 0) >= cutoff}
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, self.path)


FILE_USN_JOURNAL = FileUsnJournal(os.path.join(BASE_DIR, "file_usn_journal.json"))
FILE_DRIVER_TIMEOUTS = {"DATECS_TIMEOUT"}  # TREMOL_TIMEOUT retrage fișierul neprelucrat -> sigur netipărit


def _print_with_usn_journal(driver, payload: dict, task_id: str, usn: str) -> dict:
    safe = re.sub(r"[^A-Za-z0-9_-]", "", usn)[:48] or re.sub(r"[^A-Za-z0-9_-]", "", task_id)[:48]
    rec = FILE_USN_JOURNAL.get(usn)
    if rec.get("state") == "done":
        logger.info(f"[USN] Bonul {usn} a fost deja tipărit ({rec.get('file')}); nu se retipărește.")
        return rec.get("result") or {}
    if rec.get("state") == "sent":
        state, info = driver.recover_receipt(rec["file"])
        if state == "done":
            FILE_USN_JOURNAL.set(usn, state="done", result=info)
            return info
        if state == "unknown":
            raise FiscalException(
                "UNCERTAIN_RECEIPT",
                "Bonul a fost trimis anterior către casă și nu s-a primit confirmare. Verificați ultimul bon "
                "tipărit pe casă: dacă s-a tipărit, închideți încasarea cu numărul lui; dacă NU s-a tipărit, "
                "anulați încasarea în Sincrona și încasați din nou. Bonul nu se retrimite automat ca să nu se dubleze.")
        logger.info(f"[USN] Încercarea anterioară {rec['file']} pentru {usn}: {state}; se retrimite.")
    attempt = int(rec.get("attempt") or 0) + 1
    file_base = f"{safe}_{attempt}"
    FILE_USN_JOURNAL.set(usn, state="sent", file=file_base, attempt=attempt)
    try:
        res = driver.print_receipt(payload, task_id, file_base=file_base)
    except FiscalException as exc:
        if exc.code not in FILE_DRIVER_TIMEOUTS:
            FILE_USN_JOURNAL.set(usn, state="failed", error=f"{exc.code}: {exc.text}")
        raise
    FILE_USN_JOURNAL.set(usn, state="done", result=res)
    return res


# ==================== UNIVERSAL FISCAL ROUTER ====================

class FiscalRouter:
    @staticmethod
    def get_driver() -> BaseFiscalDriver:
        cfg = get_config()
        drv_name = cfg.get("FISCAL_DRIVER", "erpnet").lower()

        if drv_name == "datecs":
            return DatecsDriver(
                in_dir=cfg.get("DATECS_IN_DIR", r"C:\Datecs\In"),
                out_dir=cfg.get("DATECS_OUT_DIR", r"C:\Datecs\Out"),
                format_mode=cfg.get("DATECS_FORMAT", "dude"),
                timeout_s=cfg.get("TIMEOUT_SECONDS", 45),
            )
        elif drv_name == "tremol":
            return TremolDriver(
                in_dir=cfg.get("TREMOL_IN_DIR", r"C:\ServerTremol\Bonuri"),
                out_dir=cfg.get("TREMOL_OUT_DIR", r"C:\ServerTremol"),
                timeout_s=cfg.get("TIMEOUT_SECONDS", 45),
            )
        else:
            return ErpNetFPDriver(
                erpnet_url=cfg.get("ERPNET_URL", "http://127.0.0.1:8001")
            )

    _print_lock = threading.Lock()

    @classmethod
    def execute_job(cls, job: dict) -> dict:
        # o singură comandă odată către casă: joburile din cloud și bonurile offline (de pe calculator) nu se suprapun
        with cls._print_lock:
            return cls._execute_job(job)

    @classmethod
    def _execute_job(cls, job: dict) -> dict:
        driver = cls.get_driver()
        task_id = job.get("idempotency_key") or job.get("id") or str(int(time.time()))
        # „Reîncearcă” păstrează id-ul jobului: ErpNet.FP ține taskId-urile în memorie și ar răspunde E109 cu
        # rezultatul (eșuat) al primei încercări. Fiecare reîncercare primește deci propriul taskId (max. 60 car.);
        # dublarea bonului rămâne blocată de uniqueSaleNumber (pa-<id plată>) din jurnalul USN al driverului.
        retry_at = re.sub(r"\D", "", str(job.get("retry_requested_at") or ""))[:14]
        if retry_at:
            task_id = f"{task_id}-r{retry_at}"[:60]
        kind = job.get("kind", "receipt").lower()
        payload = job.get("payload", {})

        norm_kind = kind.replace("_", "")
        logger.info(f"[Router] Procesare job {job.get('id')} cu driver {driver.__class__.__name__} (kind={kind})")
        if norm_kind in ("xreport", "zreport", "periodreport") or "report" in norm_kind or payload.get("endpoint") in ("xreport", "zreport", "periodreport"):
            rep_type = payload.get("endpoint") or norm_kind
            return driver.print_report(rep_type, payload, task_id)
        if isinstance(driver, (DatecsDriver, TremolDriver)):
            receipt = payload.get("receipt") or payload.get("body") or {}
            usn = str(receipt.get("uniqueSaleNumber") or job.get("idempotency_key") or job.get("id") or task_id)
            return _print_with_usn_journal(driver, payload, task_id, usn)
        return driver.print_receipt(payload, task_id)


def trigger_anaf_download(from_date: str = "", to_date: str = "") -> dict:
    """Declanșează descărcarea/exportul automat al rapoartelor ANAF prin driverul fiscal activ (Datecs sau Tremol)."""
    driver = FiscalRouter.get_driver()
    if hasattr(driver, "_download_anaf_via_tremol"):
        return driver._download_anaf_via_tremol(from_date, to_date)
    if hasattr(driver, "_download_anaf_via_dude"):
        return driver._download_anaf_via_dude(from_date, to_date)

    cfg = get_config()
    driver_name = (cfg.get("FISCAL_DRIVER") or "datecs").lower()
    if driver_name == "tremol":
        d = TremolDriver(
            in_dir=cfg.get("TREMOL_IN_DIR", r"C:\ServerTremol\Bonuri"),
            out_dir=cfg.get("TREMOL_OUT_DIR", r"C:\ServerTremol"),
            timeout_s=cfg.get("TIMEOUT_SECONDS", 45),
        )
        return d._download_anaf_via_tremol(from_date, to_date)
    else:
        d = DatecsDriver(
            in_dir=cfg.get("DATECS_IN_DIR", r"C:\Datecs\In"),
            out_dir=cfg.get("DATECS_OUT_DIR", r"C:\Datecs\Out"),
            format_mode=cfg.get("DATECS_FORMAT", "dude"),
            timeout_s=cfg.get("TIMEOUT_SECONDS", 45),
        )
        return d._download_anaf_via_dude(from_date, to_date)


# ==================== CLOUD API & WEBSOCKET ENGINE ====================

def get_headers():
    token = get_config().get("DEVICE_TOKEN", "")
    return {
        "X-Device-Token": token,
        "User-Agent": "Sincrona-Enterprise-Edge-Bridge/2.0",
        "Content-Type": "application/json"
    }

def complete_job_http(job_id: str, status: str, result: dict):
    api_url = get_config().get("API_URL", "").rstrip("/")
    if not api_url:
        return
    try:
        r = requests.post(
            f"{api_url}/api/device/fiscal-printer/jobs/{job_id}/complete",
            headers=get_headers(),
            json={"status": status, "result": result},
            timeout=15
        )
        if r.status_code == 409:
            logger.info(f"[Cloud] Job {job_id} era deja marcat ca finalizat pe cloud.")
            return
        r.raise_for_status()
        logger.info(f"[Cloud] Job {job_id} marcat ca {status} pe cloud.")
    except Exception as e:
        log_error("cloud", "COMPLETE_FAIL", f"Eroare la confirmarea jobului {job_id}: {e}")

# Evidența încercărilor procesate. Același job poate sosi de două ori pentru aceeași încercare (push WebSocket +
# preluare HTTP): se tipărește o singură dată. „Reîncearcă” din Sincrona păstrează id-ul jobului, dar setează
# retry_requested_at, deci cheia diferă și jobul se procesează din nou. Dacă jobul revine pentru o încercare deja
# terminată (confirmarea către cloud a eșuat), nu se retipărește: se retrimite rezultatul salvat.
_job_attempts = OrderedDict()   # cheie încercare -> {"state": processing|completed|failed, "result": ..., "at": ts}
_claimed_lock = threading.Lock()
_ATTEMPT_TTL_S = 6 * 3600
_ATTEMPT_MAX = 1000


def _attempt_key(job: dict) -> str:
    return f"{job.get('id') or ''}|{job.get('retry_requested_at') or ''}"


def claim_job(job: dict):
    """(True, None) dacă încercarea e nouă și a fost revendicată; (False, înregistrare) dacă e deja în lucru
    sau terminată. Cele mai vechi înregistrări (peste TTL sau peste limită) se elimină în ordinea sosirii."""
    key = _attempt_key(job)
    now_ts = time.time()
    with _claimed_lock:
        while _job_attempts:
            first_key, first = next(iter(_job_attempts.items()))
            if len(_job_attempts) > _ATTEMPT_MAX or now_ts - first["at"] > _ATTEMPT_TTL_S:
                if first["state"] == "processing" and now_ts - first["at"] <= _ATTEMPT_TTL_S:
                    break
                _job_attempts.pop(first_key)
            else:
                break
        rec = _job_attempts.get(key)
        if rec is not None:
            return False, dict(rec)
        _job_attempts[key] = {"state": "processing", "result": None, "at": now_ts}
        return True, None


def finish_job(job: dict, status: str, result: dict) -> None:
    with _claimed_lock:
        _job_attempts[_attempt_key(job)] = {"state": status, "result": result, "at": time.time()}


def resend_finished(job: dict, rec: dict) -> bool:
    """Încercare deja terminată care revine de la server: retrimite rezultatul, fără a retipări bonul."""
    if rec and rec.get("state") in ("completed", "failed"):
        logger.info(f"[Router] Job {job.get('id')} deja {rec['state']} pentru această încercare; retrimit confirmarea.")
        complete_job_http(job.get("id"), rec["state"], rec.get("result") or {})
        return True
    return False

class ResilientWebSocketClient:
    """Client WebSocket cu auto-reconnect, ping/pong și procesare comenzi."""
    def __init__(self):
        self.stop_event = threading.Event()

    def get_ws_url(self) -> str:
        cfg = get_config()
        if cfg.get("WS_URL"):
            return cfg["WS_URL"]
        api = cfg.get("API_URL", "https://api.sincrona.ro").rstrip("/")
        ws_scheme = "wss" if api.startswith("https") else "ws"
        host = urlparse(api).netloc or "api.sincrona.ro"
        return f"{ws_scheme}://{host}/ws/device/fiscal-printer"

    async def _run_loop(self):
        backoff = 2
        while not self.stop_event.is_set():
            cfg = get_config()
            token = cfg.get("DEVICE_TOKEN", "")
            if not token:
                update_state(connected_ws=False)
                await asyncio.sleep(3)
                continue

            ws_url = self.get_ws_url()
            if "token=" not in ws_url and token:
                sep = "&" if "?" in ws_url else "?"
                ws_target_url = f"{ws_url}{sep}token={token}"
            else:
                ws_target_url = ws_url

            logger.info(f"[WebSocket] Conectare la {ws_url}...")
            try:
                extra_headers = {
                    "X-Device-Token": token,
                    "User-Agent": "SincronaBridge/2.0",
                }
                async with websockets.connect(
                    ws_target_url,
                    additional_headers=extra_headers,
                    ping_interval=20,
                    ping_timeout=15,
                    close_timeout=10,
                ) as ws:
                    logger.info("[WebSocket] Conexiune stabilită cu succes!")
                    update_state(connected_ws=True, last_error=None)
                    backoff = 2

                    # Mesaj de inițializare
                    await ws.send(json.dumps({
                        "action": "register",
                        "token": token,
                        "driver": cfg.get("FISCAL_DRIVER"),
                        "version": "2.0-native"
                    }))

                    while not self.stop_event.is_set():
                        msg_raw = await ws.recv()
                        update_state(last_ws_msg_ts=datetime.now().isoformat(timespec="seconds"))
                        try:
                            msg = json.loads(msg_raw)
                        except Exception:
                            continue

                        # Răspuns la Ping
                        if msg.get("action") == "ping":
                            await ws.send(json.dumps({"action": "pong", "time": time.time()}))
                            continue

                        # Procesare Job Fiscal
                        if msg.get("type") == "job" or "job" in msg:
                            job = msg.get("job") or msg
                            job_id = job.get("id")
                            if job_id:
                                claimed, rec = claim_job(job)
                                if not claimed:
                                    await asyncio.to_thread(resend_finished, job, rec)
                                    continue
                            update_state(last_job_id=job_id, last_job_status="processing")
                            try:
                                res = await asyncio.to_thread(FiscalRouter.execute_job, job)
                                update_state(
                                    total_jobs_ok=get_state()["total_jobs_ok"] + 1,
                                    last_job_status="completed"
                                )
                                # Răspuns prin WS și redundanță HTTP
                                finish_job(job, "completed", res)
                                await ws.send(json.dumps({
                                    "type": "job_result",
                                    "job_id": job_id,
                                    "status": "completed",
                                    "result": res
                                }))
                                complete_job_http(job_id, "completed", res)
                            except Exception as err:
                                err_code = getattr(err, "code", "EXECUTION_ERROR")
                                err_text = getattr(err, "text", str(err))
                                update_state(
                                    total_jobs_failed=get_state()["total_jobs_failed"] + 1,
                                    last_job_status="failed",
                                    last_error=f"{err_code}: {err_text}"
                                )
                                log_error("router", err_code, err_text, {"job_id": job_id})
                                fail_payload = {"error": err_code, "text": err_text}
                                finish_job(job, "failed", fail_payload)
                                await ws.send(json.dumps({
                                    "type": "job_result",
                                    "job_id": job_id,
                                    "status": "failed",
                                    "result": fail_payload
                                }))
                                complete_job_http(job_id, "failed", fail_payload)

            except Exception as e:
                update_state(connected_ws=False)
                err_str = str(e)
                if "403" in err_str or "404" in err_str:
                    logger.info("[WebSocket] Endpoint WebSocket indisponibil pe server; comutare automată pe REST Polling.")
                    backoff = 60
                else:
                    logger.warning(f"[WebSocket] Conexiune întreruptă: {e}. Reconectare în {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    def start(self):
        if not HAS_WEBSOCKETS:
            logger.warning("[WebSocket] Pachetul 'websockets' lipsește; rulează doar pe HTTP Polling.")
            return

        def _runner():
            asyncio.run(self._run_loop())

        t = threading.Thread(target=_runner, daemon=True, name="WebSocketWorker")
        t.start()


# ==================== HTTP POLLING ENGINE (FALLBACK / REDUNDANCY) ====================

def run_http_polling():
    """Rulează în fundal pentru a prelua joburi prin REST (redundant și robust)."""
    logger.info("[PollWorker] Worker de verificare HTTP pornit.")
    while True:
        cfg = get_config()
        if not cfg.get("POLL_FALLBACK", True) or not cfg.get("DEVICE_TOKEN"):
            time.sleep(4)
            continue

        api_url = cfg.get("API_URL", "").rstrip("/")
        if not api_url:
            time.sleep(4)
            continue

        try:
            r = requests.get(f"{api_url}/api/device/fiscal-printer/jobs", headers=get_headers(), timeout=12)
            if r.status_code == 200:
                update_state(connected_cloud=True, last_poll_ts=datetime.now().isoformat(timespec="seconds"))
                job = r.json().get("job")
                if job:
                    job_id = job.get("id")
                    if job_id:
                        claimed, rec = claim_job(job)
                        if not claimed:
                            resend_finished(job, rec)
                            time.sleep(1)
                            continue
                    logger.info(f"[PollWorker] Job recepționat prin HTTP: {job_id}")
                    update_state(last_job_id=job_id, last_job_status="processing")
                    try:
                        res = FiscalRouter.execute_job(job)
                        finish_job(job, "completed", res)
                        update_state(
                            total_jobs_ok=get_state()["total_jobs_ok"] + 1,
                            last_job_status="completed"
                        )
                        complete_job_http(job_id, "completed", res)
                    except Exception as err:
                        err_code = getattr(err, "code", "EXECUTION_ERROR")
                        err_text = getattr(err, "text", str(err))
                        finish_job(job, "failed", {"error": err_code, "text": err_text})
                        update_state(
                            total_jobs_failed=get_state()["total_jobs_failed"] + 1,
                            last_job_status="failed",
                            last_error=f"{err_code}: {err_text}"
                        )
                        log_error("router", err_code, err_text, {"job_id": job_id})
                        complete_job_http(job_id, "failed", {"error": err_code, "text": err_text})
            else:
                update_state(connected_cloud=False)
        except Exception as e:
            update_state(connected_cloud=False)
        time.sleep(3)


# ==================== MOD OFFLINE (FĂRĂ INTERNET) ====================
# Când nu e internet, aplicația Sincrona din browser (pe ACEST calculator) trimite bonul direct aici:
#   GET  /local/status   — puntea răspunde? are setările casei? câte bonuri offline așteaptă trimiterea
#   POST /local/receipt  — {client_ref, label, lines:[{name, qty, unit_price (cu TVA), vat_rate}],
#                           payments:[{amount, method: numerar|card}], buyer_tax_id?, buyer_name?}
#   POST /local/report   — {client_ref, type: "x" | "z"}
# Setările casei (adresa ErpNet.FP, imprimanta, mod test/live, cotele TVA → grupe fiscale) se iau din Sincrona cât
# e internet (OFFLINE_PROFILE_FILE). Fiecare bon / raport offline se notează în OFFLINE_RECORDS_FILE și se trimite
# în Sincrona când revine internetul (POST /api/device/fiscal-printer/offline-records), o singură dată.
# Același client_ref trimis de două ori nu tipărește al doilea bon (se întoarce rezultatul primului).

OFFLINE_PROFILE_FILE = os.path.join(BASE_DIR, "offline_profile.json")
OFFLINE_RECORDS_FILE = os.path.join(BASE_DIR, "offline_records.json")
OFFLINE_PROFILE_EVERY_S = 600
OFFLINE_UPLOAD_EVERY_S = 30
OFFLINE_KEEP_DAYS = 90
# paginile Sincrona care pot trimite bonuri la punte (antetul Origin al browserului nu poate fi falsificat de alt site)
LOCAL_ORIGIN_RE = re.compile(r"^(https://([a-z0-9-]+\.)?sincrona\.ro|https?://(localhost|127\.0\.0\.1)(:\d+)?)$")
CLIENT_REF_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
_offline_lock = threading.Lock()


def _read_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def _write_json(path: str, data) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def offline_profile() -> dict:
    return _read_json(OFFLINE_PROFILE_FILE, {})


def refresh_offline_profile() -> bool:
    """Setările casei din Sincrona, păstrate pentru bonurile emise fără internet."""
    api_url = get_config().get("API_URL", "").rstrip("/")
    if not api_url or not get_config().get("DEVICE_TOKEN"):
        return False
    try:
        r = requests.get(f"{api_url}/api/device/fiscal-printer/status", headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return False
        prof = (r.json() or {}).get("offline_profile")
        if prof:
            _write_json(OFFLINE_PROFILE_FILE, {**prof, "saved_at": datetime.now().isoformat(timespec="seconds")})
            return True
    except Exception:  # noqa: BLE001 — fără internet: rămâne profilul salvat anterior
        pass
    return False


def offline_records() -> dict:
    return _read_json(OFFLINE_RECORDS_FILE, {})


def _save_record(ref: str, **rec) -> dict:
    with _offline_lock:
        data = offline_records()
        data[ref] = {**(data.get(ref) or {}), **rec}
        cutoff = time.time() - OFFLINE_KEEP_DAYS * 86400
        data = {k: v for k, v in data.items() if not v.get("uploaded") or v.get("ts", 0) >= cutoff}
        _write_json(OFFLINE_RECORDS_FILE, data)
        return dict(data[ref])


def _money(v) -> float:
    return round(float(v or 0) + 0.0, 2)


def build_offline_receipt(body: dict, prof: dict) -> dict:
    """Bonul în formatul joburilor din Sincrona (aceleași drivere: ErpNet.FP / Datecs / Tremol)."""
    vat_map = {str(k): int(v) for k, v in (prof.get("vat_to_tax_group") or {"21": 1, "11": 2, "0": 3}).items()}
    items = []
    total = 0.0
    for ln in body.get("lines") or []:
        qty = float(ln.get("qty") or 0)
        price = _money(ln.get("unit_price"))
        if qty <= 0 or price <= 0:
            raise FiscalException("INVALID_LINE", "Fiecare rând are nevoie de cantitate și preț mai mari decât zero.")
        rate = str(int(round(float(ln.get("vat_rate", 21) or 0))))
        if rate not in vat_map:
            raise FiscalException("VAT_MAPPING", f"Cota TVA {rate}% nu are grupă fiscală în setările casei de marcat.")
        text = sanitize_comment_text(str(ln.get("name") or "Produs"), 36) or "Produs"
        items.append({"text": text, "quantity": qty, "unit": str(ln.get("unit") or "buc")[:6], "unitPrice": price,
                      "taxGroup": vat_map[rate]})
        total += qty * price
    if not items:
        raise FiscalException("EMPTY_RECEIPT", "Bonul nu are niciun produs.")
    total = round(total, 2)
    payments = []
    for p in body.get("payments") or [{"amount": total, "method": "numerar"}]:
        method = str(p.get("method") or "numerar").lower()
        payments.append({"amount": _money(p.get("amount")), "paymentType": "card" if method in ("card", "pos") else "cash"})
    if abs(sum(p["amount"] for p in payments) - total) > 0.009:
        raise FiscalException("PAYMENT_TOTAL", f"Plata ({sum(p['amount'] for p in payments):.2f}) trebuie să fie egală cu totalul bonului ({total:.2f}).")
    ref = body["client_ref"]
    receipt = {"uniqueSaleNumber": f"of-{ref}"[:64], "items": items, "payments": payments}
    payload = {
        "erpnet_url": prof.get("erpnet_url") or get_config().get("ERPNET_URL", "http://127.0.0.1:8001"),
        "printer_id": prof.get("printer_id") or "default",
        "mode": prof.get("mode") or "test",
        "test_transport": prof.get("test_transport") or "local",
        "receipt": receipt,
        "endpoint": "receipt",
    }
    tax = re.sub(r"[^0-9A-Za-z]", "", str(body.get("buyer_tax_id") or "")).upper()
    if tax and not (len(tax) == 13 and tax.isdigit()):  # doar firme (CUI), nu CNP
        payload["buyer_tax_id"] = tax
        payload["client_cui"] = tax
        payload["buyer_name"] = str(body.get("buyer_name") or "Client")[:60]
        if prof.get("supports_invoice") is not False:
            payload["endpoint"] = "invoice"
            payload["body"] = {**receipt, "recipient": {"name": payload["buyer_name"], "identifier": tax.removeprefix("RO"),
                                                        "uniqueRegistrationNumber": tax.removeprefix("RO"),
                                                        **({"vatNumber": tax} if tax.startswith("RO") else {})}}
    return {"payload": payload, "total": total}


def run_offline_job(kind: str, ref: str, payload: dict, meta: dict) -> dict:
    prev = offline_records().get(ref)
    if prev and prev.get("status") == "completed":
        return prev  # același bon cerut din nou: nu se retipărește
    _save_record(ref, client_ref=ref, kind=kind, payload=payload, status="printing", ts=time.time(),
                 at=datetime.now().astimezone().isoformat(timespec="seconds"), uploaded=False, **meta)
    job = {"id": f"off-{ref}", "kind": kind, "idempotency_key": f"of-{ref}", "payload": payload}
    try:
        res = FiscalRouter.execute_job(job)
        rec = _save_record(ref, status="completed", result=res or {}, uploaded=False)
        update_state(total_jobs_ok=get_state()["total_jobs_ok"] + 1, last_job_status="completed (offline)")
    except Exception as err:  # noqa: BLE001
        code = getattr(err, "code", "EXECUTION_ERROR")
        text = getattr(err, "text", str(err))
        rec = _save_record(ref, status="failed", result={"error": code, "text": text}, uploaded=False)
        log_error("offline", code, text, {"client_ref": ref})
    threading.Thread(target=upload_offline_records, daemon=True).start()
    return rec


def upload_offline_records() -> int:
    """Bonurile și rapoartele emise fără internet ajung în Sincrona (Casă), o singură dată."""
    api_url = get_config().get("API_URL", "").rstrip("/")
    if not api_url or not get_config().get("DEVICE_TOKEN"):
        return 0
    pending = [r for r in offline_records().values() if not r.get("uploaded") and r.get("status") in ("completed", "failed")]
    sent = 0
    for i in range(0, len(pending), 25):
        batch = pending[i:i + 25]
        try:
            r = requests.post(f"{api_url}/api/device/fiscal-printer/offline-records", headers=get_headers(),
                              json={"records": batch}, timeout=20)
            if r.status_code != 200:
                break
            done = {x.get("client_ref") for x in (r.json() or {}).get("results", []) if x.get("status") in ("saved", "exists")}
            for ref in done:
                _save_record(ref, uploaded=True)
            sent += len(done)
        except Exception:  # noqa: BLE001 — tot fără internet: se reîncearcă
            break
    return sent


def run_offline_worker():
    last_profile = 0.0
    while True:
        try:
            if time.time() - last_profile > OFFLINE_PROFILE_EVERY_S:
                if refresh_offline_profile():
                    last_profile = time.time()
                else:
                    last_profile = time.time() - OFFLINE_PROFILE_EVERY_S + 60  # fără internet: reîncercare peste 1 minut
            upload_offline_records()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Offline] {e}")
        time.sleep(OFFLINE_UPLOAD_EVERY_S)


def local_status() -> dict:
    prof = offline_profile()
    recs = offline_records()
    return {
        "ok": True,
        "bridge": "sincrona",
        "driver": get_config().get("FISCAL_DRIVER", "erpnet"),
        "connected_cloud": get_state().get("connected_cloud"),
        "profile": {k: prof.get(k) for k in ("tenant_id", "branch_id", "branch_name", "printer_id", "mode", "vat_to_tax_group", "saved_at")} if prof else None,
        "pending_upload": sum(1 for r in recs.values() if not r.get("uploaded")),
        "recent": sorted(
            [{k: r.get(k) for k in ("client_ref", "kind", "label", "total", "status", "at", "uploaded")} |
             {"receipt_number": (r.get("result") or {}).get("receiptNumber"), "error": (r.get("result") or {}).get("text")}
             for r in recs.values()], key=lambda x: x.get("at") or "", reverse=True)[:20],
    }


# ==================== LOCAL ADMIN WEB INTERFACE (PORT 8002) ====================

ADMIN_HTML = r"""<!DOCTYPE html>
<html lang="ro">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sincrona – Universal Fiscal Router</title>
  <style>
    :root {
      --bg: #0b0f19;
      --card: #151d2f;
      --card-border: #23304a;
      --fg: #f1f5f9;
      --muted: #94a3b8;
      --accent: #0284c7;
      --accent-hover: #0369a1;
      --accent-glow: rgba(2, 132, 199, 0.25);
      --success: #10b981;
      --danger: #ef4444;
      --warning: #f59e0b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--fg); padding-bottom: 40px; }
    header { background: #0f172a; border-bottom: 1px solid var(--card-border); padding: 18px 32px; display: flex; justify-content: space-between; align-items: center; }
    .brand { display: flex; align-items: center; gap: 12px; font-size: 1.25rem; font-weight: 700; color: #fff; }
    .brand span { color: #38bdf8; font-size: 0.85rem; font-weight: 400; padding: 2px 8px; border: 1px solid #0369a1; border-radius: 6px; }
    .status-pill { padding: 6px 14px; border-radius: 99px; font-size: 0.8rem; font-weight: 600; display: flex; align-items: center; gap: 6px; }
    .status-pill.online { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
    .status-pill.offline { background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; }
    main { max-width: 1200px; margin: 28px auto; padding: 0 20px; display: grid; grid-template-columns: 1.1fr 0.9fr; gap: 24px; }
    @media(max-width: 960px) { main { grid-template-columns: 1fr; } }
    .card { background: var(--card); border: 1px solid var(--card-border); border-radius: 12px; padding: 22px; margin-bottom: 24px; box-shadow: 0 4px 20px rgba(0,0,0,0.25); }
    .card h2 { font-size: 1.05rem; color: #38bdf8; margin-bottom: 18px; display: flex; align-items: center; justify-content: space-between; }
    .field { margin-bottom: 16px; }
    label { display: block; font-size: 0.82rem; font-weight: 600; color: var(--muted); margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
    input, select { width: 100%; padding: 10px 14px; background: #0b1120; border: 1px solid var(--card-border); border-radius: 8px; color: #fff; font-size: 0.95rem; outline: none; transition: 0.2s border-color; }
    input:focus, select:focus { border-color: #38bdf8; box-shadow: 0 0 0 3px var(--accent-glow); }
    .row-actions { display: flex; gap: 10px; margin-top: 20px; }
    button { cursor: pointer; padding: 10px 18px; border-radius: 8px; font-weight: 600; font-size: 0.9rem; border: none; transition: 0.2s; }
    button.primary { background: var(--accent); color: #fff; }
    button.primary:hover { background: var(--accent-hover); }
    button.secondary { background: #334155; color: #f1f5f9; }
    button.secondary:hover { background: #475569; }
    button.danger { background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }
    button.danger:hover { background: rgba(239, 68, 68, 0.3); }
    .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 16px; }
    .stat-box { background: #0b1120; border: 1px solid var(--card-border); border-radius: 8px; padding: 12px; }
    .stat-label { font-size: 0.75rem; color: var(--muted); margin-bottom: 4px; }
    .stat-val { font-size: 1.15rem; font-weight: 700; color: #fff; word-break: break-all; }
    .stat-val.ok { color: var(--success); }
    .stat-val.fail { color: var(--danger); }
    .msg-box { margin-top: 14px; padding: 12px; border-radius: 8px; font-size: 0.85rem; font-family: Consolas, monospace; background: #0b1120; border: 1px solid var(--card-border); white-space: pre-wrap; max-height: 180px; overflow-y: auto; }
    table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
    th, td { text-align: left; padding: 9px 8px; border-bottom: 1px solid #1e293b; }
    th { color: var(--muted); font-weight: 600; }
    .badge { display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    .badge.driver { background: #0369a1; color: #e0f2fe; }
    .badge.cloud { background: #1e3a8a; color: #bfdbfe; }
  </style>
</head>
<body>

<header>
  <div class="brand">
    ⚡ Sincrona Edge Bridge <span>Universal IoT Router</span>
  </div>
  <div id="wsIndicator" class="status-pill offline">
    <div class="dot"></div> <span id="wsStatusText">WebSocket: Se conectează...</span>
  </div>
</header>

<main>
  <!-- COLOANA STÂNGA: CONFIGURARE -->
  <div>
    <div class="card">
      <h2>⚙️ Configurare Driver Fiscal</h2>
      
      <div class="field">
        <label for="drvSelect">Driver Fiscal Activ</label>
        <select id="drvSelect" onchange="toggleDriverFields()">
          <option value="erpnet">ErpNet.FP (HTTP REST pe localhost:8001)</option>
          <option value="datecs">Datecs DUDE / FPrinter (Fișiere .inp cu watchdog)</option>
          <option value="tremol">Tremol ZFPServer (Protocol fișiere Bonuri)</option>
        </select>
      </div>

      <!-- Câmpuri specifice ErpNet -->
      <div id="erpnetFields">
        <div class="field">
          <label for="erpnetUrl">Adresă ErpNet.FP</label>
          <input id="erpnetUrl" placeholder="http://127.0.0.1:8001">
        </div>
      </div>

      <!-- Câmpuri specifice Datecs -->
      <div id="datecsFields" style="display:none;">
        <div class="field">
          <label for="datecsFormat">Sintaxă Datecs</label>
          <select id="datecsFormat">
            <option value="dude">Datecs DUDE (Comenzi native 48, 49, 53, 56)</option>
            <option value="fprinter">Datecs FPrinter clasic (S,1,... P,1,...)</option>
          </select>
        </div>
        <div class="field">
          <label for="datecsIn">Director IN Datecs</label>
          <input id="datecsIn" placeholder="C:\Datecs\In">
        </div>
        <div class="field">
          <label for="datecsOut">Director OUT Datecs (Watchdog)</label>
          <input id="datecsOut" placeholder="C:\Datecs\Out">
        </div>
        <div class="field">
          <label for="datecsComPort">Port COM Datecs (pentru extragere automată ANAF)</label>
          <input id="datecsComPort" type="number" placeholder="6">
        </div>
        <div class="field">
          <label for="datecsBaudRate">Baudrate Port COM</label>
          <input id="datecsBaudRate" type="number" placeholder="115200">
        </div>
        <div class="field">
          <label for="datecsAnafDir">Folder Pachete ANAF (.p7b / .xml)</label>
          <input id="datecsAnafDir" placeholder="C:\Datecs\ANAF">
        </div>
        <div class="field" style="display:flex;align-items:center;gap:8px;margin-top:10px;">
          <input id="anafAutoDownload" type="checkbox" style="width:auto;margin:0;">
          <label for="anafAutoDownload" style="margin:0;font-weight:normal;">Descărcare automată lunară a pachetelor ANAF prin DUDE COM la începutul fiecărei luni</label>
        </div>
      </div>

      <!-- Câmpuri specifice Tremol -->
      <div id="tremolFields" style="display:none;">
        <div class="field">
          <label for="tremolIn">Director Bonuri Tremol (IN)</label>
          <input id="tremolIn" placeholder="C:\ServerTremol\Bonuri">
        </div>
        <div class="field">
          <label for="tremolOut">Director Răspuns Tremol (OUT)</label>
          <input id="tremolOut" placeholder="C:\ServerTremol">
        </div>
      </div>

      <h2 style="margin-top:24px;">🧾 Fișiere XML ANAF (rapoarte Z)</h2>
      <div class="field">
        <label for="anafDirs">Foldere urmărite (câte unul pe rând)</label>
        <textarea id="anafDirs" rows="3" style="width:100%;font-family:monospace" placeholder="C:\Datecs\ANAF&#10;C:\ServerTremol\ANAF"></textarea>
        <small>Folderele în care DUDE, utilitarul Tremol sau simulatorul salvează fișierele XML ale rapoartelor Z. Puntea le trimite automat în Sincrona și le atașează la Raportul Z (Facturi › Contabilitate › Rapoarte Z).</small>
      </div>

      <div style="background:rgba(255,255,255,0.03);padding:14px;border-radius:6px;margin-top:12px;border:1px solid var(--border);">
        <strong style="display:block;margin-bottom:8px;font-size:0.95rem;">📥 Extragere Manuală Pachete ANAF (Datecs / ServerTremol)</strong>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
          <div>
            <label for="anafFromDate" style="font-size:0.8rem;display:block;margin-bottom:4px;">De la data</label>
            <input id="anafFromDate" type="date" style="width:100%;">
          </div>
          <div>
            <label for="anafToDate" style="font-size:0.8rem;display:block;margin-bottom:4px;">Până la data</label>
            <input id="anafToDate" type="date" style="width:100%;">
          </div>
        </div>
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
          <button class="secondary" style="padding:6px 12px;font-size:0.8rem;" type="button" onclick="setPrevMonthDates()">📅 Selectează Luna Trecută</button>
          <button class="primary" style="padding:6px 12px;font-size:0.8rem;" type="button" onclick="downloadAnafNow()">⚡ Descarcă Pachete ANAF Acum</button>
        </div>
      </div>

      <h2 style="margin-top:24px;">☁️ Conexiune Cloud (Sincrona)</h2>
      <div class="field">
        <label for="apiUrl">API URL</label>
        <input id="apiUrl" placeholder="https://api.sincrona.ro">
      </div>
      <div class="field">
        <label for="wsUrl">WebSocket URL (Opțional / Auto-detectat)</label>
        <input id="wsUrl" placeholder="wss://api.sincrona.ro/ws/device/fiscal-printer">
      </div>
      <div class="field">
        <label for="deviceToken">Device Token</label>
        <input id="deviceToken" type="password" placeholder="Lăsați gol pentru a păstra token-ul salvat">
      </div>

      <div class="row-actions">
        <button class="primary" onclick="saveConfiguration()">💾 Salvează Configurația</button>
        <button class="secondary" onclick="testDriver()">🔌 Test Driver Local</button>
        <button class="secondary" onclick="testCloud()">🌐 Test Conexiune Cloud</button>
        <button class="secondary" onclick="pollNow()">⚡ Interoghează Joburi Acum</button>
      </div>

      <div id="actionMsg" class="msg-box" style="display:none;"></div>
    </div>
  </div>

  <!-- COLOANA DREAPTA: STATISTICI & LOGURI -->
  <div>
    <div class="card">
      <h2>📊 Stare Sistem în Timp Real</h2>
      <div class="stat-grid">
        <div class="stat-box">
          <div class="stat-label">DRIVER ACTIV</div>
          <div class="stat-val" id="stDriver">-</div>
        </div>
        <div class="stat-box">
          <div class="stat-label">CLOUD POLL</div>
          <div class="stat-val ok" id="stCloud">-</div>
        </div>
        <div class="stat-box">
          <div class="stat-label">BONURI CU SUCCES</div>
          <div class="stat-val ok" id="stJobsOk">0</div>
        </div>
        <div class="stat-box">
          <div class="stat-label">BONURI EȘUATE</div>
          <div class="stat-val fail" id="stJobsFail">0</div>
        </div>
      </div>
      <div class="stat-box" style="margin-bottom:12px;">
        <div class="stat-label">ULTIMUL JOB PROCESAT</div>
        <div class="stat-val" style="font-size:0.9rem;" id="stLastJob">Niciunul</div>
      </div>
      <div class="stat-box">
        <div class="stat-label">ULTIMA EROARE</div>
        <div class="stat-val fail" style="font-size:0.85rem;" id="stLastError">—</div>
      </div>
    </div>

    <div class="card">
      <h2>
        <span>📋 Jurnal Erori</span>
        <button class="danger" style="padding:4px 10px; font-size:0.75rem;" onclick="clearErrorsLog()">Șterge Jurnal</button>
      </h2>
      <div style="max-height: 240px; overflow-y: auto;">
        <table>
          <thead><tr><th>Ora</th><th>Sursă</th><th>Cod</th><th>Mesaj</th></tr></thead>
          <tbody id="errorsTable"><tr><td colspan="4" style="color:var(--muted);">Se încarcă...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>
</main>

<script>
function toggleDriverFields() {
  const d = document.getElementById('drvSelect').value;
  document.getElementById('erpnetFields').style.display = (d === 'erpnet') ? 'block' : 'none';
  document.getElementById('datecsFields').style.display = (d === 'datecs') ? 'block' : 'none';
  document.getElementById('tremolFields').style.display = (d === 'tremol') ? 'block' : 'none';
}

async function loadData() {
  try {
    const res = await fetch('/service/config');
    const c = await res.json();
    document.getElementById('drvSelect').value = c.FISCAL_DRIVER || 'erpnet';
    document.getElementById('erpnetUrl').value = c.ERPNET_URL || '';
    document.getElementById('datecsFormat').value = c.DATECS_FORMAT || 'dude';
    document.getElementById('datecsIn').value = c.DATECS_IN_DIR || '';
    document.getElementById('datecsOut').value = c.DATECS_OUT_DIR || '';
    document.getElementById('datecsComPort').value = c.DATECS_COM_PORT || 6;
    document.getElementById('datecsBaudRate').value = c.DATECS_BAUDRATE || 115200;
    document.getElementById('datecsAnafDir').value = c.DATECS_ANAF_DIR || 'C:\\Datecs\\ANAF';
    document.getElementById('anafAutoDownload').checked = (c.ANAF_AUTO_MONTHLY_DOWNLOAD !== false);
    document.getElementById('tremolIn').value = c.TREMOL_IN_DIR || '';
    document.getElementById('tremolOut').value = c.TREMOL_OUT_DIR || '';
    document.getElementById('anafDirs').value = (Array.isArray(c.ANAF_XML_DIRS) ? c.ANAF_XML_DIRS : String(c.ANAF_XML_DIRS || '').split(';')).filter(Boolean).join('\n');
    document.getElementById('apiUrl').value = c.API_URL || '';
    document.getElementById('wsUrl').value = c.WS_URL || '';
    setPrevMonthDates();
    toggleDriverFields();
  } catch(e) {}

  updateStatus();
  updateErrors();
}

async function updateStatus() {
  try {
    const res = await fetch('/service/status');
    const s = await res.json();
    const ind = document.getElementById('wsIndicator');
    const txt = document.getElementById('wsStatusText');
    if (s.connected_ws) {
      ind.className = 'status-pill online';
      txt.textContent = 'Cloud: WebSocket Conectat';
    } else if (s.connected_cloud) {
      ind.className = 'status-pill online';
      txt.textContent = 'Cloud: REST Polling Activ';
    } else {
      ind.className = 'status-pill offline';
      txt.textContent = 'Cloud: Deconectat';
    }

    document.getElementById('stDriver').textContent = (s.active_driver || 'erpnet').toUpperCase();
    document.getElementById('stCloud').textContent = s.connected_cloud ? 'ACTIV' : 'OFFLINE';
    document.getElementById('stCloud').className = 'stat-val ' + (s.connected_cloud ? 'ok' : 'fail');
    document.getElementById('stJobsOk').textContent = s.total_jobs_ok || 0;
    document.getElementById('stJobsFail').textContent = s.total_jobs_failed || 0;
    document.getElementById('stLastJob').textContent = s.last_job_id ? (s.last_job_id + ' (' + s.last_job_status + ')') : 'Niciunul';
    document.getElementById('stLastError').textContent = s.last_error || 'Nicio eroare';
  } catch(e) {}
}

async function updateErrors() {
  try {
    const res = await fetch('/service/errors');
    const data = await res.json();
    const rows = data.errors || [];
    const tbody = document.getElementById('errorsTable');
    if (rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" style="color:var(--muted);">Nicio eroare înregistrată.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r => `
      <tr>
        <td>${(r.timestamp||'').split('T')[1]||r.timestamp}</td>
        <td><span class="badge ${r.source==='cloud'?'cloud':'driver'}">${r.source}</span></td>
        <td><code>${r.code}</code></td>
        <td title="${r.message}">${r.message.substring(0, 45)}...</td>
      </tr>
    `).join('');
  } catch(e) {}
}

async function saveConfiguration() {
  const body = {
    FISCAL_DRIVER: document.getElementById('drvSelect').value,
    ERPNET_URL: document.getElementById('erpnetUrl').value.trim(),
    DATECS_FORMAT: document.getElementById('datecsFormat').value,
    DATECS_IN_DIR: document.getElementById('datecsIn').value.trim(),
    DATECS_OUT_DIR: document.getElementById('datecsOut').value.trim(),
    DATECS_COM_PORT: parseInt(document.getElementById('datecsComPort').value) || 6,
    DATECS_BAUDRATE: parseInt(document.getElementById('datecsBaudRate').value) || 115200,
    DATECS_ANAF_DIR: document.getElementById('datecsAnafDir').value.trim() || 'C:\\Datecs\\ANAF',
    ANAF_AUTO_MONTHLY_DOWNLOAD: document.getElementById('anafAutoDownload').checked,
    TREMOL_IN_DIR: document.getElementById('tremolIn').value.trim(),
    TREMOL_OUT_DIR: document.getElementById('tremolOut').value.trim(),
    ANAF_XML_DIRS: document.getElementById('anafDirs').value.split(/\r?\n/).map(x => x.trim()).filter(Boolean),
    API_URL: document.getElementById('apiUrl').value.trim(),
    WS_URL: document.getElementById('wsUrl').value.trim()
  };
  const tok = document.getElementById('deviceToken').value.trim();
  if (tok) body.DEVICE_TOKEN = tok;

  const box = document.getElementById('actionMsg');
  box.style.display = 'block';
  box.textContent = 'Salvare în curs...';
  try {
    const res = await fetch('/service/config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    const jr = await res.json();
    box.textContent = '✓ Configurație salvată cu succes!\n' + JSON.stringify(jr, null, 2);
    document.getElementById('deviceToken').value = '';
    updateStatus();
  } catch(e) {
    box.textContent = '✗ Eroare la salvare: ' + e.message;
  }
}

function setPrevMonthDates() {
  const d = new Date();
  const firstCur = new Date(d.getFullYear(), d.getMonth(), 1);
  const lastPrev = new Date(firstCur.getTime() - 86400000);
  const firstPrev = new Date(lastPrev.getFullYear(), lastPrev.getMonth(), 1);
  const fmt = dt => {
    const y = dt.getFullYear();
    const m = String(dt.getMonth() + 1).padStart(2, '0');
    const day = String(dt.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  };
  const elFrom = document.getElementById('anafFromDate');
  const elTo = document.getElementById('anafToDate');
  if (elFrom && !elFrom.value) elFrom.value = fmt(firstPrev);
  if (elTo && !elTo.value) elTo.value = fmt(lastPrev);
}

async function downloadAnafNow() {
  const from_date = (document.getElementById('anafFromDate').value || '').trim();
  const to_date = (document.getElementById('anafToDate').value || '').trim();
  const box = document.getElementById('actionMsg');
  box.style.display = 'block';
  box.textContent = `Descărcare/export pachete ANAF (${from_date} -> ${to_date}) în curs...`;
  try {
    const res = await fetch('/service/download-anaf', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ from_date, to_date })
    });
    const jr = await res.json();
    box.textContent = (jr.ok ? '✓ Descărcare ANAF finalizată cu succes:\n' : '✗ Eroare descărcare ANAF:\n') + JSON.stringify(jr, null, 2);
    updateStatus();
  } catch(e) {
    box.textContent = '✗ Eroare apel: ' + e.message;
  }
}

async function testDriver() {
  const box = document.getElementById('actionMsg');
  box.style.display = 'block';
  box.textContent = 'Testare driver local în curs...';
  try {
    const res = await fetch('/service/test-driver', { method: 'POST' });
    const jr = await res.json();
    box.textContent = (jr.ok ? '✓ Driver Test OK:\n' : '✗ Eroare Driver:\n') + JSON.stringify(jr, null, 2);
  } catch(e) {
    box.textContent = '✗ Eroare test: ' + e.message;
  }
}

async function testCloud() {
  const box = document.getElementById('actionMsg');
  box.style.display = 'block';
  box.textContent = 'Testare conexiune Cloud Sincrona...';
  try {
    const res = await fetch('/service/test-cloud', { method: 'POST' });
    const jr = await res.json();
    box.textContent = (jr.ok ? '✓ Conexiune Cloud OK:\n' : '✗ Conexiune Eșuată:\n') + JSON.stringify(jr, null, 2);
  } catch(e) {
    box.textContent = '✗ Eroare conexiune: ' + e.message;
  }
}

async function pollNow() {
  const box = document.getElementById('actionMsg');
  box.style.display = 'block';
  box.textContent = 'Interogare coadă de joburi pe Sincrona Cloud...';
  try {
    const res = await fetch('/service/poll-now', { method: 'POST' });
    const jr = await res.json();
    box.textContent = (jr.job_found ? '✓ Job recepționat și procesat:\n' : 'ℹ Coada este goală (nicio comandă în așteptare):\n') + JSON.stringify(jr, null, 2);
    updateStatus();
  } catch(e) {
    box.textContent = '✗ Eroare interogare coadă: ' + e.message;
  }
}

async function clearErrorsLog() {
  if (!confirm('Doriți să ștergeți jurnalul de erori?')) return;
  await fetch('/service/clear-errors', { method: 'POST' });
  updateErrors();
}

loadData();
setInterval(updateStatus, 3000);
setInterval(updateErrors, 6000);
</script>
</body>
</html>
"""

class AdminHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Evită spamming-ul consolei de către requesturile de polling

    def _origin(self) -> str:
        return (self.headers.get("Origin") or "").strip()

    def _same_origin(self) -> bool:
        # panoul de administrare (http://127.0.0.1:8002) sau un program local fără Origin
        o = self._origin()
        return not o or re.match(r"^http://(127\.0\.0\.1|localhost):8002$", o) is not None

    def _cors(self):
        o = self._origin()
        if o and LOCAL_ORIGIN_RE.match(o):
            self.send_header("Access-Control-Allow-Origin", o)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            # Chrome: o pagină de pe internet care vorbește cu un program de pe calculator (Private Network Access)
            self.send_header("Access-Control-Allow-Private-Network", "true")
            self.send_header("Access-Control-Max-Age", "600")

    def _send_json(self, code: int, data: dict):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = min(int(self.headers.get("Content-Length", 0) or 0), 1_000_000)
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode("utf-8") or "{}")

    def do_OPTIONS(self):
        if not LOCAL_ORIGIN_RE.match(self._origin()) or not urlparse(self.path).path.startswith("/local/"):
            self.send_response(403)
            self.end_headers()
            return
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _local_post(self, path: str):
        if not (LOCAL_ORIGIN_RE.match(self._origin()) or self._same_origin()):
            self._send_json(403, {"ok": False, "error": "Origine nepermisă"})
            return
        try:
            body = self._read_json()
        except ValueError:
            self._send_json(400, {"ok": False, "error": "JSON invalid"})
            return
        ref = str(body.get("client_ref") or "")
        if not CLIENT_REF_RE.match(ref):
            self._send_json(400, {"ok": False, "error": "client_ref invalid"})
            return
        prof = offline_profile()
        if not prof:
            self._send_json(409, {"ok": False, "error": "Puntea nu are încă setările casei de marcat. Porniți-o o dată cu internet (se descarcă singure din Sincrona)."})
            return
        try:
            if path == "/local/receipt":
                built = build_offline_receipt(body, prof)
                rec = run_offline_job("fiscal_receipt", ref, built["payload"], {
                    "label": str(body.get("label") or "Bon fiscal offline")[:120], "total": built["total"],
                    "lines": body.get("lines") or [], "payments": body.get("payments") or [], "source": body.get("source") or {}})
            else:
                rtype = str(body.get("type") or "x").lower()
                if rtype not in ("x", "z"):
                    raise FiscalException("REPORT_TYPE", "Raportul poate fi X sau Z.")
                payload = {"erpnet_url": prof.get("erpnet_url") or get_config().get("ERPNET_URL"), "printer_id": prof.get("printer_id") or "default",
                           "mode": prof.get("mode") or "test", "test_transport": prof.get("test_transport") or "local",
                           "endpoint": f"{rtype}report", "body": {}}
                rec = run_offline_job(f"{rtype}_report", ref, payload, {"label": f"Raport {rtype.upper()} (offline)", "total": 0})
        except FiscalException as exc:
            self._send_json(400, {"ok": False, "error": exc.text, "code": exc.code})
            return
        ok = rec.get("status") == "completed"
        res = rec.get("result") or {}
        self._send_json(200 if ok else 502, {"ok": ok, "status": rec.get("status"), "receipt_number": res.get("receiptNumber"),
                                             "error": None if ok else res.get("text"), "code": None if ok else res.get("error"), "record": {
                                                 k: rec.get(k) for k in ("client_ref", "kind", "label", "total", "status", "at")}})

    def _send_html(self, html: str):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/local/status":
            if not (LOCAL_ORIGIN_RE.match(self._origin()) or self._same_origin()):
                self._send_json(403, {"ok": False})
                return
            self._send_json(200, local_status())
            return
        if not self._same_origin():
            self._send_json(403, {"error": "Origine nepermisă"})
            return
        if path in ("/", "/index.html", "/admin"):
            self._send_html(ADMIN_HTML)
        elif path == "/service/status":
            self._send_json(200, get_state())
        elif path == "/service/config":
            cfg = get_config()
            # Mascare token
            tok = cfg.get("DEVICE_TOKEN", "")
            masked = (tok[:6] + "..." + tok[-4:]) if len(tok) > 10 else ("***" if tok else "")
            resp = dict(cfg)
            resp["DEVICE_TOKEN"] = masked
            self._send_json(200, resp)
        elif path == "/service/errors":
            self._send_json(200, {"errors": read_errors(100)})
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ("/local/receipt", "/local/report", "/local/download-anaf"):
            if path == "/local/download-anaf":
                if not (LOCAL_ORIGIN_RE.match(self._origin()) or self._same_origin()):
                    self._send_json(403, {"ok": False, "error": "Origine nepermisă"})
                    return
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b"{}"
                data = {}
                if raw:
                    try:
                        data = json.loads(raw.decode("utf-8"))
                    except Exception:
                        pass
                res = trigger_anaf_download(data.get("from_date", ""), data.get("to_date", ""))
                self._send_json(200 if res.get("ok") else 500, res)
                return
            self._local_post(path)
            return
        # setările punții se schimbă doar din panoul ei (un site străin nu poate redirecționa tokenul casei)
        if not self._same_origin():
            self._send_json(403, {"ok": False, "error": "Origine nepermisă"})
            return

        if path == "/service/download-anaf":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            data = {}
            if raw:
                try:
                    data = json.loads(raw.decode("utf-8"))
                except Exception:
                    pass
            from_date = data.get("from_date") or data.get("startDate") or ""
            to_date = data.get("to_date") or data.get("endDate") or ""
            res = trigger_anaf_download(from_date, to_date)
            self._send_json(200 if res.get("ok") else 500, res)

        elif path == "/service/config":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
            except Exception as e:
                self._send_json(400, {"ok": False, "error": f"JSON Invalid: {e}"})
                return

            saved = save_config(data)
            logger.info(f"[Admin] Configurație actualizată: driver={saved.get('FISCAL_DRIVER')}")
            self._send_json(200, {"ok": True, "saved": saved})

        elif path == "/service/test-driver":
            driver = FiscalRouter.get_driver()
            try:
                res = driver.test_connection()
                self._send_json(200, res)
            except Exception as e:
                self._send_json(200, {"ok": False, "error": str(e)})

        elif path == "/service/test-cloud":
            cfg = get_config()
            api = cfg.get("API_URL", "").rstrip("/")
            if not api:
                self._send_json(400, {"ok": False, "error": "API_URL nu este configurat."})
                return
            try:
                r = requests.get(f"{api}/api/device/fiscal-printer/status", headers=get_headers(), timeout=10)
                r.raise_for_status()
                data = r.json() if r.content else {}
                self._send_json(200, {"ok": True, "status": r.status_code, "data": data})
            except Exception as e:
                self._send_json(200, {"ok": False, "error": f"{type(e).__name__}: {e}"})

        elif path == "/service/poll-now":
            cfg = get_config()
            api = cfg.get("API_URL", "").rstrip("/")
            if not api:
                self._send_json(400, {"ok": False, "error": "API_URL nu este configurat."})
                return
            try:
                r = requests.get(f"{api}/api/device/fiscal-printer/jobs", headers=get_headers(), timeout=12)
                r.raise_for_status()
                data = r.json() if r.content else {}
                job = data.get("job")
                processed = False
                res = None
                if job:
                    job_id = job.get("id")
                    claimed, rec = claim_job(job) if job_id else (False, None)
                    if not claimed and rec:
                        processed = resend_finished(job, rec)
                        res = rec.get("result")
                    if claimed:
                        update_state(last_job_id=job_id, last_job_status="processing")
                        try:
                            res = FiscalRouter.execute_job(job)
                            finish_job(job, "completed", res)
                            update_state(total_jobs_ok=get_state()["total_jobs_ok"] + 1, last_job_status="completed")
                            complete_job_http(job_id, "completed", res)
                            processed = True
                        except Exception as err:
                            err_code = getattr(err, "code", "EXECUTION_ERROR")
                            err_text = getattr(err, "text", str(err))
                            finish_job(job, "failed", {"error": err_code, "text": err_text})
                            update_state(total_jobs_failed=get_state()["total_jobs_failed"] + 1, last_job_status="failed", last_error=f"{err_code}: {err_text}")
                            complete_job_http(job_id, "failed", {"error": err_code, "text": err_text})
                            res = {"error": err_code, "text": err_text}
                self._send_json(200, {"ok": True, "job_found": bool(job), "job": job, "processed": processed, "result": res})
            except Exception as e:
                self._send_json(200, {"ok": False, "error": f"{type(e).__name__}: {e}"})

        elif path == "/service/clear-errors":
            clear_errors()
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "Endpoint inexistent"})

def start_admin_server(port: int = 8002):
    try:
        # doar pe acest calculator (panoul și bonurile offline nu se deschid din rețea); ADMIN_BIND=0.0.0.0 pentru acces din LAN
        bind = get_config().get("ADMIN_BIND") or "127.0.0.1"
        server = http.server.ThreadingHTTPServer((bind, port), AdminHTTPRequestHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True, name="AdminWeb8002")
        t.start()
        logger.info(f"[Admin] Panou web pornit pe http://127.0.0.1:{port}/")
        return server
    except Exception as e:
        logger.error(f"[Admin] Nu s-a putut deschide portul {port}: {e}")
        return None


# ==================== MAIN ENTRY POINT ====================

# ==================== FIȘIERE XML ANAF (A4200) ====================

class AnafXmlWatcher:
    """Urmărește folderele ANAF_XML_DIRS și trimite fișierele noi (.xml / .p7m / .p7s / .zip) la Sincrona.
    Evidența (anaf_xml_sent.json) reține amprenta fiecărui fișier: un fișier atașat nu se mai retrimite;
    unul pentru care Raportul Z nu există încă în Sincrona se reîncearcă la 10 minute."""

    EXT = (".xml", ".p7m", ".p7s", ".zip")
    RETRY_S = 600
    MAX_BYTES = 15 * 1024 * 1024

    def __init__(self):
        self.path = os.path.join(BASE_DIR, "anaf_xml_sent.json")
        self.ledger = {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.ledger = json.load(f)
        except Exception:
            self.ledger = {}

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.ledger, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.path)

    @staticmethod
    def dirs():
        raw = get_config().get("ANAF_XML_DIRS") or []
        if isinstance(raw, str):
            raw = [p for p in re.split(r"[;\n]", raw) if p.strip()]
        result = [os.path.expandvars(os.path.expanduser(str(p).strip())) for p in raw if str(p).strip()]
        # Fallback pe folderele standard dacă există pe disc și nu sunt adăugate deja
        for fallback in (get_config().get("DATECS_ANAF_DIR", r"C:\Datecs\ANAF"), r"C:\Datecs\ANAF", r"C:\ServerTremol\ANAF", r"C:\ServerTremol\RapoarteTXT", r"C:\ServerTremol\Bonuri\done"):
            if os.path.isdir(fallback) and fallback not in result:
                result.append(fallback)
        return result

    def check_auto_monthly_download(self):
        """Verifică dacă a început o lună nouă și descarcă automat extrasele ANAF pentru luna precedentă."""
        cfg = get_config()
        if not cfg.get("ANAF_AUTO_MONTHLY_DOWNLOAD", True):
            return

        now_ts = time.time()
        if hasattr(self, "_last_auto_check") and (now_ts - self._last_auto_check < 3600):
            return
        self._last_auto_check = now_ts

        from_date, to_date = get_previous_month_range()
        month_key = from_date[:7]
        hist_path = os.path.join(BASE_DIR, "anaf_monthly_downloads.json")
        history = {}
        if os.path.isfile(hist_path):
            try:
                with open(hist_path, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = {}

        last_entry = history.get(month_key)
        if last_entry and last_entry.get("status") == "ok":
            return

        # Dacă a dat eroare recent, reîncercăm după 6 ore pentru a nu bloca portul COM
        if last_entry and (now_ts - float(last_entry.get("ts", 0)) < 21600):
            return

        logger.info(f"[ANAF AUTO] Declanșare descărcare automată pachete ANAF pentru luna precedentă {month_key} ({from_date} -> {to_date})...")
        res = trigger_anaf_download(from_date, to_date)
        history[month_key] = {
            "ts": now_ts,
            "from_date": from_date,
            "to_date": to_date,
            "status": "ok" if res.get("ok") else "error",
            "error": res.get("error") if not res.get("ok") else None,
            "details": res
        }
        try:
            with open(hist_path, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            logger.warning(f"[ANAF AUTO] Nu s-a putut salva istoricul descărcărilor ANAF: {e}")

    def candidates(self):
        max_age = float(get_config().get("ANAF_XML_MAX_AGE_DAYS", 45)) * 86400
        limit = time.time() - max_age
        for root_dir in self.dirs():
            if not os.path.isdir(root_dir):
                continue
            for dirpath, _dirs, files in os.walk(root_dir):
                for name in files:
                    if not name.lower().endswith(self.EXT):
                        continue
                    full = os.path.join(dirpath, name)
                    try:
                        st = os.stat(full)
                    except OSError:
                        continue
                    # fișier încă în scriere / prea vechi / prea mare
                    if st.st_mtime < limit or time.time() - st.st_mtime < 5 or st.st_size == 0 or st.st_size > self.MAX_BYTES:
                        continue
                    yield full, name

    def scan_once(self):
        cfg = get_config()
        api_url = cfg.get("API_URL", "").rstrip("/")
        if not api_url or not cfg.get("DEVICE_TOKEN"):
            return
        changed = False
        for full, name in self.candidates():
            try:
                with open(full, "rb") as f:
                    data = f.read()
            except OSError:
                continue
            digest = hashlib.sha256(data).hexdigest()
            rec = self.ledger.get(digest) or {}
            if rec.get("status") in ("attached", "duplicate", "already_attached", "invalid"):
                continue
            if rec and time.time() - float(rec.get("ts", 0)) < self.RETRY_S:
                continue
            try:
                r = requests.post(
                    f"{api_url}/api/device/fiscal-printer/a4200",
                    headers=get_headers(),
                    json={"filename": name, "content_b64": base64.b64encode(data).decode("ascii")},
                    timeout=30,
                )
                r.raise_for_status()
                results = r.json().get("results") or []
            except Exception as e:
                log_error("anaf_xml", "UPLOAD_FAIL", f"Trimiterea {name} a eșuat: {e}")
                self.ledger[digest] = {"path": full, "status": "error", "ts": time.time()}
                changed = True
                continue
            statuses = {x.get("status") for x in results}
            status = ("attached" if "attached" in statuses else
                      "not_found" if "not_found" in statuses else
                      next(iter(statuses), "invalid"))
            self.ledger[digest] = {"path": full, "status": status, "ts": time.time(),
                                   "z": [z.get("z_number") for x in results for z in (x.get("z_reports") or [])]}
            changed = True
            if status == "attached":
                logger.info(f"[ANAF XML] {name} atașat la Raportul Z {self.ledger[digest]['z']}")
            elif status == "not_found":
                logger.info(f"[ANAF XML] {name}: Raportul Z nu e încă în Sincrona, reîncerc mai târziu")
            elif status == "invalid":
                log_error("anaf_xml", "INVALID", f"{name}: {results[0].get('message') if results else 'fișier invalid'}")
        if changed:
            # evidența păstrează doar ultimele 5000 de fișiere
            if len(self.ledger) > 5000:
                for k in sorted(self.ledger, key=lambda k: self.ledger[k].get("ts", 0))[:-5000]:
                    self.ledger.pop(k, None)
            try:
                self._save()
            except Exception as e:
                logger.error(f"[ANAF XML] Nu pot salva evidența: {e}")
        update_state(anaf_xml_dirs=self.dirs(), anaf_xml_last_scan=datetime.now().isoformat(timespec="seconds"),
                     anaf_xml_attached=sum(1 for v in self.ledger.values() if v.get("status") == "attached"),
                     anaf_xml_waiting=sum(1 for v in self.ledger.values() if v.get("status") in ("not_found", "error")))

    def run(self):
        while True:
            try:
                self.check_auto_monthly_download()
            except Exception as e:
                logger.warning(f"[ANAF AUTO] Eroare verificare descărcare automată: {e}")

            try:
                if self.dirs():
                    self.scan_once()
            except Exception as e:
                log_error("anaf_xml", "SCAN_FAIL", f"Eroare la scanarea fișierelor ANAF: {e}")
            time.sleep(max(15, int(get_config().get("ANAF_XML_SCAN_SECONDS", 60) or 60)))


def main():
    logger.info("==================================================")
    logger.info(" Sincrona Enterprise IoT Edge Bridge - Starting   ")
    logger.info(f" Base Directory: {BASE_DIR}")
    logger.info(f" Config File:    {CONFIG_FILE}")
    logger.info("==================================================")

    # 1. Pornire Panou Admin pe 8002
    start_admin_server(8002)

    # 2. Pornire Client WebSocket Rezilient
    ws_client = ResilientWebSocketClient()
    ws_client.start()

    # 3. Pornire HTTP Polling Worker (redundant / fallback)
    poll_thread = threading.Thread(target=run_http_polling, daemon=True, name="HTTPPollWorker")
    poll_thread.start()

    # 4. Mod offline: setările casei din Sincrona + trimiterea bonurilor emise fără internet
    threading.Thread(target=run_offline_worker, daemon=True, name="OfflineWorker").start()

    # 4. Preluare automată fișiere XML ANAF (rapoarte Z) din folderele configurate
    threading.Thread(target=AnafXmlWatcher().run, daemon=True, name="AnafXmlWatcher").start()

    # Buclă de supraveghere principală
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("[Main] Oprire solicitată. Ieșire.")
        sys.exit(0)

if __name__ == "__main__":
    main()
