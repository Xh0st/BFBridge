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
import glob
import http.server
import json
import logging
import os
import re
import socket
import sys
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime
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
    "TREMOL_IN_DIR": r"C:\ServerTremol\Bonuri",
    "TREMOL_OUT_DIR": r"C:\ServerTremol",
    "POLL_FALLBACK": True,
    "TIMEOUT_SECONDS": 45,
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

        endpoint = job_payload.get("endpoint") or "receipt"
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

    def test_connection(self) -> dict:
        in_ok = os.path.isdir(self.in_dir) and os.access(self.in_dir, os.W_OK)
        out_ok = os.path.isdir(self.out_dir) and os.access(self.out_dir, os.R_OK)
        return {
            "ok": in_ok and out_ok,
            "in_dir": self.in_dir,
            "in_dir_writable": in_ok,
            "out_dir": self.out_dir,
            "out_dir_readable": out_ok,
            "format": self.format_mode
        }

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
            raise FiscalException("DATECS_TIMEOUT", f"Nu s-a primit fișier de răspuns în {self.out_dir} în {self.timeout_s}s.")

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
        recipient = receipt.get("recipient") or {}
        customer = receipt.get("customer") or {}
        tax_num = str(
            payload.get("buyer_tax_id")
            or recipient.get("identifier")
            or recipient.get("vatNumber")
            or customer.get("tax_number")
            or ""
        ).strip().upper()
        tax_digits = tax_num[2:] if tax_num.startswith("RO") else tax_num

        # Comanda 48: Deschidere bon fiscal (I + CUI dacă este persoană juridică)
        if tax_digits:
            lines.append(f"48,1\t0001\t1\tI\t{tax_digits}\t")
        else:
            lines.append("48,1\t0001\t1\t\t\t")

        # Comanda 49: Înregistrare articole
        items = receipt.get("items") or receipt.get("lines") or []
        for it in items:
            name = (it.get("text") or it.get("name") or it.get("description") or "Articol").strip()[:36].replace("\t", " ")
            vat = it.get("taxGroup") or it.get("vat_group") or it.get("vatGroup") or 1
            raw_price = it.get("unitPrice") if it.get("unitPrice") is not None else it.get("price", 0)
            price = f"{float(raw_price):.2f}"
            qty = f"{float(it.get('quantity', 1)):.3f}"
            dep = it.get("department", 1)
            um = (it.get("unit") or "BUC.")[:4]
            lines.append(f"49,{name}\t{vat}\t{price}\t{qty}\t\t\t{dep}\t{um}\t")

        # Comanda 53: Înregistrare plăți
        # Mapare tip plată Datecs DUDE: 0=Numerar, 1=Card, 2=Tichete Masă, 3=Tichete Cadou, 4=Credit, etc.
        payment_map = {
            "cash": 0,
            "numerar": 0,
            "card": 1,
            "pos": 1,
            "ticket": 2,
            "tichet": 2,
            "tichete": 2,
            "voucher": 3,
            "credit": 4,
            "bank": 8,
            "virament": 8,
            "op": 8,
            "other": 8,
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
        recipient = receipt.get("recipient") or {}
        customer = receipt.get("customer") or {}
        tax_num = str(
            payload.get("buyer_tax_id")
            or recipient.get("identifier")
            or recipient.get("vatNumber")
            or customer.get("tax_number")
            or ""
        ).strip().upper()
        tax_digits = tax_num[2:] if tax_num.startswith("RO") else tax_num
        if tax_digits:
            lines.append(f"CF,1,______,_,__;{tax_digits};")

        items = receipt.get("items") or receipt.get("lines") or []
        for it in items:
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

    def print_receipt(self, job_payload: dict, task_id: str) -> dict:
        receipt = job_payload.get("receipt") or job_payload.get("body") or {}
        base_name = f"bon_{int(time.time())}_{task_id[-8:]}"

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
        # Redenumire atomică pentru a preveni citirea parțială de către driver
        if os.path.exists(inp_file):
            os.remove(inp_file)
        os.rename(tmp_file, inp_file)

        # Așteaptă confirmarea în folderul OUT
        res_text = self._wait_for_response(base_name)
        logger.info(f"[Datecs] Răspuns primit pentru {base_name}: {res_text[:100]}")

        # Parsare erori: dacă conține ERR sau -1
        if "ERR" in res_text.upper() or "ERROR" in res_text.upper():
            raise FiscalException("DATECS_REJECTED", f"Datecs a raportat eroare: {res_text}")

        return {"ok": True, "raw_response": res_text, "file": base_name}

    def print_report(self, report_type: str, job_payload: dict, task_id: str) -> dict:
        low_type = report_type.lower().replace("_", "")
        base_name = f"rep_{low_type}_{int(time.time())}"

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
        os.rename(tmp_file, inp_file)

        res_text = self._wait_for_response(base_name)
        return {"ok": True, "raw_response": res_text, "report": report_type}


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

    def test_connection(self) -> dict:
        in_ok = os.path.isdir(self.in_dir) and os.access(self.in_dir, os.W_OK)
        out_ok = os.path.isdir(self.out_dir) and os.access(self.out_dir, os.R_OK)
        return {
            "ok": in_ok and out_ok,
            "in_dir": self.in_dir,
            "in_dir_writable": in_ok,
            "out_dir": self.out_dir,
            "out_dir_readable": out_ok
        }

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

        while time.time() < deadline:
            # 1. Verificăm dacă ServerTremol a marcat ca eroare sau sintaxă greșită
            if os.path.exists(err_file) or os.path.exists(bad_file):
                resp_info = ""
                rasp_path = os.path.join(self.out_dir, "raspuns.txt")
                if os.path.exists(rasp_path):
                    try:
                        with open(rasp_path, "r", encoding="cp1250", errors="replace") as f:
                            resp_info = f.read().strip()
                    except Exception:
                        pass
                raise FiscalException("TREMOL_ERROR", f"Tremol a respins fișierul ({'bad' if os.path.exists(bad_file) else 'error'}). Răspuns: {resp_info}")

            # 2. Verificăm dacă fișierul a fost finalizat cu succes în done/
            if os.path.exists(done_file):
                resp_info = "OK (Executat cu succes în Bonuri/done)"
                rasp_path = os.path.join(self.out_dir, "raspuns.txt")
                if os.path.exists(rasp_path):
                    try:
                        with open(rasp_path, "r", encoding="cp1250", errors="replace") as f:
                            c = f.read().strip()
                            if c:
                                resp_info = c
                    except Exception:
                        pass
                return resp_info

            # 3. Verificăm dacă raspuns.txt a fost modificat
            for p in candidates:
                if os.path.exists(p):
                    mtime = os.path.getmtime(p)
                    if mtime > init_mtimes.get(p, 0):
                        time.sleep(0.3)
                        with open(p, "r", encoding="cp1250", errors="replace") as f:
                            ans = f.read().strip()
                            if "ERR" in ans.upper() or "ERROR" in ans.upper():
                                raise FiscalException("TREMOL_REJECTED", ans)
                            return ans
            time.sleep(0.4)

        # 4. Dacă fișierul a dispărut din IN (a fost preluat), considerăm succes dacă nu e în error
        in_file = os.path.join(self.in_dir, f"{job_name}.txt")
        if not os.path.exists(in_file):
            if os.path.exists(err_file) or os.path.exists(bad_file):
                raise FiscalException("TREMOL_ERROR", "Fișierul a fost mutat în error/bad de către ServerTremol")
            return "OK (Fișier prelucrat de Tremol)"

        raise FiscalException("TREMOL_TIMEOUT", f"Tremol nu a procesat bonul în {self.timeout_s}s. Verificați că ServerTremol.exe rulează și casa este conectată în REG.OPER.")

    def print_receipt(self, job_payload: dict, task_id: str) -> dict:
        """
        Construiește bon fiscal conform specificației oficiale ServerTremol:
        Linia 1: C|<cui>|<denumire>|<adresa> (obligatoriu prima linie dacă există CUI)
        Linii V: V|<denumire produs>~<um>|<tva 0-5>|<pret>|<cantitate>|<-discount>|<tip disc>|<dep>
        Linia U: U|0|0 (subtotal)
        Linii P: P|<tip plata>|<suma> (0=Numerar, 1=Card, 3=Tichete, 5=Voucher, 8=Alte)
        """
        receipt = job_payload.get("receipt") or job_payload.get("body") or {}
        job_name = f"bon.{int(time.time())}_{task_id[-6:]}"
        lines = []

        # CUI client dacă există conform sintaxă Tremol:
        # C|<cui - max 15>|<denumire - max 30>|<adresa - max 30>
        recipient = receipt.get("recipient") or {}
        customer = receipt.get("customer") or {}
        cui = str(
            job_payload.get("buyer_tax_id")
            or receipt.get("buyer_tax_id")
            or recipient.get("identifier")
            or recipient.get("vatNumber")
            or customer.get("tax_number")
            or ""
        ).strip().upper()

        if cui:
            name = (recipient.get("name") or customer.get("name") or job_payload.get("buyer_name") or "Client").strip()[:30].replace("|", " ")
            addr = (recipient.get("address") or customer.get("address") or job_payload.get("buyer_address") or "-").strip()[:30].replace("|", " ")
            cui_clean = cui[:15].replace("|", "")
            lines.append(f"C|{cui_clean}|{name}|{addr}")

        # Articole conform DocumentatieFormatServerTremol.txt:
        # V|<denumire produs>[max 32]`sau~<um max 3>|<tva 0-5>|<pret>|<cantitate>|<-discount>|<tip discount>|<departament>
        items = receipt.get("items") or receipt.get("lines") or []
        for it in items:
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
        os.rename(tmp_file, target_file)

        res = self._wait_for_response(job_name)
        return {"ok": True, "result": res}

    def print_report(self, report_type: str, job_payload: dict, task_id: str) -> dict:
        """
        Execută rapoarte fiscale Tremol:
        - Raport Z zilnic (bon.z.raport.txt cu comanda Z)
        - Raport X informativ (bon.x.raport.txt cu comanda X)
        - Raport periodic memorie fiscală pe date (F|DDMMYY|DDMMYY|0=sumar/1=detaliat)
        """
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
            file_name = f"bon.z.raport_{int(time.time())}.txt" if is_z else f"bon.x.raport_{int(time.time())}.txt"

        target_path = os.path.join(self.in_dir, file_name)
        tmp_path = os.path.join(self.in_dir, f"{file_name}.tmp")

        with open(tmp_path, "w", newline="", encoding="cp1250", errors="replace") as f:
            f.write(cmd)
        if os.path.exists(target_path):
            os.remove(target_path)
        os.rename(tmp_path, target_path)

        res = self._wait_for_response(file_name.replace(".txt", ""))
        return {"ok": True, "result": res, "report": report_type}


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

    @classmethod
    def execute_job(cls, job: dict) -> dict:
        driver = cls.get_driver()
        task_id = job.get("idempotency_key") or job.get("id") or str(int(time.time()))
        kind = job.get("kind", "receipt").lower()
        payload = job.get("payload", {})

        norm_kind = kind.replace("_", "")
        logger.info(f"[Router] Procesare job {job.get('id')} cu driver {driver.__class__.__name__} (kind={kind})")
        if norm_kind in ("xreport", "zreport", "periodreport") or "report" in norm_kind or payload.get("endpoint") in ("xreport", "zreport", "periodreport"):
            rep_type = payload.get("endpoint") or norm_kind
            return driver.print_report(rep_type, payload, task_id)
        else:
            return driver.print_receipt(payload, task_id)


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
        r.raise_for_status()
        logger.info(f"[Cloud] Job {job_id} marcat ca {status} pe cloud.")
    except Exception as e:
        log_error("cloud", "COMPLETE_FAIL", f"Eroare la confirmarea jobului {job_id}: {e}")

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
                            update_state(last_job_id=job_id, last_job_status="processing")
                            try:
                                res = FiscalRouter.execute_job(job)
                                update_state(
                                    total_jobs_ok=get_state()["total_jobs_ok"] + 1,
                                    last_job_status="completed"
                                )
                                # Răspuns prin WS și redundanță HTTP
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
    """Rulează în fundal pentru a prelua joburi prin REST dacă WS nu este activ."""
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
                    logger.info(f"[PollWorker] Job recepționat prin HTTP: {job_id}")
                    update_state(last_job_id=job_id, last_job_status="processing")
                    try:
                        res = FiscalRouter.execute_job(job)
                        update_state(
                            total_jobs_ok=get_state()["total_jobs_ok"] + 1,
                            last_job_status="completed"
                        )
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
                        complete_job_http(job_id, "failed", {"error": err_code, "text": err_text})
            else:
                update_state(connected_cloud=False)
        except Exception as e:
            update_state(connected_cloud=False)
        time.sleep(3)


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
    document.getElementById('tremolIn').value = c.TREMOL_IN_DIR || '';
    document.getElementById('tremolOut').value = c.TREMOL_OUT_DIR || '';
    document.getElementById('apiUrl').value = c.API_URL || '';
    document.getElementById('wsUrl').value = c.WS_URL || '';
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
    TREMOL_IN_DIR: document.getElementById('tremolIn').value.trim(),
    TREMOL_OUT_DIR: document.getElementById('tremolOut').value.trim(),
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

    def _send_json(self, code: int, data: dict):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
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

        if path == "/service/config":
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

        elif path == "/service/clear-errors":
            clear_errors()
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "Endpoint inexistent"})

def start_admin_server(port: int = 8002):
    try:
        server = http.server.ThreadingHTTPServer(("0.0.0.0", port), AdminHTTPRequestHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True, name="AdminWeb8002")
        t.start()
        logger.info(f"[Admin] Panou web pornit pe http://127.0.0.1:{port}/")
        return server
    except Exception as e:
        logger.error(f"[Admin] Nu s-a putut deschide portul {port}: {e}")
        return None


# ==================== MAIN ENTRY POINT ====================

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

    # Buclă de supraveghere principală
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("[Main] Oprire solicitată. Ieșire.")
        sys.exit(0)

if __name__ == "__main__":
    main()
