#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simulator fiscal ErpNet.FP — înlocuiește ErpNet.FP + casa de marcat reală.

Toată configurația operațională vine din config.json (secțiunea `simulator`).
Persistență:
  - simulator_state.json    contor fiscal + idempotență (supraviețuiește restartului)
  - simulator_ledger.jsonl  jurnal imutabil append-only (echivalentul memoriei fiscale)
  - bon_<timestamp>.json    copie per bon (inspectare ușoară)
  - simulator_errors.jsonl  coduri de eroare
"""

import http.server
import json
import os
import re
import signal
import socket
import sys
import threading
import time
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlparse, parse_qs

# ================================================================
# BASE DIR + mod .exe (windowed → fără consolă)
# ================================================================
def _base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = _base_dir()

# Când rulează ca .exe `--windowed`, sys.stdout e None.
# Redirecționăm către fișier ca `print()` să nu crape.
if getattr(sys, "frozen", False):
    _con = open(os.path.join(BASE_DIR, "simulator_console.log"), "a",
                encoding="utf-8", buffering=1)
    sys.stdout = _con
    sys.stderr = _con

def _safe_print(*args):
    try:
        print(*args, flush=True)
    except UnicodeEncodeError:
        try:
            safe_args = [str(a).encode("ascii", "replace").decode("ascii") for a in args]
            print(*safe_args, flush=True)
        except Exception:
            pass
    except Exception:
        pass

# ================================================================
# CONFIG — totul vine din config.json, cu fallback minim
# ================================================================
def _config_path():
    return os.path.join(BASE_DIR, "config.json")

def _load_raw_config():
    p = _config_path()
    if not os.path.exists(p):
        _safe_print(f"[Simulator] {p} lipsește — folosesc valorile de rezervă.")
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        _safe_print(f"[Simulator] Config încărcat din {p}")
        return data if isinstance(data, dict) else {}
    except Exception as e:
        _safe_print(f"[Simulator] Eroare la citirea {p}: {e}. Folosesc valorile de rezervă.")
        return {}

def _pick(section, root, key, default):
    if isinstance(section, dict) and key in section:
        return section[key]
    if isinstance(root, dict) and key in root:
        return root[key]
    return default

def _as_int(v, default):
    try: return int(v)
    except (TypeError, ValueError): return default

def _as_float(v, default):
    try: return float(v)
    except (TypeError, ValueError): return default

def _as_bool(v, default=False):
    if isinstance(v, bool): return v
    if isinstance(v, str): return v.strip().lower() in ("1","true","yes","on","da")
    if isinstance(v, (int, float)): return bool(v)
    return default

def _as_str(v, default):
    return str(v) if isinstance(v, (str, int, float)) else default

_RAW = _load_raw_config()
_SIM = _RAW.get("simulator") if isinstance(_RAW.get("simulator"), dict) else {}

HOST          = _as_str(_pick(_SIM, _RAW, "host", "127.0.0.1"), "127.0.0.1")
PORT          = _as_int(_pick(_SIM, _RAW, "port", 8001), 8001)
PRINTER_ID    = _as_str(_pick(_SIM, _RAW, "printer_id", "TremolS25_Simulated"), "TremolS25_Simulated")
PRINTER_NAME  = _as_str(_pick(_SIM, _RAW, "printer_name", "Tremol S25 Simulator"), "Tremol S25 Simulator")
PRINTER_MODEL = _as_str(_pick(_SIM, _RAW, "printer_model", "Tremol S25"), "Tremol S25")
FIRMWARE      = _as_str(_pick(_SIM, _RAW, "firmware", "1.0.0-sim"), "1.0.0-sim")
FM_SERIAL     = _as_str(_pick(_SIM, _RAW, "fiscal_memory_serial", "SIMULATOR99"), "SIMULATOR99")
RECEIPT_START = _as_int(_pick(_SIM, _RAW, "receipt_counter_start", 1000), 1000)
# Bon cu CUI cumpărător (POST /printers/{id}/invoice, câmpul „recipient” din ErpNet.FP) — tipărește CF^<CUI>.
SUPPORTS_INVOICE = _as_bool(_pick(_SIM, _RAW, "supports_invoice", True), True)
# Fișierul XML ANAF de test generat la fiecare raport Z (imită exportul ANAF din DUDE / Tremol).
# Puntea îl preia dacă folderul e trecut la „Foldere urmărite” (ANAF_XML_DIRS). Gol = dezactivat.
ANAF_XML_DIR = _as_str(_pick(_SIM, _RAW, "anaf_xml_dir", os.path.join(BASE_DIR, "anaf_xml")), "")
SIM_NUI      = _as_str(_pick(_SIM, _RAW, "nui", "4000000099"), "4000000099")
SIM_CIF      = _as_str(_pick(_SIM, _RAW, "cif", ""), "")

PHASE_IN_PROGRESS = _as_float(_pick(_SIM, _RAW, "phase_in_progress_seconds", 0.3), 0.3)
PHASE_FINISHED    = _as_float(_pick(_SIM, _RAW, "phase_finished_seconds", 1.5), 1.5)
if PHASE_FINISHED <= PHASE_IN_PROGRESS:
    _safe_print(f"[Simulator] Ajustez phase_finished la {PHASE_IN_PROGRESS + 0.5}s.")
    PHASE_FINISHED = PHASE_IN_PROGRESS + 0.5

_INIT_STATE = _pick(_SIM, _RAW, "initial_state", {})
if not isinstance(_INIT_STATE, dict): _INIT_STATE = {}

INITIAL_STATE = {
    "paperOut":  _as_bool(_INIT_STATE.get("paperOut",  False)),
    "coverOpen": _as_bool(_INIT_STATE.get("coverOpen", False)),
    "offline":   _as_bool(_INIT_STATE.get("offline",   False)),
}

if not (0 < PORT <= 65535):
    _safe_print(f"[Simulator] Port invalid {PORT}, revin la 8001.")
    PORT = 8001

# ================================================================
# FIȘIERE
# ================================================================
STATE_FILE  = os.path.join(BASE_DIR, "simulator_state.json")
LEDGER_FILE = os.path.join(BASE_DIR, "simulator_ledger.jsonl")
ERRORS_FILE = os.path.join(BASE_DIR, "simulator_errors.jsonl")

# ================================================================
# ERROR LOG
# ================================================================
def log_error(code, message, extra=None):
    entry = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "source": "simulator",
        "code": code,
        "message": message,
    }
    if extra:
        entry["extra"] = extra
    try:
        with open(ERRORS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass

def read_errors(limit=200):
    out = []
    try:
        with open(ERRORS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try: out.append(json.loads(line))
                except json.JSONDecodeError: pass
    except FileNotFoundError:
        pass
    out.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
    return out[:limit]

# ================================================================
# STARE runtime + persistență
# ================================================================
state_lock = threading.RLock()
tasks_lock = threading.RLock()

receipt_counter = RECEIPT_START
tasks           = {}
receipts_seen   = {}    # USN -> result (idempotență, persistat)
recent_receipts = []    # UI
drawer_open     = False
last_task_id    = None

printer_state = dict(INITIAL_STATE)

MAX_RECEIPTS_SEEN = 10000   # limită cache idempotență
MAX_RECENT        = 100     # limită UI

def _recover_counter_from_ledger():
    """Ultimul receiptNumber din ledger (pentru cazul în care state.json s-a pierdut)."""
    if not os.path.exists(LEDGER_FILE):
        return None
    try:
        with open(LEDGER_FILE, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0: return None
            f.seek(max(0, size - 8192))
            data = f.read().decode("utf-8", errors="ignore")
            for line in reversed(data.splitlines()):
                line = line.strip()
                if not line: continue
                try:
                    obj = json.loads(line)
                    n = obj.get("receiptNumber")
                    if n is not None:
                        return int(n)
                except (json.JSONDecodeError, ValueError):
                    continue
    except OSError:
        pass
    return None

def load_state():
    global receipt_counter, receipts_seen, recent_receipts
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            saved_counter = _as_int(data.get("receipt_counter"), 0)
            saved_seen = data.get("receipts_seen") or {}
            saved_recent = data.get("recent_receipts") or []
            if isinstance(saved_seen, dict):
                receipts_seen = saved_seen
            if isinstance(saved_recent, list):
                recent_receipts = saved_recent[:MAX_RECENT]
            ledger_counter = _recover_counter_from_ledger() or 0
            receipt_counter = max(RECEIPT_START, saved_counter, ledger_counter)
            _safe_print(f"[Simulator] State încărcat: counter={receipt_counter:07d}, "
                        f"idempotency={len(receipts_seen)} chei")
            return
        except Exception as e:
            _safe_print(f"[Simulator] State corupt ({e}), recuperez din ledger...")

    ledger_counter = _recover_counter_from_ledger() or 0
    receipt_counter = max(RECEIPT_START, ledger_counter)
    _safe_print(f"[Simulator] State nou: counter={receipt_counter:07d}")

def save_state():
    """Scriere atomică (tmp + rename) ca să nu corupem la crash."""
    try:
        with state_lock:
            data = {
                "receipt_counter": receipt_counter,
                "receipts_seen": receipts_seen,
                "recent_receipts": recent_receipts[:MAX_RECENT],
                "saved_at": datetime.now().isoformat(timespec="seconds"),
            }
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except OSError as e:
        log_error("STATE_WRITE_FAILED", str(e))

def ledger_append(entry):
    """Append-only + fsync — echivalentul scrierii în memoria fiscală."""
    try:
        with open(LEDGER_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()
            try: os.fsync(f.fileno())
            except OSError: pass
    except OSError as e:
        log_error("LEDGER_WRITE_FAILED", str(e))

# ================================================================
# Bon fiscal – căutare în ledger + generare PDF
# ================================================================
def _get_receipt_from_ledger(receipt_number):
    """Caută un bon sau raport după identificator în ledger (append-only)."""
    if not os.path.exists(LEDGER_FILE):
        return None
    found = None
    r_target = str(receipt_number or "").strip()
    try:
        with open(LEDGER_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                t = e.get("type")
                if t == "receipt" and e.get("receiptNumber") == r_target:
                    found = e
                elif t == "storno" and (e.get("stornoNumber") == r_target or e.get("receiptNumber") == r_target):
                    found = e
                elif t == "zreport" and (f"Z #{e.get('zNumber')}" == r_target or str(e.get("zNumber")) == r_target or r_target.upper() in ("Z", "RAPORT Z")):
                    found = e
                elif t == "xreport" and r_target.upper() in ("X", "RAPORT X"):
                    found = e
                elif t == "periodreport" and (r_target.upper() in ("MF", "RAPORT MF") or r_target == e.get("fromDate")):
                    found = e
    except OSError:
        pass
    return found


def _generate_receipt_pdf(receipt):
    """
    Generează un PDF minimal (80mm thermal) fără dependințe externe.
    Font: Courier (built-in PDF), text ASCII (diacriticele RO sunt transliterate).
    Suportă bonuri fiscale, storno, Raport X, Raport Z și Raport Memorie Fiscală.
    """
    WIDTH = 36
    r_type = receipt.get("type", "receipt")
    ts    = str(receipt.get("timestamp", ""))
    fm    = str(receipt.get("fiscalMemorySerialNumber") or FM_SERIAL)

    def center(t): return t.center(WIDTH)
    def lr(left, right):
        sp = WIDTH - len(left) - len(right)
        return left + (" " * max(0, sp)) + right

    if r_type == "zreport":
        z_no = receipt.get("zNumber", 1)
        cnt  = receipt.get("count", 0)
        tot  = float(receipt.get("total", 0) or 0)
        lines = [
            "=" * WIDTH,
            center("SIMULATOR FISCAL S.R.L."),
            center("CUI: RO12345678"),
            "=" * WIDTH,
            center("RAPORT Z - INCHIDERE ZILNICA"),
            center("MEMORIE FISCALA"),
            "-" * WIDTH,
            f"Raport Z nr: {z_no}",
            f"Data/Ora:    {ts}",
            f"Documente:   {cnt}",
            "-" * WIDTH,
        ]
        by_pay = receipt.get("byPayment") or {}
        for ptype, pamt in by_pay.items():
            lines.append(lr(str(ptype).upper(), f"{float(pamt):.2f} RON"))
        lines += [
            "-" * WIDTH,
            lr("TOTAL ZI:", f"{tot:.2f} RON"),
            "=" * WIDTH,
            f"Seria FM: {fm}",
            center("INREGISTRAT IN MEMORIA FISCALA"),
            "=" * WIDTH,
        ]
    elif r_type == "xreport":
        cnt = receipt.get("count", 0)
        tot = float(receipt.get("total", 0) or 0)
        lines = [
            "=" * WIDTH,
            center("SIMULATOR FISCAL S.R.L."),
            center("CUI: RO12345678"),
            "=" * WIDTH,
            center("RAPORT X - INFORMATIV ZI"),
            center("(NU INCHIDE ZIUA FISCALA)"),
            "-" * WIDTH,
            f"Data/Ora:    {ts}",
            f"Documente:   {cnt}",
            "-" * WIDTH,
        ]
        by_pay = receipt.get("byPayment") or {}
        for ptype, pamt in by_pay.items():
            lines.append(lr(str(ptype).upper(), f"{float(pamt):.2f} RON"))
        lines += [
            "-" * WIDTH,
            lr("TOTAL VANZARI:", f"{tot:.2f} RON"),
            "=" * WIDTH,
            f"Seria FM: {fm}",
            "=" * WIDTH,
        ]
    elif r_type == "periodreport":
        d_from = str(receipt.get("fromDate", ""))[:10]
        d_to   = str(receipt.get("toDate", ""))[:10]
        z_cnt  = receipt.get("zCount", 0)
        tot    = float(receipt.get("total", 0) or 0)
        lines = [
            "=" * WIDTH,
            center("SIMULATOR FISCAL S.R.L."),
            center("CUI: RO12345678"),
            "=" * WIDTH,
            center("RAPORT MEMORIE FISCALA"),
            center("PERIODIC / LUNAR"),
            "-" * WIDTH,
            f"De la data:  {d_from}",
            f"Pana la data:{d_to}",
            f"Rapoarte Z:  {z_cnt}",
            f"Data emiterii:{ts}",
            "-" * WIDTH,
            lr("TOTAL PERIOADA:", f"{tot:.2f} RON"),
            "=" * WIDTH,
            f"Seria FM: {fm}",
            "=" * WIDTH,
        ]
    else:
        rno   = str(receipt.get("receiptNumber") or receipt.get("stornoNumber") or "N/A")
        usn   = receipt.get("uniqueSaleNumber") or ""
        total = float(receipt.get("receiptAmount", 0) or receipt.get("stornoAmount", 0) or 0)
        is_storno = (r_type == "storno")

        lines = [
            "=" * WIDTH,
            center("SIMULATOR FISCAL S.R.L."),
            center("CUI: RO12345678"),
            center("Str. Exemplu nr. 1"),
            center("Bucuresti, Sector 1"),
            center("Tel: 021.000.0000"),
            "=" * WIDTH,
            center("BON FISCAL - STORNO" if is_storno else "BON FISCAL"),
            "-" * WIDTH,
            f"Nr. {'storno' if is_storno else 'bon'}: {rno}",
            f"Data:    {ts}",
        ]
        if usn:
            lines.append(f"USN:     {usn[:WIDTH - 9]}")
        rcp = receipt.get("recipient") or {}
        if rcp:
            # CF^: CUI-ul cumpărătorului tipărit de casă (bon fiscal cu valoare de factură simplificată)
            lines.append(f"CF^{rcp.get('vatNumber') or rcp.get('identifier') or ''}"[:WIDTH])
            lines.append(f"Client: {rcp.get('name') or ''}"[:WIDTH])
        lines.append("-" * WIDTH)

        for it in receipt.get("items", []) or []:
            if not isinstance(it, dict):
                continue
            if it.get("type") in ("comment", "footer-comment", "separator"):
                lines.append(str(it.get("text", ""))[:WIDTH])
                continue
            name = it.get("text") or it.get("name") or ""
            lines.append(str(name)[:WIDTH])
            qty = float(it.get("quantity", 1) or 1)
            price = float(it.get("unitPrice", 0) or 0)
            amt_raw = it.get("totalAmount")
            amt = float(amt_raw) if amt_raw is not None else qty * price
            lines.append(lr(f"  {qty:g} x {price:.2f}", f"{amt:.2f}"))

        lines.append("-" * WIDTH)
        lines.append(lr("TOTAL:", f"{total:.2f} RON"))
        lines.append("-" * WIDTH)

        for p in receipt.get("payments", []) or []:
            if not isinstance(p, dict):
                continue
            ptype = str(p.get("paymentType") or "cash").upper()
            pamt  = float(p.get("amount", 0) or 0)
            lines.append(lr(ptype, f"{pamt:.2f}"))

        lines += [
            "-" * WIDTH,
            f"Seria FM: {fm}",
            "",
            center("MULTUMIM! VA MAI ASTEPTAM!"),
            "",
            center(f"*{rno}*"),
            "=" * WIDTH,
        ]

    # ---------- construim PDF-ul ----------
    PAGE_W, LINE_H = 226, 10
    PAGE_H = max(400, len(lines) * LINE_H + 40)

    parts = ["BT", "/F1 7 Tf"]
    y = PAGE_H - 15
    import unicodedata
    special = {"\u2014": "-", "\u2013": "-", "\u20ac": "EUR", "\u201e": '"', "\u201d": '"', "\u201c": '"'}
    for line in lines:
        for ch, asc in special.items():
            line = line.replace(ch, asc)
        # ă â î ș ț (virgulă sau sedilă) -> a a i s t; restul caracterelor non-ASCII sunt eliminate
        line = unicodedata.normalize("NFKD", line).encode("ascii", "ignore").decode("ascii")
        esc = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        parts.append(f"1 0 0 1 10 {y} Tm")
        parts.append(f"({esc}) Tj")
        y -= LINE_H
    parts.append("ET")
    stream = "\n".join(parts)
    stream_bytes = stream.encode("ascii")

    objs = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_W} {PAGE_H}] "
        f"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        f"<< /Length {len(stream_bytes)} >>\nstream\n{stream}\nendstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>",
    ]

    pdf = b"%PDF-1.4\n"
    offsets = []
    for i, obj in enumerate(objs, 1):
        offsets.append(len(pdf))
        pdf += f"{i} 0 obj\n{obj}\nendobj\n".encode("latin-1")

    xref = len(pdf)
    pdf += f"xref\n0 {len(objs) + 1}\n".encode("latin-1")
    pdf += b"0000000000 65535 f \n"
    for off in offsets:
        pdf += f"{off:010d} 00000 n \n".encode("latin-1")
    pdf += (f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref}\n%%EOF").encode("latin-1")

    return pdf

# ================================================================
# Utilitare
# ================================================================
def write_test_anaf_xml(z_no, ts, total, by_pay, count):
    """XML de TEST pentru raportul Z (structură simplificată, NESEMNAT): idM = NUI + data/ora, nrRapI/nrRapF.
    Fișierele reale vin semnate din casa de marcat (OPANAF 146/2018) prin DUDE / utilitarul Tremol."""
    if not ANAF_XML_DIR:
        return ""
    try:
        os.makedirs(ANAF_XML_DIR, exist_ok=True)
        stamp = re.sub(r"\D", "", str(ts))[:14].ljust(14, "0")
        pays = "".join(f"<plata tip=\"{k}\" suma=\"{float(v):.2f}\"/>" for k, v in (by_pay or {}).items())
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f"<mReg><idM>{SIM_NUI[:10].ljust(10, '0')}{stamp}</idM><cif>{SIM_CIF}</cif>"
            f"<nrRapI>{z_no}</nrRapI><nrRapF>{z_no}</nrRapF><tip_amef>U</tip_amef>"
            f"<raportZ nrZ=\"{z_no}\" serie=\"{FM_SERIAL}\" data=\"{str(ts)[:10]}\" nrBonuri=\"{count}\" "
            f"total=\"{float(total):.2f}\">{pays}</raportZ><test>SIMULATOR - fisier nesemnat</test></mReg>\n"
        )
        path = os.path.join(ANAF_XML_DIR, f"Z_{int(z_no):04d}_{stamp[:8]}_{FM_SERIAL}.xml")
        with open(path, "w", encoding="utf-8") as f:
            f.write(xml)
        return path
    except Exception as e:
        log_error("ANAF_XML_WRITE", f"Nu pot scrie fișierul XML de test: {e}")
        return ""


def now_iso():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def money(v):
    return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def total_of(items):
    total = Decimal("0")
    for it in items:
        if not isinstance(it, dict): continue
        if it.get("type") in ("comment", "footer-comment", "separator"): continue
        qty = Decimal(str(it.get("quantity", 1)))
        if "totalAmount" in it:
            total += money(it["totalAmount"])
        else:
            total += money(Decimal(str(it.get("unitPrice", 0))) * qty)
    return total

def save_bon_file(data):
    try:
        with open(os.path.join(BASE_DIR, f"bon_{int(time.time()*1000)}.json"),
                  "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        pass

def printer_busy():
    with tasks_lock:
        for t in tasks.values():
            if t.get("taskStatus") in ("pending", "in-progress"):
                return True
    return False

def current_task():
    with tasks_lock:
        if not last_task_id: return None
        return tasks.get(last_task_id)

# ================================================================
# Rutare
# ================================================================
ROUTES = {
    "receipt":      re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?receipt/?$"),
    "invoice":      re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?invoice/?$"),
    "storno":       re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?storno/?$"),
    "xreport":      re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?xreport/?$"),
    "zreport":      re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?zreport/?$"),
    "periodreport": re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?periodreport/?$"),
    "report":       re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?report/?$"),
    "open-drawer":  re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?open-drawer/?$"),
    "raw":          re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?raw/?$"),
    "datetime":     re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?datetime/?$"),
    "status":       re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?status/?$"),
    "taskinfo":     re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?taskinfo/?$"),
    "taskinfo_id":  re.compile(r"^/printers/(?:(?P<id>[^/]+)/)?taskinfo/?$"),
    "printer":      re.compile(r"^/printers/(?P<id>[^/]+)/?$"),
    "printers":     re.compile(r"^/printers/?$"),
    "service":      re.compile(r"^/service(?:/.*)?$"),
    "admin":        re.compile(r"^/admin/?$"),
    "index":        re.compile(r"^/$"),
}

def match_route(path):
    for name, rx in ROUTES.items():
        m = rx.match(path)
        if m: return name, m.groupdict()
    return None, {}

# ================================================================
# HTML Admin
# ================================================================
ADMIN_HTML = r"""<!DOCTYPE html>
<html lang="ro"><head><meta charset="utf-8">
<title>ErpNet.FP Simulator – Admin</title>
<style>
  :root{--bg:#0f172a;--card:#1e293b;--fg:#e2e8f0;--accent:#38bdf8;--ok:#22c55e;--err:#ef4444;--warn:#f59e0b}
  *{box-sizing:border-box}
  body{margin:0;font-family:system-ui,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--fg)}
  header{padding:20px 28px;border-bottom:1px solid #334155;display:flex;justify-content:space-between;align-items:center}
  header h1{margin:0;font-size:1.3rem}
  .badge{padding:4px 10px;border-radius:999px;font-size:.8rem;font-weight:600}
  .badge.ok{background:#14532d;color:#86efac}
  .badge.err{background:#7f1d1d;color:#fca5a5}
  .badge.warn{background:#78350f;color:#fde68a}
  main{max-width:1200px;margin:24px auto;padding:0 20px;display:grid;grid-template-columns:1fr 1fr;gap:20px}
  @media(max-width:900px){main{grid-template-columns:1fr}}
  .card{background:var(--card);border:1px solid #334155;border-radius:14px;padding:20px}
  .card h2{margin:0 0 14px;font-size:1rem;color:var(--accent)}
  .kv{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px dashed #334155;font-size:.9rem;gap:12px}
  .kv:last-child{border-bottom:none}
  .kv span{color:#94a3b8}
  .kv b{word-break:break-all;text-align:right}
  .ok{color:var(--ok)} .err{color:var(--err)} .warn{color:var(--warn)}
  table{width:100%;border-collapse:collapse;font-size:.82rem}
  th,td{text-align:left;padding:8px 6px;border-bottom:1px solid #334155;vertical-align:top}
  th{color:#94a3b8;font-weight:600}
  code{background:#0b1220;padding:2px 6px;border-radius:4px;font-size:.8rem}
  input,button,select,label{font:inherit}
  input,select{width:100%;padding:9px 12px;border-radius:8px;border:1px solid #334155;
               background:#0b1220;color:var(--fg);margin-bottom:10px}
  label{display:block;font-size:.8rem;color:#94a3b8;margin-bottom:4px}
  button{background:var(--accent);color:#042f4a;border:none;padding:8px 14px;border-radius:8px;
         font-weight:700;cursor:pointer}
  button:hover{filter:brightness(1.1)}
  button.secondary{background:#475569;color:#e2e8f0}
  pre{background:#0b1220;border:1px solid #334155;border-radius:8px;padding:12px;
      font-size:.75rem;overflow:auto;max-height:260px;white-space:pre-wrap}
  .muted{color:#94a3b8;font-size:.8rem}
  .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
  .print-btn{
    background:#0284c7;color:#fff;border:none;
    padding:4px 10px;border-radius:6px;
    font-size:.75rem;font-weight:600;cursor:pointer;white-space:nowrap;
  }
  .print-btn:hover{filter:brightness(1.15)}
</style></head><body>
<header>
  <h1>🧾 ErpNet.FP Simulator</h1>
  <span class="badge" id="conn">se conectează…</span>
</header>
<main>
  <section class="card"><h2>Imprimantă</h2>
    <div id="printerInfo" class="muted">…</div>
    <div id="printerStatus" style="margin-top:12px"></div>
  </section>

  <section class="card"><h2>Task curent</h2>
    <div id="currentTask" class="muted">niciun task</div>
  </section>

  <section class="card"><h2>Stare simulată</h2>
    <select id="errMode">
      <option value="normal">🟢 Normal</option>
      <option value="paperOut">🟡 Paper out</option>
      <option value="coverOpen">🟡 Cover open</option>
      <option value="offline">🔴 Offline</option>
    </select>
    <button onclick="setErrMode()">Aplică</button>
    <div id="errModeMsg" class="muted" style="margin-top:8px"></div>
  </section>

  <section class="card"><h2>Tipărește bon (flux async ca bridge-ul)</h2>
    <label>uniqueSaleNumber (opțional)</label>
    <input id="usn" placeholder="ex: USN-2026-0001">
    <label>Nume produs</label>
    <input id="itemName" value="Produs test">
    <label>Cantitate</label>
    <input id="itemQty" type="number" value="1" min="1">
    <label>Preț unitar</label>
    <input id="itemPrice" type="number" value="12.50" step="0.01">
    <button onclick="printTest()">Trimite bon</button>
    <pre id="lastResp">—</pre>
  </section>

  <section class="card"><h2>Rapoarte fiscale (X / Z / Memorie fiscală)</h2>
    <div class="row" style="margin-bottom:10px">
      <button onclick="runReport('xreport')" style="background:#0284c7;color:#fff">📊 Raport X (Informativ)</button>
      <button onclick="runReport('zreport')" style="background:#ea580c;color:#fff">🔒 Raport Z (Închidere zi)</button>
    </div>
    <div style="border-top:1px dashed #334155;padding-top:10px;margin-top:10px">
      <label>Raport memorie fiscală (Z lunar / perioadă)</label>
      <div class="row">
        <input id="repFrom" type="date" style="max-width:140px;margin-bottom:0">
        <span style="color:#94a3b8">→</span>
        <input id="repTo" type="date" style="max-width:140px;margin-bottom:0">
        <button onclick="runPeriodReport()" class="secondary">🖨️ Raport MF</button>
      </div>
    </div>
    <pre id="reportResp" style="margin-top:12px">—</pre>
  </section>

  <section class="card" style="grid-column:1/-1"><h2>Ultimele bonuri și rapoarte</h2>
    <table>
      <thead><tr>
        <th>#</th><th>Dată</th><th>USN / Tip</th><th>Sumă</th><th>Status</th><th>Acțiuni</th>
      </tr></thead>
      <tbody id="receiptsBody"><tr><td colspan="6" class="muted">—</td></tr></tbody>
    </table>
  </section>

  <section class="card" style="grid-column:1/-1"><h2>Coduri de eroare</h2>
    <div class="row" style="margin-bottom:12px">
      <button class="secondary" onclick="loadErrors()">Reîncarcă</button>
      <button class="secondary" onclick="clearErrors()">Șterge log erori</button>
    </div>
    <table>
      <thead><tr><th>Timestamp</th><th>Cod</th><th>Mesaj</th></tr></thead>
      <tbody id="errBody"><tr><td colspan="3" class="muted">—</td></tr></tbody>
    </table>
  </section>

  <section class="card" style="grid-column:1/-1"><h2>Endpoint-uri (compatibil ErpNet.FP)</h2>
    <pre>POST /printers/{id}/receipt?asyncTimeout=0&taskId=X   → {"taskId":"X"}
POST /printers/{id}/xreport                             → raport X (informativ zi)
POST /printers/{id}/zreport                             → raport Z (închidere zilnică)
POST /printers/{id}/periodreport                        → raport memorie fiscală (Z lunar / perioadă)
POST /printers/{id}/report                              → raport generic ({"type":"x|z|period"})
GET  /printers/taskinfo?id=X                            → {"taskStatus":"pending|in-progress|finished", "result":{...}}
GET  /printers/{id}/taskinfo                            → idem
GET  /printers                                          → listă
GET  /printers/{id}/status                              → stare live
POST /printers/{id}/storno  ·  /open-drawer  ·  /raw
GET/POST /printers/{id}/datetime
GET  /service  ·  /service/recent-receipts  ·  /service/current-task
GET  /service/errors  ·  /service/config  ·  /admin
GET  /service/receipt-pdf?receiptNumber=0001022        → bon fiscal / raport PDF (80mm)</pre>
  </section>
</main>
<script>
const PRINTER_ID = "__PRINTER_ID__";
async function j(u,o){const r=await fetch(u,o);if(!r.ok)throw new Error('HTTP '+r.status);return r.json()}

async function refresh(){
  try{
    const p=await j('/printers');
    const list=Array.isArray(p)?p:(p.printers||[]);
    const pr=list[0]; if(!pr)return;
    document.getElementById('printerInfo').innerHTML=`
      <div class="kv"><span>ID</span><b>${pr.id}</b></div>
      <div class="kv"><span>Model</span><b>${pr.model||'-'}</b></div>
      <div class="kv"><span>Serial FM</span><b>${pr.fiscalMemorySerialNumber||'-'}</b></div>
      <div class="kv"><span>Firmware</span><b>${pr.firmwareVersion||'-'}</b></div>
      <div class="kv"><span>Ultimul bon</span><b>${pr.lastReceiptNumber||'-'}</b></div>`;
    const st=await j(`/printers/${pr.id}/status`);
    const s=st.status||{};
    const flag=(v,l)=>{
      const cls = v ? 'err' : 'ok';
      return `<div class="kv"><span>${l}</span><b class="${cls}">${v?'DA':'NU'}</b></div>`;
    };
    document.getElementById('printerStatus').innerHTML=
      `<div class="kv"><span>Busy</span><b class="${s.busy?'warn':'ok'}">${s.busy?'DA':'NU'}</b></div>`+
      `<div class="kv"><span>Ready</span><b class="${s.ready?'ok':'err'}">${s.ready?'DA':'NU'}</b></div>`+
      flag(s.paperOut,'Paper out')+
      flag(s.coverOpen,'Cover open')+
      flag(s.offline,'Offline')+
      flag(s.drawerOpen,'Drawer open');

    const ct=await fetch('/service/current-task');
    if(ct.ok){
      const tj=await ct.json();
      const el=document.getElementById('currentTask');
      if(tj.task){
        const badge = tj.task.taskStatus==='finished' ? 'ok' : 'warn';
        el.innerHTML=`<div class="kv"><span>Task ID</span><b>${tj.task_id||'-'}</b></div>
          <div class="kv"><span>Status</span><b class="${badge}">${tj.task.taskStatus}</b></div>`;
      } else { el.textContent='niciun task'; }
    }

    const b=document.getElementById('conn');
    b.textContent='● Conectat'; b.className='badge ok';
  }catch(e){
    const b=document.getElementById('conn');
    b.textContent='● Deconectat'; b.className='badge err';
  }
}

async function setErrMode(){
  const mode=document.getElementById('errMode').value;
  const r=await fetch('/admin/error-mode',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({mode})});
  const jr=await r.json();
  document.getElementById('errModeMsg').textContent='Setat: '+JSON.stringify(jr.printer_state);
  refresh();
}

async function printTest(){
  const usn=document.getElementById('usn').value||undefined;
  const qty=Number(document.getElementById('itemQty').value)||1;
  const price=Number(document.getElementById('itemPrice').value)||12.5;
  const body={uniqueSaleNumber:usn,
    items:[{text:document.getElementById('itemName').value||'Produs test',
      quantity:qty, unitPrice:price, taxGroup:2}],
    payments:[{amount:qty*price, paymentType:'cash'}]};
  const taskId='admin-'+Date.now();
  const out=document.getElementById('lastResp');
  out.textContent='POST receipt…';
  try{
    const r=await j(`/printers/${PRINTER_ID}/receipt?asyncTimeout=0&taskId=${encodeURIComponent(taskId)}`,{
      method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    out.textContent='Răspuns POST: '+JSON.stringify(r)+'\nPolling taskinfo…';
    if(r.taskId){
      let done=null;
      for(let i=0;i<20;i++){
        await new Promise(res=>setTimeout(res,250));
        const t=await j(`/printers/taskinfo?id=${encodeURIComponent(r.taskId)}`);
        out.textContent='Polling '+i+': '+JSON.stringify(t);
        if(t.taskStatus==='finished'){ done=t.result; break; }
      }
      out.textContent = done ? 'REZULTAT FINAL:\n'+JSON.stringify(done,null,2) : out.textContent+'\n(TIMEOUT)';
    } else {
      out.textContent='Răspuns final:\n'+JSON.stringify(r,null,2);
    }
    refresh(); loadRecent();
  }catch(e){ out.textContent='Eroare: '+e.message; }
}

async function runReport(kind){
  const out=document.getElementById('reportResp');
  out.textContent='POST '+kind+'…';
  const taskId='admin-'+kind+'-'+Date.now();
  try{
    const r=await j(`/printers/${PRINTER_ID}/${kind}?asyncTimeout=0&taskId=${encodeURIComponent(taskId)}`,{
      method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
    out.textContent='Răspuns POST: '+JSON.stringify(r)+'\nPolling taskinfo…';
    if(r.taskId){
      let done=null;
      for(let i=0;i<20;i++){
        await new Promise(res=>setTimeout(res,250));
        const t=await j(`/printers/taskinfo?id=${encodeURIComponent(r.taskId)}`);
        out.textContent='Polling '+i+': '+JSON.stringify(t);
        if(t.taskStatus==='finished'){ done=t.result; break; }
      }
      out.textContent = done ? 'REZULTAT RAPORT:\n'+JSON.stringify(done,null,2) : out.textContent+'\n(TIMEOUT)';
    } else {
      out.textContent='Răspuns final:\n'+JSON.stringify(r,null,2);
    }
    refresh(); loadRecent();
  }catch(e){ out.textContent='Eroare: '+e.message; }
}

async function runPeriodReport(){
  const out=document.getElementById('reportResp');
  const f=document.getElementById('repFrom').value;
  const t=document.getElementById('repTo').value;
  out.textContent='POST periodreport…';
  const taskId='admin-mf-'+Date.now();
  const body={fromDate:f?(f+'T00:00:00'):undefined, toDate:t?(t+'T23:59:59'):undefined, type:'short'};
  try{
    const r=await j(`/printers/${PRINTER_ID}/periodreport?asyncTimeout=0&taskId=${encodeURIComponent(taskId)}`,{
      method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    out.textContent='Răspuns POST: '+JSON.stringify(r)+'\nPolling taskinfo…';
    if(r.taskId){
      let done=null;
      for(let i=0;i<20;i++){
        await new Promise(res=>setTimeout(res,250));
        const t=await j(`/printers/taskinfo?id=${encodeURIComponent(r.taskId)}`);
        out.textContent='Polling '+i+': '+JSON.stringify(t);
        if(t.taskStatus==='finished'){ done=t.result; break; }
      }
      out.textContent = done ? 'REZULTAT RAPORT MEMORIE FISCALĂ:\n'+JSON.stringify(done,null,2) : out.textContent+'\n(TIMEOUT)';
    } else {
      out.textContent='Răspuns final:\n'+JSON.stringify(r,null,2);
    }
    refresh(); loadRecent();
  }catch(e){ out.textContent='Eroare: '+e.message; }
}

async function loadRecent(){
  try{
    const r=await j('/service/recent-receipts');
    const rows=r.receipts||[];
    const tb=document.getElementById('receiptsBody');
    tb.innerHTML=rows.length
      ? rows.map(x=>`<tr>
          <td>${x.receiptNumber}</td>
          <td>${x.receiptDateTime || x.timestamp || '-'}</td>
          <td>${x.uniqueSaleNumber||'-'}</td>
          <td>${x.receiptAmount || x.amount || '-'}</td>
          <td class="${(x.ok || x.status==='OK')?'ok':'err'}">${(x.ok || x.status==='OK')?'OK':'EROARE'}</td>
          <td>
            <button class="print-btn"
                    onclick="openReceipt('${x.receiptNumber}')"
                    title="Deschide bonul sau raportul ca PDF">🖨️ PDF</button>
          </td>
        </tr>`).join('')
      : '<tr><td colspan="6" class="muted">—</td></tr>';
  }catch(e){}
}

function openReceipt(rno){
  // deschide PDF-ul într-un tab nou (inline → preview în browser)
  window.open(
    '/service/receipt-pdf?receiptNumber=' + encodeURIComponent(rno),
    '_blank'
  );
}

async function loadErrors(){
  try{
    const r=await j('/service/errors');
    const rows=r.errors||[];
    const tb=document.getElementById('errBody');
    tb.innerHTML=rows.length
      ? rows.map(x=>`<tr><td>${x.timestamp||'-'}</td>
          <td><code>${x.code||'-'}</code></td><td>${x.message||''}</td></tr>`).join('')
      : '<tr><td colspan="3" class="muted">Nicio eroare.</td></tr>';
  }catch(e){}
}

async function clearErrors(){
  if(!confirm('Ștergi log-ul de erori al simulatorului?'))return;
  await fetch('/service/clear-errors',{method:'POST'});
  loadErrors();
}

refresh(); loadRecent(); loadErrors();
setInterval(refresh, 2000);
setInterval(loadRecent, 4000);
</script></body></html>
"""

def render_admin():
    return ADMIN_HTML.replace("__PRINTER_ID__", PRINTER_ID)

# ================================================================
# Handler
# ================================================================
class FiscalSimulatorHandler(http.server.BaseHTTPRequestHandler):
    server_version = "ErpNetFP-Sim/2.4"

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Access-Control-Request-Private-Network")
        self.send_header("Access-Control-Allow-Private-Network", "true")

    def _send_json(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, status, html):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, code, text):
        self._send_json(status, {"ok": "false", "success": False,
                                 "messages": [{"type": "error", "code": code, "text": text}]})

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    # ---------------- GET ----------------
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        # GET /printers/taskinfo?id=X (folosit de bridge)
        if path in ("/printers/taskinfo", "/printers/taskinfo/"):
            task_id = (qs.get("id") or [None])[0]
            if not task_id:
                self._send_json(400, {"ok": "false", "error": "id required"}); return
            with tasks_lock:
                task = tasks.get(task_id)
            if task:
                self._send_json(200, task)
            else:
                self._send_json(404, {"ok": "false", "error": "unknown task id"})
            return

        # ---------- PDF bon fiscal ----------
        if path in ("/service/receipt-pdf", "/service/receipt-pdf/"):
            rno = (qs.get("receiptNumber") or [None])[0]
            if not rno:
                self._error(400, "MISSING_PARAM", "receiptNumber required")
                return
            receipt = _get_receipt_from_ledger(rno)
            if not receipt:
                self._error(404, "NOT_FOUND", f"Bonul {rno} nu există în ledger")
                return
            try:
                pdf_bytes = _generate_receipt_pdf(receipt)
            except Exception as e:
                log_error("PDF_GEN_FAILED", str(e), {"receiptNumber": rno})
                self._error(500, "PDF_GEN_FAILED", str(e))
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'inline; filename="bon_{rno}.pdf"')
            self.send_header("Content-Length", str(len(pdf_bytes)))
            self._cors()
            self.end_headers()
            self.wfile.write(pdf_bytes)
            return

        route, params = match_route(path)

        if route in ("admin", "index"):
            self._send_html(200, render_admin()); return

        if route == "printers":
            with state_lock:
                cnt = receipt_counter
            self._send_json(200, [{
                "id": PRINTER_ID, "name": PRINTER_NAME, "model": PRINTER_MODEL,
                "fiscalMemorySerialNumber": FM_SERIAL, "firmwareVersion": FIRMWARE,
                "active": True, "lastReceiptNumber": f"{cnt:07d}", "supportsInvoice": SUPPORTS_INVOICE,
            }]); return

        if route == "printer":
            self._send_json(200, {"id": params["id"], "name": PRINTER_NAME,
                                  "model": PRINTER_MODEL,
                                  "fiscalMemorySerialNumber": FM_SERIAL,
                                  "firmwareVersion": FIRMWARE, "active": True,
                                  "supportsInvoice": SUPPORTS_INVOICE}); return

        if route == "status":
            pid = params.get("id") or PRINTER_ID
            with state_lock:
                drawer = drawer_open
                cnt = receipt_counter
                ps = dict(printer_state)
            ready = not (ps["paperOut"] or ps["coverOpen"] or ps["offline"])
            self._send_json(200, {
                "printer": {"id": pid, "model": PRINTER_MODEL,
                            "fiscalMemorySerialNumber": FM_SERIAL,
                            "firmwareVersion": FIRMWARE,
                            "lastReceiptNumber": f"{cnt:07d}"},
                "status": {
                    "ready": ready, "busy": printer_busy(),
                    "coverOpen": ps["coverOpen"], "paperOut": ps["paperOut"],
                    "drawerOpen": drawer, "offline": ps["offline"],
                    "errorMessage": self._error_message(ps),
                },
            }); return

        if route in ("taskinfo", "taskinfo_id"):
            task_id = (qs.get("id") or [None])[0]
            if task_id:
                with tasks_lock:
                    t = tasks.get(task_id)
                if t: self._send_json(200, t)
                else: self._send_json(404, {"ok": "false", "error": "unknown task id"})
                return
            ct = current_task()
            if ct: self._send_json(200, ct)
            else: self._send_json(404, {"ok": "false", "error": "no task found"})
            return

        if route == "service":
            if path.endswith("/recent-receipts"):
                with state_lock:
                    self._send_json(200, {"receipts": list(recent_receipts)})
                return
            if path.endswith("/current-task"):
                with tasks_lock:
                    tid = last_task_id
                    t = tasks.get(tid) if tid else None
                self._send_json(200, {"task_id": tid, "task": t})
                return
            if path.endswith("/errors"):
                self._send_json(200, {"errors": read_errors(200)}); return
            if path.endswith("/config"):
                with state_lock:
                    cnt = receipt_counter
                self._send_json(200, {
                    "host": HOST, "port": PORT,
                    "printer_id": PRINTER_ID, "printer_name": PRINTER_NAME,
                    "printer_model": PRINTER_MODEL, "firmware": FIRMWARE,
                    "fiscal_memory_serial": FM_SERIAL,
                    "receipt_counter_start": RECEIPT_START,
                    "receipt_counter_current": cnt,
                    "phase_in_progress_seconds": PHASE_IN_PROGRESS,
                    "phase_finished_seconds": PHASE_FINISHED,
                    "initial_state": INITIAL_STATE,
                    "state_file": STATE_FILE,
                    "ledger_file": LEDGER_FILE,
                    "errors_file": ERRORS_FILE,
                    "base_dir": BASE_DIR,
                }); return
            self._send_json(200, {"status": "running", "version": "2.4-sim",
                                  "printers": [PRINTER_ID]}); return

        self._error(404, "NOT_FOUND", f"Unknown route: {path}")

    # ---------------- POST ----------------
    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError as e:
            self._error(400, "INVALID_JSON", f"Invalid JSON: {e}"); return

        if path in ("/admin/error-mode", "/admin/error-mode/"):
            mode = (data or {}).get("mode", "normal")
            with state_lock:
                printer_state["paperOut"]  = (mode == "paperOut")
                printer_state["coverOpen"] = (mode == "coverOpen")
                printer_state["offline"]   = (mode == "offline")
                ps = dict(printer_state)
            self._send_json(200, {"ok": "true", "printer_state": ps})
            return

        if path in ("/service/clear-errors", "/service/clear-errors/"):
            try:
                open(ERRORS_FILE, "w").close()
            except OSError:
                pass
            self._send_json(200, {"ok": True}); return

        route, params = match_route(path)
        pid = params.get("id") or PRINTER_ID
        if route == "invoice":
            if not SUPPORTS_INVOICE:
                self._error(400, "E413", "Invoice is not implemented by the driver"); return
            rcp = (data or {}).get("recipient") or {}
            if not str(rcp.get("name") or "").strip() or not str(rcp.get("identifier") or "").strip():
                self._error(400, "E405", "Recipient name and identifier are required"); return
            route = "receipt"
        if route == "receipt":
            task_id = (qs.get("taskId") or [None])[0]
            async_mode = (qs.get("asyncTimeout") or [None])[0] == "0"
            self._handle_receipt(pid, data, task_id, async_mode)
        elif route == "storno":
            self._handle_storno(pid, data)
        elif route in ("xreport", "zreport", "periodreport", "report"):
            kind = route
            if kind == "report":
                raw_type = str((data or {}).get("type") or (data or {}).get("reportType") or "x").lower().strip()
                if "z" in raw_type: kind = "zreport"
                elif "period" in raw_type or "mf" in raw_type or "month" in raw_type: kind = "periodreport"
                else: kind = "xreport"
            task_id = (qs.get("taskId") or [None])[0]
            async_mode = (qs.get("asyncTimeout") or [None])[0] == "0"
            self._handle_report(kind, data, task_id, async_mode)
        elif route == "open-drawer":
            self._handle_open_drawer(pid)
        elif route == "raw":
            self._handle_raw(pid, data)
        elif route == "datetime":
            self._handle_datetime(pid, data)
        else:
            self._error(404, "NOT_FOUND", f"Unknown route: {path}")

    # ---------------- helpers ----------------
    @staticmethod
    def _error_message(ps):
        if ps["offline"]:   return "Printer offline"
        if ps["coverOpen"]: return "Cover open"
        if ps["paperOut"]:  return "Paper out"
        return None

    def _build_error_result(self, code, text):
        return {"ok": "false", "success": False,
                "messages": [{"type": "error", "code": code, "text": text}]}

    # ---------------- receipt ----------------
    def _handle_receipt(self, printer_id, data, task_id, async_mode):
        global last_task_id

        # Idempotență pe taskId
        if task_id:
            with tasks_lock:
                cached = tasks.get(task_id)
            if cached:
                if cached["taskStatus"] == "finished" and not async_mode:
                    self._send_json(200, cached["result"])
                else:
                    self._send_json(200, {"taskId": task_id})
                return

        result = self._compute_result(data)
        effective_id = task_id or f"sim_{int(time.time()*1000)}"

        if async_mode:
            with tasks_lock:
                tasks[effective_id] = {"taskStatus": "pending", "result": None}
            with state_lock:
                last_task_id = effective_id
            self._schedule_transitions(effective_id, result)
            self._send_json(200, {"taskId": effective_id})
        else:
            with tasks_lock:
                tasks[effective_id] = {"taskStatus": "finished", "result": result}
            with state_lock:
                last_task_id = effective_id
            self._send_json(200, result)

    def _schedule_transitions(self, task_id, result):
        def _set(status, result_val=None):
            with tasks_lock:
                if task_id in tasks:
                    tasks[task_id]["taskStatus"] = status
                    if result_val is not None:
                        tasks[task_id]["result"] = result_val

        def _do():
            _set("in-progress")
            time.sleep(PHASE_FINISHED - PHASE_IN_PROGRESS)
            _set("finished", result)
            if result.get("ok") == "true":
                rno = (result.get("receiptNumber") or result.get("stornoNumber")
                       or (f"Z #{result.get('zNumber')}" if result.get("zNumber") else None)
                       or result.get("reportType") or "OK")
                ramt = result.get("receiptAmount") or result.get("stornoAmount") or result.get("total")
                amt_str = f" · {ramt} RON" if ramt is not None else ""
                _safe_print(f"[PRINT] Task {task_id} OK · {rno}{amt_str}")
            else:
                msgs = result.get("messages") or [{}]
                code = msgs[0].get("code", "UNKNOWN")
                text = msgs[0].get("text", "")
                log_error(code, text, {"task_id": task_id})
                _safe_print(f"[PRINT] Task {task_id} EȘUAT · {code}")

        threading.Timer(PHASE_IN_PROGRESS, _do).start()

    def _compute_result(self, data):
        global receipt_counter, receipts_seen, recent_receipts

        with state_lock:
            ps = dict(printer_state)
        if ps["offline"]:
            return self._build_error_result("OFFLINE", "Printer is offline")
        if ps["coverOpen"]:
            return self._build_error_result("COVER_OPEN", "Printer cover is open")
        if ps["paperOut"]:
            return self._build_error_result("PAPER_OUT", "Printer is out of paper")

        items = data.get("items") or data.get("receipt", {}).get("items", [])
        if not isinstance(items, list) or not items:
            return self._build_error_result("NO_ITEMS",
                                            "Receipt must contain at least one item")
        for idx, it in enumerate(items):
            if not isinstance(it, dict):
                return self._build_error_result("INVALID_ITEM",
                                                f"Item {idx} is not an object")
            if it.get("type") in ("comment", "footer-comment", "separator"):
                continue
            if not it.get("text") and not it.get("name"):
                return self._build_error_result("ITEM_TEXT_MISSING",
                                                f"Item {idx} has no text/name")
            try:
                if Decimal(str(it.get("quantity", 1))) <= 0:
                    raise ValueError
            except Exception:
                return self._build_error_result("INVALID_QTY",
                                                f"Item {idx} quantity must be > 0")

        usn = data.get("uniqueSaleNumber")
        if usn:
            with state_lock:
                cached = receipts_seen.get(usn)
            if cached:
                return cached

        total = total_of(items)
        pays = data.get("payments") or []
        if pays:
            paid = sum((money(p.get("amount", 0)) for p in pays if isinstance(p, dict)), Decimal("0"))
            if paid < total:
                return self._build_error_result(
                    "E406", f"Payments {paid} do not cover the receipt total {total}")
        with state_lock:
            receipt_counter += 1
            rno = f"{receipt_counter:07d}"
            ts = now_iso()
            result = {
                "ok": "true", "success": True,
                "messages": [{"type": "info",
                              "text": "Simulated fiscal receipt printed successfully"}],
                "receiptNumber": rno,
                "receiptDateTime": ts,
                "receiptAmount": float(total),
                "fiscalMemorySerialNumber": FM_SERIAL,
                "_usn": usn,
            }
            if usn:
                receipts_seen[usn] = {k: v for k, v in result.items()
                                      if not k.startswith("_")}
                # cap idempotență
                if len(receipts_seen) > MAX_RECEIPTS_SEEN:
                    # păstrează cele mai recente (dict ordonat din Python 3.7+)
                    excess = len(receipts_seen) - MAX_RECEIPTS_SEEN
                    for k in list(receipts_seen.keys())[:excess]:
                        receipts_seen.pop(k, None)
            recent_receipts.insert(0, {
                "receiptNumber": rno, "receiptDateTime": ts,
                "uniqueSaleNumber": usn, "receiptAmount": float(total), "ok": True,
            })
            del recent_receipts[MAX_RECENT:]

        # Persistență + jurnal fiscal
        ledger_append({
            "type": "receipt",
            "timestamp": ts,
            "receiptNumber": rno,
            "receiptAmount": float(total),
            "fiscalMemorySerialNumber": FM_SERIAL,
            "uniqueSaleNumber": usn,
            "items": items,
            "payments": data.get("payments") or [],
            "recipient": data.get("recipient") or None,
        })
        save_bon_file(data)
        save_state()

        return {k: v for k, v in result.items() if not k.startswith("_")}

    # ---------------- rapoarte X / Z / perioadă (memorie fiscală) ----------------
    def _report_result(self, kind, data):
        with state_lock:
            ps = dict(printer_state)
        msg = self._error_message(ps)
        if msg:
            return self._build_error_result(msg.upper().replace(" ", "_"), msg)
        entries = []
        try:
            with open(LEDGER_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try: entries.append(json.loads(line))
                    except json.JSONDecodeError: pass
        except FileNotFoundError:
            pass

        ts = now_iso()

        if kind == "periodreport":
            raw_from = str(data.get("fromDate") or data.get("from_date") or data.get("startDate") or "").strip()[:19]
            raw_to = str(data.get("toDate") or data.get("to_date") or data.get("endDate") or "").strip()[:19]
            if raw_from and len(raw_from) == 10:
                raw_from += "T00:00:00"
            if raw_to and len(raw_to) == 10:
                raw_to += "T23:59:59"
            elif not raw_to:
                raw_to = "9999-12-31T23:59:59"

            sel = [e for e in entries if e.get("type") == "zreport" and raw_from <= (e.get("timestamp") or "")[:19] <= raw_to]
            total = sum(Decimal(str(e.get("total", 0))) for e in sel)
            rep_type = data.get("type", "short")
            ledger_append({
                "type": "periodreport", "timestamp": ts, "fromDate": raw_from, "toDate": raw_to,
                "reportType": rep_type, "zCount": len(sel), "total": float(total),
                "fiscalMemorySerialNumber": FM_SERIAL,
            })
            period_str = f"{raw_from[:10]} - {raw_to[:10]}" if raw_from else "Toata perioada"
            text = f"Raport memorie fiscala {period_str}: {len(sel)} rapoarte Z, total {money(total)} RON"
            _safe_print(f"[REPORT] {text}")
            return {
                "ok": "true",
                "success": True,
                "reportType": "periodreport",
                "fromDate": raw_from,
                "toDate": raw_to,
                "zCount": len(sel),
                "total": float(total),
                "fiscalMemorySerialNumber": FM_SERIAL,
                "receiptDateTime": ts,
                "messages": [{"type": "info", "text": text}],
            }

        # X/Z: bonurile de după ultimul raport Z
        last_z = max((i for i, e in enumerate(entries) if e.get("type") == "zreport"), default=-1)
        day = [e for e in entries[last_z + 1:] if e.get("type") in ("receipt", "storno")]
        by_pay, total, count = {}, Decimal("0"), 0
        for e in day:
            sign = -1 if e.get("type") == "storno" else 1
            amt = Decimal(str(e.get("receiptAmount") or e.get("stornoAmount") or 0)) * sign
            total += amt
            count += 1
            pays = e.get("payments") or [{"paymentType": "cash", "amount": float(amt)}]
            for pmt in pays:
                key = pmt.get("paymentType", "cash")
                by_pay[key] = by_pay.get(key, Decimal("0")) + Decimal(str(pmt.get("amount", 0))) * sign

        pay_summary = ", ".join(f"{k}: {money(v)} RON" for k, v in by_pay.items()) if by_pay else "fara incasari"
        text = f"Raport {'Z' if kind == 'zreport' else 'X'} {ts}: {count} documente, total {money(total)} RON ({pay_summary})"

        out = {
            "ok": "true",
            "success": True,
            "reportType": kind,
            "docCount": count,
            "total": float(total),
            "byPayment": {k: float(v) for k, v in by_pay.items()},
            "fiscalMemorySerialNumber": FM_SERIAL,
            "receiptDateTime": ts,
        }

        if kind == "zreport":
            z_no = sum(1 for e in entries if e.get("type") == "zreport") + 1
            out["zNumber"] = z_no
            text = f"Z nr. {z_no} - " + text
            ledger_append({
                "type": "zreport", "timestamp": ts, "zNumber": z_no, "count": count,
                "total": float(total), "byPayment": {k: float(v) for k, v in by_pay.items()},
                "fiscalMemorySerialNumber": FM_SERIAL,
            })
            xml_path = write_test_anaf_xml(z_no, ts, total, by_pay, count)
            if xml_path:
                _safe_print(f"[ANAF XML] Fișier de test: {xml_path}")
        else:
            ledger_append({
                "type": "xreport", "timestamp": ts, "count": count,
                "total": float(total), "byPayment": {k: float(v) for k, v in by_pay.items()},
                "fiscalMemorySerialNumber": FM_SERIAL,
            })

        out["messages"] = [{"type": "info", "text": text}]
        _safe_print(f"[REPORT] {text}")
        return out

    def _handle_report(self, kind, data, task_id, async_mode):
        global last_task_id
        if task_id:
            with tasks_lock:
                cached = tasks.get(task_id)
            if cached:
                if cached.get("taskStatus") == "finished" and not async_mode:
                    self._send_json(200, cached.get("result") or {})
                else:
                    self._send_json(200, {"taskId": task_id})
                return
        result = self._report_result(kind, data)
        effective_id = task_id or f"sim_{int(time.time()*1000)}"

        with state_lock:
            last_task_id = effective_id
            if result.get("ok") == "true":
                z_no = result.get("zNumber")
                r_label = f"Z #{z_no}" if kind == "zreport" else ("Raport X" if kind == "xreport" else "Raport MF")
                recent_receipts.insert(0, {
                    "receiptNumber": r_label,
                    "receiptDateTime": result.get("receiptDateTime") or now_iso(),
                    "uniqueSaleNumber": kind.upper(),
                    "receiptAmount": float(result.get("total", 0)),
                    "amount": f"{float(result.get('total', 0)):.2f}",
                    "ok": True,
                    "status": "OK",
                })
                del recent_receipts[MAX_RECENT:]
        save_state()

        if async_mode:
            with tasks_lock:
                tasks[effective_id] = {"taskStatus": "finished", "result": result}
            self._send_json(200, {"taskId": effective_id})
        else:
            with tasks_lock:
                tasks[effective_id] = {"taskStatus": "finished", "result": result}
            self._send_json(200, result)

    # ---------------- storno / drawer / raw / datetime ----------------
    def _handle_storno(self, printer_id, data):
        global receipt_counter

        with state_lock:
            ps = dict(printer_state)
        if ps["offline"] or ps["coverOpen"] or ps["paperOut"]:
            msg = self._error_message(ps)
            code = msg.upper().replace(" ", "_")
            log_error(code, msg, {"endpoint": "storno"})
            self._send_json(400, self._build_error_result(code, msg))
            return

        items = data.get("items") or data.get("receipt", {}).get("items", [])
        if not items:
            self._error(400, "NO_ITEMS", "Storno must contain at least one item")
            return
        total = total_of(items)
        with state_lock:
            receipt_counter += 1
            rno = f"{receipt_counter:07d}"
            ts = now_iso()

        ledger_append({
            "type": "storno", "timestamp": ts, "stornoNumber": rno,
            "stornoAmount": float(total), "fiscalMemorySerialNumber": FM_SERIAL,
            "items": items,
        })
        save_bon_file(data)
        save_state()

        self._send_json(200, {"ok": "true", "success": True,
                              "stornoNumber": rno, "stornoAmount": float(total),
                              "receiptDateTime": ts,
                              "fiscalMemorySerialNumber": FM_SERIAL})

    def _handle_open_drawer(self, printer_id):
        global drawer_open
        with state_lock:
            drawer_open = True
        threading.Timer(3.0, self._close_drawer).start()
        self._send_json(200, {"ok": "true", "success": True})

    def _close_drawer(self):
        global drawer_open
        with state_lock:
            drawer_open = False

    def _handle_raw(self, printer_id, data):
        self._send_json(200, {"ok": "true", "success": True})

    def _handle_datetime(self, printer_id, data):
        dt = data.get("datetime") or now_iso()
        self._send_json(200, {"ok": "true", "success": True, "datetime": dt})

    def log_message(self, fmt, *args):
        _safe_print(f"[{self.log_date_time_string()}] {self.address_string()} {fmt % args}")


# ================================================================
# Server + graceful shutdown
# ================================================================
def preflight_check(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port)); return True
        except OSError:
            return False

def _graceful_shutdown(signum=None, frame=None):
    _safe_print("\n[Simulator] Oprire — salvez state...")
    save_state()
    _safe_print("[Simulator] State salvat. La revedere.")
    os._exit(0)

if __name__ == "__main__":
    if not preflight_check(HOST, PORT):
        msg = f"Portul {PORT} pe {HOST} este ocupat."
        log_error("PORT_BUSY", msg, {"port": PORT, "host": HOST})
        _safe_print(f"[EROARE] {msg}")
        raise SystemExit(1)

    load_state()

    try: signal.signal(signal.SIGINT,  _graceful_shutdown)
    except (ValueError, OSError): pass
    try: signal.signal(signal.SIGTERM, _graceful_shutdown)
    except (ValueError, OSError): pass

    server = http.server.ThreadingHTTPServer((HOST, PORT), FiscalSimulatorHandler)
    server.allow_reuse_address = True

    _safe_print("=" * 60)
    _safe_print(f"ErpNet.FP Simulator  ·  http://{HOST}:{PORT}")
    _safe_print(f"Admin                ·  http://{HOST}:{PORT}/admin")
    _safe_print(f"Config activ         ·  http://{HOST}:{PORT}/service/config")
    _safe_print(f"Erori                ·  http://{HOST}:{PORT}/service/errors")
    _safe_print("-" * 60)
    _safe_print(f"Base dir             · {BASE_DIR}")
    _safe_print(f"Printer ID           · {PRINTER_ID}")
    _safe_print(f"Serial FM            · {FM_SERIAL}")
    _safe_print(f"Firmware             · {FIRMWARE}")
    _safe_print(f"Counter curent       · {receipt_counter:07d}")
    _safe_print(f"Idempotență cache    · {len(receipts_seen)} chei")
    _safe_print(f"State file           · {STATE_FILE}")
    _safe_print(f"Ledger file          · {LEDGER_FILE}")
    _safe_print(f"Errors file          · {ERRORS_FILE}")
    _safe_print(f"Phase in-progress    · {PHASE_IN_PROGRESS}s")
    _safe_print(f"Phase finished       · {PHASE_FINISHED}s")
    _safe_print(f"Initial state        · paperOut={INITIAL_STATE['paperOut']} "
                f"coverOpen={INITIAL_STATE['coverOpen']} "
                f"offline={INITIAL_STATE['offline']}")
    _safe_print("=" * 60)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _graceful_shutdown()