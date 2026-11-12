import os
import sys
import time
import random
import json
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Căile implicite din DatecsDriver
FOLDER_IN = r"C:\Datecs\In"
FOLDER_OUT = r"C:\Datecs\Out"
STATE_FILE = "datecs_sim_state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {
        "bon_counter": 0,
        "z_counter": 0,
        "daily_doc_count": 0,
        "daily_total": 0.0,
        "daily_card": 0.0,
        "daily_cash": 0.0,
        "grand_total": 0.0
    }

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except:
        pass

def main():
    os.makedirs(FOLDER_IN, exist_ok=True)
    os.makedirs(FOLDER_OUT, exist_ok=True)
    
    state = load_state()
    
    print("=== Datecs DUDE / FPrinter Simulator (Mod Fiscal Realist) Pornit ===")
    print(f"Monitorizez IN:  {FOLDER_IN}")
    print(f"Stare curentă: Z={state['z_counter']}, Bonuri Azi={state['daily_doc_count']}, Bon Curent={state['bon_counter']}")
    print("Aștept comenzi (.inp) de la Sincrona Bridge... (Ctrl+C pentru oprire)\n")
    
    fisiere_procesate = set()

    try:
        while True:
            for filename in os.listdir(FOLDER_IN):
                if filename.endswith(".inp") and filename not in fisiere_procesate:
                    filepath = os.path.join(FOLDER_IN, filename)
                    print(f"\n[Datecs SIM] Comandă NOUĂ primită: {filename}")
                    
                    time.sleep(1)
                    
                    try:
                        force_error = False
                        lines = []
                        with open(filepath, 'r', encoding='cp1250', errors='replace') as f:
                            lines = [l.strip() for l in f.readlines()]
                            for line in lines:
                                print(f"  -> {line}")
                                if "EroareTest" in line:
                                    force_error = True

                        base_name = filename.replace(".inp", "")
                        out_filepath = os.path.join(FOLDER_OUT, f"{base_name}.out")
                        
                        # Identificare tip comandă
                        is_z = "rep_z" in filename.lower() or any(l.startswith("69,Z") or l.startswith("Z,1") for l in lines)
                        is_x = "rep_x" in filename.lower() or any(l.startswith("69,X") or l.startswith("X,1") for l in lines)
                        is_period = "rep_period" in filename.lower() or any(l.startswith("94,") or l.startswith("M,1") for l in lines)

                        with open(out_filepath, 'w', encoding='cp1250') as out_f:
                            if force_error:
                                print(f"[Datecs SIM] Simulez EROARE pentru {filename}...")
                                out_f.write("-1;112;Lipsa hartie;Comanda respinsa")
                            
                            elif is_z:
                                state['z_counter'] += 1
                                print(f"[Datecs SIM] Raport Z executat: Z nr. {state['z_counter']}, {state['daily_doc_count']} doc, total {state['daily_total']:.2f} RON (card: {state['daily_card']:.2f}, cash: {state['daily_cash']:.2f})")
                                
                                out_f.write(f"0;0;0;{state['z_counter']};{state['daily_doc_count']};{state['daily_total']:.2f};{state['daily_card']:.2f};{state['daily_cash']:.2f};TR123456;")
                                
                                # Resetare contoare zilnice după Z
                                state['daily_doc_count'] = 0
                                state['daily_total'] = 0.0
                                state['daily_card'] = 0.0
                                state['daily_cash'] = 0.0
                                save_state(state)
                                
                            elif is_x:
                                print(f"[Datecs SIM] Raport X executat: {state['daily_doc_count']} doc, total {state['daily_total']:.2f} RON")
                                out_f.write(f"0;0;0;0;{state['daily_doc_count']};{state['daily_total']:.2f};{state['daily_card']:.2f};{state['daily_cash']:.2f};TR123456;")
                                
                            elif is_period:
                                print(f"[Datecs SIM] Raport Memorie Fiscală executat: total {state['grand_total']:.2f} RON")
                                out_f.write(f"0;0;0;1-{state['z_counter']};{state['z_counter']};{state['grand_total']:.2f};0.00;0.00;TR123456;")
                                
                            else:
                                state['bon_counter'] += 1
                                state['daily_doc_count'] += 1
                                
                                # Extrage valoarea și tipul plății din DUDE (comanda 53)
                                b_total = 0.0
                                b_card = 0.0
                                b_cash = 0.0
                                
                                for l in lines:
                                    if l.startswith("53,"):
                                        p_parts = l.split("\t")
                                        if len(p_parts) >= 3 and p_parts[2]:
                                            val = float(p_parts[2])
                                            b_total += val
                                            p_type = p_parts[0].split(",")[1]
                                            if p_type == "1" or p_type == "8": # Card/Bank
                                                b_card += val
                                            else:
                                                b_cash += val

                                # Dacă nu găsim comenzi 53, căutăm formatul FPrinter vechi (P,1...)
                                if b_total == 0.0:
                                    for l in lines:
                                        if l.startswith("P,1,"):
                                            parts = l.split(";")
                                            if len(parts) >= 3:
                                                val = float(parts[2])
                                                b_total += val
                                                p_type = parts[1]
                                                if p_type == "1" or p_type == "3":
                                                    b_card += val
                                                else:
                                                    b_cash += val
                                
                                state['daily_total'] += b_total
                                state['daily_card'] += b_card
                                state['daily_cash'] += b_cash
                                state['grand_total'] += b_total
                                save_state(state)
                                
                                print(f"[Datecs SIM] Bon Fiscal OK! Nr bon: {state['bon_counter']}, total: {b_total:.2f} RON")
                                out_f.write(f"0;0;0;{state['bon_counter']};0;{b_total:.2f};TR123456;")
                        
                        fisiere_procesate.add(filename)
                        
                    except Exception as e:
                        print(f"[Eroare Datecs SIM] {e}")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nSimulator Datecs oprit.")

if __name__ == "__main__":
    main()