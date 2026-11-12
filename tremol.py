import os
import sys
import time
import shutil
import random
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Căile conform configurării implicite din bridge
FOLDER_BONURI = r"C:\ServerTremol\Bonuri"
FOLDER_OUT = r"C:\ServerTremol"  # Aici stă raspuns.txt

FOLDER_DONE = os.path.join(FOLDER_BONURI, "done")
FOLDER_ERROR = os.path.join(FOLDER_BONURI, "error")
FOLDER_BAD = os.path.join(FOLDER_BONURI, "bad")

def main():
    # Creăm structura completă de foldere
    for folder in [FOLDER_BONURI, FOLDER_OUT, FOLDER_DONE, FOLDER_ERROR, FOLDER_BAD]:
        os.makedirs(folder, exist_ok=True)
    
    print("=== Tremol Server Simulator (Muta in DONE/ERROR) Pornit ===")
    print(f"Monitorizez folderul: {FOLDER_BONURI}")
    print("Aștept bonuri de la Sincrona Bridge... (Ctrl+C pentru oprire)\n")
    
    z_counter = 8
    bon_counter = 100

    try:
        while True:
            # Citim toate fișierele .txt din folderul rădăcină Bonuri/
            for filename in os.listdir(FOLDER_BONURI):
                filepath = os.path.join(FOLDER_BONURI, filename)
                
                # Ignorăm directoarele, procesăm doar fișierele txt
                if os.path.isfile(filepath) and filename.endswith(".txt"):
                    print(f"\n[Tremol SIM] Comandă NOUĂ primită: {filename}")
                    
                    time.sleep(1)
                    
                    try:
                        total_plata = "0.00"
                        force_error = False
                        lines = []
                        
                        with open(filepath, 'r', encoding='cp1250', errors='replace') as f:
                            lines = [l.strip() for l in f.readlines()]
                            for line in lines:
                                print(f"  -> {line}")
                                if line.startswith("P|"):
                                    parts = line.split("|")
                                    if len(parts) >= 3:
                                        total_plata = parts[2].strip()
                                if "9999" in line:
                                    force_error = True

                        raspuns_path = os.path.join(FOLDER_OUT, "raspuns.txt")
                        
                        # Identificare tip comandă: Raport Z, Raport X, Memorie Fiscală sau Bon Fiscal
                        is_z = "bon.z." in filename.lower() or any(l == "Z" for l in lines)
                        is_x = "bon.x." in filename.lower() or any(l == "X" for l in lines)
                        is_period = "raportperiodic" in filename.lower() or any(l.startswith("F|") for l in lines)

                        if force_error:
                            print(f"[Tremol SIM] Generare EROARE: Mut fișierul în error/ și scriu raspuns.txt...")
                            with open(raspuns_path, 'w', encoding='cp1250') as out_f:
                                out_f.write("ERR: Lipsa hartie in imprimanta")
                            shutil.move(filepath, os.path.join(FOLDER_ERROR, filename))
                        elif is_z:
                            z_counter += 1
                            doc_count = 2
                            total = 586.03
                            cash = 351.28
                            card = 234.75
                            dt_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            print(f"[Tremol SIM] Raport Z executat: Z nr. {z_counter}, {doc_count} doc, total {total} RON (card: {card}, cash: {cash})")
                            # Răspuns oficial Tremol ZFPServer: 0|OK|[NumarZ]|[SerieFiscala]|[TotalGeneral]|[TotalNumerar]|[TotalCard]|[NumarBonuri]|[DataOra]
                            with open(raspuns_path, 'w', encoding='cp1250') as out_f:
                                out_f.write(f"0|OK|{z_counter}|TR87654321|{total:.2f}|{cash:.2f}|{card:.2f}|{doc_count}|{dt_str}")
                            shutil.move(filepath, os.path.join(FOLDER_DONE, filename))
                        elif is_x:
                            doc_count = 2
                            total = 586.03
                            cash = 351.28
                            card = 234.75
                            dt_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            print(f"[Tremol SIM] Raport X executat: {doc_count} doc, total {total} RON")
                            with open(raspuns_path, 'w', encoding='cp1250') as out_f:
                                out_f.write(f"0|OK|X|TR87654321|{total:.2f}|{cash:.2f}|{card:.2f}|{doc_count}|{dt_str}")
                            shutil.move(filepath, os.path.join(FOLDER_DONE, filename))
                        elif is_period:
                            total = 14250.80
                            cash = 8050.80
                            card = 6200.00
                            doc_count = z_counter * 5
                            dt_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            print(f"[Tremol SIM] Raport Memorie Fiscală executat: total {total} RON")
                            with open(raspuns_path, 'w', encoding='cp1250') as out_f:
                                out_f.write(f"0|OK|PERIODIC|TR87654321|{total:.2f}|{cash:.2f}|{card:.2f}|{doc_count}|{dt_str}")
                            shutil.move(filepath, os.path.join(FOLDER_DONE, filename))
                        else:
                            bon_counter += 1
                            print(f"[Tremol SIM] Bon SUCCESS: Mut fișierul în done/ și scriu raspuns.txt cu nr {bon_counter}...")
                            with open(raspuns_path, 'w', encoding='cp1250') as out_f:
                                out_f.write(f"0|OK|{bon_counter}|TR87654321|{total_plata}")
                            shutil.move(filepath, os.path.join(FOLDER_DONE, filename))
                            
                        print("[Tremol SIM] Răspuns trimis cu succes către Bridge!")
                        
                    except Exception as e:
                        print(f"[Eroare] Nu am putut procesa fișierul: {e}")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nSimulator oprit.")

if __name__ == "__main__":
    main()
