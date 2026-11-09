"""
Sincrona Bridge - Automated Nuitka Build Script
"""

import os
import shutil
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def build():
    print("=" * 60)
    print(" Sincrona Bridge - Nuitka C++ Native Compiler")
    print("=" * 60)

    dist_dir = os.path.join(BASE_DIR, "dist")
    if os.path.exists(dist_dir):
        print("[1/3] Curatare folder dist...")
        shutil.rmtree(dist_dir, ignore_errors=True)

    cmd = [
        sys.executable,
        "-m", "nuitka",
        "--standalone",
        "--windows-console-mode=disable",
        "--assume-yes-for-downloads",
        "--enable-plugin=anti-bloat",
        "--include-package=watchdog",
        "--include-package=websockets",
        "--include-package=requests",
        "--output-dir=dist",
        "--output-filename=SincronaBridge.exe",
        "erpnet_fp_bridge.py"
    ]

    print("[2/3] Lansare compilator Nuitka...")
    print("Comanda:", " ".join(cmd))
    res = subprocess.run(cmd, cwd=BASE_DIR)
    if res.returncode != 0:
        print(f"[EROARE] Nuitka a iesit cu codul {res.returncode}")
        return res.returncode

    # Copiere nssm.exe in folderul rezultat
    target_dist = os.path.join(dist_dir, "erpnet_fp_bridge.dist")
    nssm_src = os.path.join(BASE_DIR, "nssm.exe")
    if os.path.exists(nssm_src) and os.path.isdir(target_dist):
        print("[3/3] Copiere nssm.exe in pachet...")
        shutil.copy2(nssm_src, target_dist)

    print("=" * 60)
    print(f" Build Nuitka finalizat cu succes in: {target_dist}")
    print("=" * 60)
    return 0

if __name__ == "__main__":
    sys.exit(build())
