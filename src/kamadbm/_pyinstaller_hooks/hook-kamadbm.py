import os
import sys

binaries = []

is_windows = sys.platform == "win32"
scripts_directory = "Scripts" if is_windows else "bin"
exe_name = "kama-dbm.exe" if is_windows else "kama-dbm"

exe_path = os.path.join(sys.prefix, scripts_directory, exe_name)

if os.path.exists(exe_path):
    binaries.append((exe_path, "."))
