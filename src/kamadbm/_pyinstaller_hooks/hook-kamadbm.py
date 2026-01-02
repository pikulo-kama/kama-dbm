import os
import sys
import importlib.metadata

from PyInstaller.utils.hooks import collect_entry_point, copy_metadata
from PyInstaller.utils.hooks import collect_submodules, collect_data_files


binaries = []
datas, hiddenimports = collect_entry_point("kama_dbm.plugins")

# Import modules and metadata of KamaUI plugins
# since they're being discovered and invoked
# dynamically.
for dist in importlib.metadata.distributions():
    if not any(ep.group == "kama_dbm.plugins" for ep in dist.entry_points):
        continue

    # Use the actual package name (the folder name),
    # not just the metadata Name
    library_name = dist.metadata["Name"]
    packages = dist.read_text("top_level.txt").strip().splitlines()

    datas += copy_metadata(library_name)

    for package_name in packages:
        hiddenimports.append(package_name)
        hiddenimports += collect_submodules(package_name)
        datas += collect_data_files(package_name)

# Add kama-dbm CLI tool to binaries.
is_windows = sys.platform == "win32"
scripts_directory = "Scripts" if is_windows else "bin"
exe_name = "kama-dbm.exe" if is_windows else "kama-dbm"

exe_path = os.path.join(sys.prefix, scripts_directory, exe_name)

if os.path.exists(exe_path):
    binaries.append((exe_path, "bin"))
