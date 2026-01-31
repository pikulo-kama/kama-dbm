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
