# PyInstaller build spec for the GUI app.

# NOTE: you may need to tweak hiddenimports depending on installed extras.

from PyInstaller.utils.hooks import collect_data_files

datas = collect_data_files("doj_disclosures")

a = Analysis(
    ["src/doj_disclosures/app.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="doj-disclosures-gui",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)
