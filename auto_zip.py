import zipfile
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
ZIP_PATH = PROJECT_DIR.parent / "tasador_simple.zip"
EXCLUDED_DIRS = {
    "__pycache__",
}
EXCLUDED_SUFFIXES = {
    ".pyc",
    ".zip",
}


def should_include(path: Path) -> bool:
    if path.is_dir():
        return False

    try:
        resolved_path = path.resolve()
    except OSError:
        return False

    if resolved_path == ZIP_PATH.resolve():
        return False

    try:
        relative_parts = resolved_path.relative_to(PROJECT_DIR).parts
    except ValueError:
        return False

    if any(part in EXCLUDED_DIRS for part in relative_parts):
        return False

    if resolved_path.suffix.lower() in EXCLUDED_SUFFIXES:
        return False

    return True


def create_backup_zip():
    print(f"Creando backup de: {PROJECT_DIR}")
    print(f"Archivo destino: {ZIP_PATH}")

    try:
        if ZIP_PATH.exists():
            ZIP_PATH.unlink()
            print("ZIP previo eliminado")

        files_added = 0

        with zipfile.ZipFile(
            ZIP_PATH,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
        ) as zip_file:
            for file_path in PROJECT_DIR.rglob("*"):
                if not should_include(file_path):
                    continue

                archive_name = file_path.relative_to(PROJECT_DIR.parent)
                zip_file.write(file_path, archive_name)
                files_added += 1

        print(f"ZIP creado correctamente: {ZIP_PATH}")
        print(f"Archivos incluidos: {files_added}")
    except OSError as error:
        print(f"Error creando backup ZIP: {error}")
    except zipfile.BadZipFile as error:
        print(f"Error generando archivo ZIP: {error}")


if __name__ == "__main__":
    create_backup_zip()
