# build.py
#
# @author: Adrian Esteban Velasquez Solano
# @date: 10-2025
#
# In collaboration with CASA - Centro de Aseguramiento del Aprendizaje
# Universidad de los Andes
# Facultad de Administración
# Bogotá D.C., Colombia
#
# Este script automatiza la compilación de la aplicación ReportesCASA
# usando PyInstaller.
# 1. Limpia compilaciones antiguas.
# 2. Ejecuta PyInstaller para crear el .app/.exe en la carpeta raíz.
# 3. Limpia los archivos temporales de compilación.
#
# Para ejecutar:
# 1. Ejecuta este script desde la raíz: python build.py

import os
import platform
import shutil
import subprocess
import sys

# --- Configuración ---
APP_NAME = "ReportesCASA"
MAIN_SCRIPT = os.path.join('src', 'gui.py')
SRC_DIR = os.path.join(os.getcwd(), 'src')

# Directorios de PyInstaller (usaremos la raíz para todo)
PROJECT_ROOT = os.getcwd()
DIST_PATH = PROJECT_ROOT  # Salida final (.app/.exe)
BUILD_PATH = os.path.join(PROJECT_ROOT, 'build_temp')  # Archivos temporales
SPEC_FILE = os.path.join(PROJECT_ROOT, f"{APP_NAME}.spec")


def clear_old_builds():
    """Limpia archivos y carpetas de compilaciones anteriores."""
    print("--- Limpiando compilaciones anteriores... ---")

    # Eliminar carpeta de build temporal
    if os.path.exists(BUILD_PATH):
        shutil.rmtree(BUILD_PATH)

    # Eliminar archivo .spec
    if os.path.exists(SPEC_FILE):
        os.remove(SPEC_FILE)

    # Eliminar .exe o .app de la raíz
    exe_path = os.path.join(DIST_PATH, f"{APP_NAME}.exe")
    app_path = os.path.join(DIST_PATH, f"{APP_NAME}.app")

    if platform.system() == "Windows" and os.path.exists(exe_path):
        print(f"Eliminando {exe_path}...")
        os.remove(exe_path)
    elif platform.system() == "Darwin" and os.path.exists(app_path):
        print(f"Eliminando {app_path}...")
        shutil.rmtree(app_path)

    print("Limpieza completada.")


def run_pyinstaller():
    """Construye el ejecutable usando PyInstaller."""
    print("\n--- Iniciando compilación con PyInstaller... ---")

    # Este es el comando que se ejecutará
    # Es como escribirlo en la terminal
    reqs = [
        'pip',
        'install',
        '-r',
        'requirements.txt'
    ]
    command = [
        'pyinstaller',
        '--onefile',
        '--windowed',
        '--name', APP_NAME,
        '--distpath', DIST_PATH,  # Dónde poner el .exe/.app final
        '--workpath', BUILD_PATH,  # Dónde poner los archivos temporales
        '-p', SRC_DIR,  # Dónde encontrar los imports (file_merger, etc.)
        MAIN_SCRIPT  # El script principal
    ]

    print(f"Ejecutando comando: {' '.join(command)}")

    try:
        # Ejecutar el comando
        subprocess.run(reqs, check=True)
        subprocess.run(command, check=True)
        print("\n--- ¡Compilación exitosa! ---")
        print(f"Archivo final guardado en: {DIST_PATH}")

    except subprocess.CalledProcessError as e:
        print(f"\n--- ¡ERROR DE COMPILACIÓN! ---")
        print(f"PyInstaller falló con el código de error: {e.returncode}")
        sys.exit(1)
    except FileNotFoundError:
        print("\n--- ¡ERROR! No se encontró PyInstaller ---")
        print("Asegúrate de haberlo instalado con: pip install pyinstaller")
        sys.exit(1)


def final_cleanup():
    """Limpia los archivos temporales después de una compilación exitosa."""
    print("\n--- Limpiando archivos temporales... ---")

    if os.path.exists(BUILD_PATH):
        shutil.rmtree(BUILD_PATH)

    if os.path.exists(SPEC_FILE):
        os.remove(SPEC_FILE)

    print("Limpieza final completada.")


# --- Flujo Principal ---
if __name__ == "__main__":
    clear_old_builds()
    run_pyinstaller()
    final_cleanup()
    print("\nProceso de 'build' completado.")
