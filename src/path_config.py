# Nombre de archivo: path_config.py
# Guardar en la carpeta: src/
#
# Descripción: Centraliza la lógica para encontrar la raíz del proyecto
# y definir las rutas a las carpetas de datos, tanto en modo de
# desarrollo (script) como en modo de producción (app empaquetada).

import sys
import os


def get_project_root():
    """
    Determina la ruta raíz del proyecto, ya sea ejecutándose como script
    o como una aplicación empaquetada (PyInstaller).

    Se asume que la carpeta 'data' está:
    1. AL LADO del ejecutable (.app / .exe) si está empaquetado.
    2. Un nivel ARRIBA de la carpeta 'src' si se ejecuta como script.
    """
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Modo empaquetado (PyInstaller)
        # sys.executable es la ruta al ejecutable
        # (ej. .../ReportesCASA.app/Contents/MacOS/ReportesCASA)
        base_path = os.path.dirname(sys.executable)

        # Si es una .app de macOS, subimos 3 niveles para salir de
        # .../Contents/MacOS/ y llegar al directorio QUE CONTIENE la .app
        if ".app" in base_path:
            base_path = os.path.abspath(os.path.join(base_path, '..', '..', '..'))

        # Si es un .exe de Windows, base_path es el directorio
        # que contiene el .exe, lo cual es correcto.

        # ¡Importante! Cambiamos el CWD al directorio base de la app.
        os.chdir(base_path)
        return base_path  # Directorio que CONTIENE la .app o .exe

    else:
        # Modo script normal (ej. python src/file_merger.py)
        # __file__ es .../src/path_config.py
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Devolvemos el directorio padre (.../), que es la raíz del proyecto
        return os.path.abspath(os.path.join(script_dir, '..'))


# --- Definir la raíz del proyecto ---
PROJECT_ROOT = get_project_root()

# --- Definir CONSTANTES GLOBALES de rutas ---
# Todas las rutas ahora son ABSOLUTAS
DATA_FOLDER = os.path.join(PROJECT_ROOT, 'data')
RAW_FOLDER = os.path.join(DATA_FOLDER, 'raw')
PROCESSED_DIR = os.path.join(DATA_FOLDER, 'procesada')
REPORTS_FOLDER = os.path.join(DATA_FOLDER, 'reportes', 'programa')

# Archivos específicos
BASE_FILE = os.path.join(RAW_FOLDER, 'base.xlsx')
ADMITIDOS_FILE = os.path.join(RAW_FOLDER, 'admitidos.xlsx')
CONSOLIDATED_FILE = os.path.join(PROCESSED_DIR, 'base_consolidada.xlsx')
STUDENT_MAP_FILE = os.path.join(PROCESSED_DIR, 'student_program_map.csv')

# Para mostrar en la GUI
print(f"Raíz del proyecto establecida en: {PROJECT_ROOT}")
print(f"Carpeta de datos: {DATA_FOLDER}")