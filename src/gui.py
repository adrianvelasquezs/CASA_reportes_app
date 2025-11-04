# Nombre de archivo: gui.py
# Guardar en la carpeta: src/
#
# @author: Adrian Esteban Velasquez Solano (Modificado por Gemini)
# @date: 10-2025
#
# Descripción: Esta es una interfaz gráfica (GUI) para gestionar el
# generador de reportes de CASA. Permite ejecutar la consolidación
# y la generación de reportes de forma secuencial.

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import sys
import os
import logging  # <-- 1. IMPORTAR LOGGING

# --- IMPORTANTE ---
# Importa la configuración de rutas PRIMERO
try:
    import path_config as paths
except ImportError:
    print("ERROR CRÍTICO: No se pudo encontrar path_config.py")

# Ahora importa el resto de scripts
try:
    import file_merger
    import program_report_generator
    import logger
except ImportError as e:
    messagebox.showerror("Error de Importación",
                         f"No se pudieron encontrar los scripts del proyecto.\n"
                         f"Asegúrate de que 'gui.py' esté en la carpeta 'src/' "
                         f"junto a 'file_merger.py' y 'program_report_generator.py'.\n"
                         f"Detalle: {e}")
    sys.exit(1)


# --- 2. NUEVA CLASE PARA MANEJAR LOGS ---
class TkinterLogHandler(logging.Handler):
    """
    Un handler de logging de Python que escribe los mensajes
    en un widget ScrolledText de Tkinter.
    """

    def __init__(self, text_widget, root):
        logging.Handler.__init__(self)
        self.text_widget = text_widget
        self.root = root

    def emit(self, record):
        """Escribe el log en el widget, de forma thread-safe."""
        msg = self.format(record)
        # Usamos root.after() para asegurar que la actualización de la GUI
        # ocurra en el hilo principal de Tkinter.
        self.root.after(0, self.thread_safe_write, f"{msg}\n")

    def thread_safe_write(self, msg):
        """Método para escribir de forma segura en el widget de texto."""
        try:
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg)
            self.text_widget.see(tk.END)  # Auto-scroll
            self.text_widget.configure(state='disabled')
        except tk.TclError:
            # Pasa si la ventana se cierra mientras se escribe
            pass


class TextRedirector:
    """Redirige la salida de stdout/stderr a un widget de texto de Tkinter."""

    def __init__(self, widget, root):
        self.widget = widget
        self.root = root

    def write(self, s):
        self.root.after(0, self.thread_safe_write, s)

    def thread_safe_write(self, s):
        try:
            self.widget.configure(state='normal')
            self.widget.insert(tk.END, s)
            self.widget.see(tk.END)
            self.widget.configure(state='disabled')
        except tk.TclError:
            pass

    def flush(self):
        pass


class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Generador de Reportes CASA")
        self.root.geometry("750x600")

        style = ttk.Style()
        style.theme_use('clam')

        main_frame = ttk.Frame(root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- 1. Frame de Instrucciones ---
        instrucciones_frame = ttk.LabelFrame(main_frame, text="Instrucciones", padding="10")
        instrucciones_frame.pack(fill=tk.X, expand=False, pady=5)
        instrucciones_texto = (
            "1. Asegúrese de que los archivos `base.xlsx` y `admitidos.xlsx` se encuentren en la carpeta `data/raw/`.\n"
            "2. Haga clic en '1. Consolidar Archivos' para unificar los datos. Espere a que termine.\n"
            "3. Una vez completada la consolidación, haga clic en '2. Generar Reportes'.\n"
            "4. Los archivos finales se guardarán en `data/procesada/` y `data/reportes/`.\n"
            "5. La consola de abajo mostrará el progreso y los posibles errores."
        )
        ttk.Label(instrucciones_frame, text=instrucciones_texto, justify=tk.LEFT).pack(fill=tk.X)

        # --- 2. Frame de Acciones ---
        acciones_frame = ttk.LabelFrame(main_frame, text="Acciones", padding="10")
        acciones_frame.pack(fill=tk.X, expand=False, pady=10)

        self.consolidar_btn = ttk.Button(acciones_frame,
                                         text="1. Consolidar Archivos",
                                         command=self.run_consolidar_task)
        self.consolidar_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        self.reportes_btn = ttk.Button(acciones_frame,
                                       text="2. Generar Reportes",
                                       command=self.run_reportes_task)
        self.reportes_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        self.limpiar_btn = ttk.Button(acciones_frame,
                                      text="Limpiar Consola",
                                      command=self.clear_log)
        self.limpiar_btn.pack(side=tk.RIGHT, fill=tk.X, expand=False, padx=5)

        # --- 3. Frame de Consola ---
        log_frame = ttk.LabelFrame(main_frame, text="Consola de Progreso", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.log_area = scrolledtext.ScrolledText(log_frame, state='disabled', height=20, wrap=tk.WORD, bg='black',
                                                  fg='white')
        self.log_area.pack(fill=tk.BOTH, expand=True)

        # --- 3. ¡SECCIÓN MODIFICADA! ---
        # Redirigir stdout/stderr (para capturar print() y errores)
        redirector = TextRedirector(self.log_area, self.root)
        sys.stdout = redirector
        sys.stderr = redirector

        # Configurar el logger de Python para que escriba en la GUI
        self.setup_gui_logging()

        print("Interfaz iniciada. Listo para comenzar.")
        print(f"Directorio de trabajo actual: {os.getcwd()}")
        print(f"Raíz del proyecto (PROJECT_ROOT): {paths.PROJECT_ROOT}")
        print(f"Buscando datos en: {paths.DATA_FOLDER}\n")

    def setup_gui_logging(self):
        """
        Reconfigura el logger raíz de Python para que envíe los
        mensajes a la consola de la GUI.
        """
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)  # Nivel mínimo a capturar

        # 1. Quitar el handler de consola (StreamHandler)
        #    que logger.py añadió en la importación.
        #    Ese handler está escribiendo en la terminal original (no visible).
        stream_handler_found = None
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                stream_handler_found = handler
                break

        if stream_handler_found:
            root_logger.removeHandler(stream_handler_found)
            print("Handler de consola (logger.py) removido.")

        # 2. Añadir nuestro nuevo handler de GUI (TkinterLogHandler)
        gui_handler = TkinterLogHandler(self.log_area, self.root)

        # 3. Definir un formato simple (sin colores ANSI) para la GUI
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                      datefmt='%H:%M:%S')
        gui_handler.setFormatter(formatter)

        # 4. Añadir el nuevo handler al logger raíz
        root_logger.addHandler(gui_handler)
        print("Handler de GUI (TkinterLogHandler) añadido.")

    def clear_log(self):
        """Limpia el área de la consola."""
        self.log_area.configure(state='normal')
        self.log_area.delete('1.0', tk.END)
        self.log_area.configure(state='disabled')
        print("Consola limpiada.\n")

    def start_task(self, task_function, button):
        """
        Inicia una función (task_function) en un hilo separado
        y deshabilita el botón (button) mientras corre.
        """
        button.config(state='disabled')
        # No limpiamos el log para poder ver el historial

        threading.Thread(target=self.task_wrapper,
                         args=(task_function, button),
                         daemon=True).start()

    def task_wrapper(self, task_function, button):
        """
        Envoltorio para las tareas que se ejecutan en hilos.
        Maneja el éxito/error y reactiva el botón.
        """
        # Usamos logging.info en lugar de print para la consistencia
        logging.info(f"--- Iniciando tarea: {task_function.__name__} ---")

        try:
            success = task_function()

            if success:
                logging.info(f"--- TAREA COMPLETADA CON ÉXITO ---")
            else:
                logging.error(f"--- TAREA FALLIDA (ver errores arriba) ---")

        except Exception as e:
            logging.error(f"--- ERROR INESPERADO EN LA TAREA ---")
            logging.error(f"Excepción: {e}")
            import traceback
            # traceback.print_exc() irá a sys.stderr, que está redirigido
            traceback.print_exc()
        finally:
            self.root.after(0, lambda: button.config(state='normal'))
            logging.info(f"--- Proceso finalizado. ---")

    def run_consolidar_task(self):
        """Llama a la tarea de consolidación."""
        self.start_task(file_merger.generate_consolidated_file, self.consolidar_btn)

    def run_reportes_task(self):
        """Llama a la tarea de generación de reportes."""
        self.start_task(program_report_generator.generate_tables_graphs, self.reportes_btn)


# --- Punto de entrada principal ---
if __name__ == "__main__":
    if 'file_merger' in locals() and 'program_report_generator' in locals():
        root = tk.Tk()
        app = App(root)
        root.mainloop()