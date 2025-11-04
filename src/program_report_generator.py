# program_report_generator.py
#
# @author: Adrian Esteban Velasquez Solano
# @date: 10-2025
#
# In collaboration with CASA - Centro de Aseguramiento del Aprendizaje
# Universidad de los Andes
# Facultad de Administración
# Bogotá D.C., Colombia
#
# Description: This script generates tables and graphs from a consolidated Excel file. It reads the consolidated
# file located in the `data` folder and produces various tables and visualizations to aid in data analysis.
# The generated tables and graphs are saved in the `reportes` folder, and are divided into programs.
# Each program will have its own set of tables and graphs.

# ================================================ IMPORTS ============================================================

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import logger

try:
    import path_config as paths
except ImportError:
    print("ERROR: No se pudo encontrar path_config.py")
    # Definir rutas de fallback por si acaso (aunque fallará)
    paths = type('obj', (object,), {
        'DATA_FOLDER': '../data/',
        'RAW_FOLDER': '../data/raw/',
        'BASE_FILE': '../data/raw/base.xlsx',
        'ADMITIDOS_FILE': '../data/raw/admitidos.xlsx',
        'PROCESSED_DIR': '../data/procesada/',
        'CONSOLIDATED_FILE': '../data/procesada/base_consolidada.xlsx',
        'STUDENT_MAP_FILE': '../data/procesada/student_program_map.csv'
    })()

# ================================================ CONSTANTS ==========================================================

log = logger.Logger()


# ================================================ MAIN GENERATOR =====================================================

def generate_tables_graphs() -> bool:
    """
    Generate a consolidated Excel file by merging base and admitidos files.
    :return: True if the file was generated successfully, False otherwise.
    """
    try:
        # Load consolidated file
        consolidated_df = load_file()

        # Load the student-program map ONCE
        try:
            map_path = paths.STUDENT_MAP_FILE
            student_map_df = pd.read_csv(map_path)
            log.info(f"Successfully loaded student-program map from {map_path}")
        except FileNotFoundError:
            log.error(f"FATAL: Student map file not found: {map_path}.")
            log.error("Please run 'file_merger.py' first to generate the map.")
            return False  # Stop execution if map is missing

        # Get unique programs
        programs = get_programs(consolidated_df)

        # Generate tables and graphs for each program
        for program in programs:
            # Create report folder for the program
            program_folder = create_report_folder(program)
            # Filter data for the program (convert to DataFrame for consistency)
            pdf = pd.DataFrame(consolidated_df[consolidated_df['programa'] == program])

            # TODO: Check valid students in the program using the loaded map
            # pdf = check_students(pdf, student_map_df)

            # If all students were filtered out, skip this program
            if pdf.empty:
                log.warning(f"No valid student data for program '{program}' after validation. Skipping.")
                continue

            # Generate tables and graphs
            generate_graphs(pdf, program_folder, program)
            generate_tables(pdf, program_folder, program)

        log.info('Tables and graphs generated successfully.')
        return True
    except FileNotFoundError as e:
        log.error(f'Error generating tables and graphs: {e}')
        return False
    except Exception as e:
        log.error(f'Unexpected error: {e}')
        return False


# ================================================ HELPERS ============================================================

def load_file() -> pd.DataFrame:
    """
    Load the consolidated Excel file into a DataFrame.
    :return: A DataFrame containing the consolidated data.
    """
    log.info('Loading consolidated file...')
    return pd.read_excel(paths.CONSOLIDATED_FILE)


def get_programs(df: pd.DataFrame) -> list:
    """
    Get the unique programs from the DataFrame.
    :param df: DataFrame containing the consolidated data.
    :return: A list of unique programs.
    """
    log.info('Getting unique programs...')
    prog_col = 'programa' if 'programa' in df.columns else 'Programa'
    return sorted(df[prog_col].dropna().unique().tolist())


def create_report_folder(program: str) -> str:
    """
    Create a report folder for the program.
    :param program: The program name.
    :return: The path to the program's report folder.
    """
    folder = os.path.join(paths.REPORTS_FOLDER, program)
    if not os.path.exists(folder):
        log.info(f'Creating report folder: {folder}')
        os.makedirs(folder, exist_ok=True)
    return folder


def check_students(pdf: pd.DataFrame, student_map: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the program DataFrame to include only students who are
    officially admitted to that program, using the pre-loaded student map.
    :param pdf: The DataFrame for a single program.
    :param student_map: The DataFrame from 'student_program_map.csv'.
    :return: A filtered DataFrame.
    """
    if pdf.empty:
        log.info("Student check skipped: DataFrame is empty.")
        return pdf

    try:
        # Get the target program from the already-filtered DataFrame
        target_program = pdf['programa'].iloc[0]

        # Get the list of all student codes *officially* in this program
        # Ensure 'código del estudiante' is string for comparison
        student_map_clean = student_map.copy()
        student_map_clean['código del estudiante'] = student_map_clean['código del estudiante'].astype(str)

        valid_students = student_map_clean[
            student_map_clean['programa'] == target_program
            ]['código del estudiante']

        # Ensure the DataFrame's student code is also a string for comparison
        pdf_clean = pdf.copy()
        pdf_clean['código del estudiante'] = pdf_clean['código del estudiante'].astype(str)

        # Filter the DataFrame to only include students in the valid list
        original_count = len(pdf_clean)
        valid_pdf = pdf_clean[
            pdf_clean['código del estudiante'].isin(valid_students)
        ]
        final_count = len(valid_pdf)

        if original_count != final_count:
            dropped_count = original_count - final_count
            log.warning(f"Program '{target_program}': "
                        f"Removed {dropped_count} records for students "
                        f"not found in the program map.")
        else:
            log.info(f"Program '{target_program}': "
                     f"All {original_count} records validated against program map.")

        return valid_pdf

    except Exception as e:
        log.error(f"Error during student validation for program '{target_program}': {e}")
        return pdf  # Return original df on unexpected error


# ================================================ TABLE GENERATION ===================================================

def generate_tables(pdf: pd.DataFrame, folder_path: str, program: str):
    """
    Generate all tables for a given program.
    :param pdf: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    df = pdf.drop(columns=['programa']).copy()
    log.info(f'Generating tables for program: {program}')
    table_1(df, folder_path, program)
    table_2(df, folder_path, program)
    table_3(df, folder_path, program)
    table_4(df, folder_path, program)
    table_5(df, folder_path, program)
    table_6(df, folder_path, program)
    table_7(df, folder_path, program)
    table_8(df, folder_path, program)
    table_9(df, folder_path, program)


def table_1(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 1: Descripción de competencias, metas y objetivos de aprendizaje.
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        comp_col = next((c for c in cols if 'competencia' in c.lower()), None)
        meta_col = next((c for c in cols if 'meta de aprendizaje' in c.lower()), None)
        obj_col = next((c for c in cols if 'objetivo de aprendizaje' in c.lower()), None)
        if comp_col is None or meta_col is None or obj_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_1.xlsx')
            with pd.ExcelWriter(out_path) as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(f'Table 1 fallback written (column not found) for program: {program}')
            return

        tmp = df[[comp_col, meta_col, obj_col]].dropna().drop_duplicates()
        metas = (tmp.groupby(comp_col)[meta_col]
                 .apply(lambda s: "".join(sorted(map(str, pd.unique(s)))))
                 .rename('Metas de aprendizaje'))
        objetivos = (tmp.groupby(comp_col)[obj_col]
                     .apply(lambda s: "".join(sorted(map(str, pd.unique(s)))))
                     .rename('Objetivos de aprendizaje'))
        out = pd.concat([metas, objetivos], axis=1).reset_index()
        out.columns = ['Competencia', 'Metas de aprendizaje', 'Objetivos de aprendizaje']

        out_path = os.path.join(folder_path, f'{program}_tabla_1.xlsx')
        with pd.ExcelWriter(out_path) as xw:
            out.to_excel(xw, index=False, sheet_name='Tabla 1')
        log.info(f'Table 1 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 1: {e}')


def table_2(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 2: Cantidad de mediciones por Objetivo de aprendizaje y Periodo.
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        per_col = next((c for c in cols if c.strip().lower().startswith(
            'semestre') or 'semestre o ciclo' in c.lower() or c.lower().startswith('periodo')), None)
        obj_col = next((c for c in cols if 'objetivo de aprendizaje' in c.lower()), None)
        if per_col is None or obj_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_2.xlsx')
            with pd.ExcelWriter(out_path) as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(f'Table 2 fallback written (column not found) for program: {program}')
            return
        pv = (df.groupby([per_col, obj_col]).size().unstack(fill_value=0).sort_index())
        pv['# Total'] = pv.sum(axis=1)
        total_row = pv.sum(axis=0).to_frame().T
        total_row.index = ['Total']
        out_df = pd.concat([pv, total_row], axis=0)
        formatted = out_df.astype(object)
        for c in formatted.columns:
            formatted[c] = formatted[c].apply(lambda x: '—' if (pd.notnull(x) and x == 0) else x)
        out_path = os.path.join(folder_path, f'{program}_tabla_2.xlsx')
        with pd.ExcelWriter(out_path) as xw:
            formatted.to_excel(xw, sheet_name='Tabla 2')
        log.info(f'Table 2 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 2: {e}')


def table_3(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 3: Descripción de criterios por Objetivo de aprendizaje.
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = list(df.columns)
        obj_candidates = [c for c in cols if 'objetivo de aprendizaje' in c.lower()]
        crit_candidates = [c for c in cols if ('código y nombre del criterio' in c.lower()) or (
                'codigo y nombre del criterio' in c.lower()) or ('nombre del criterio' in c.lower()) or (
                                   'criterio' in c.lower())]
        obj_col = obj_candidates[0] if obj_candidates else None
        criterio_col = None
        for c in crit_candidates:
            if 'código y nombre del criterio' in c.lower() or 'codigo y nombre del criterio' in c.lower():
                criterio_col = c;
                break
        if criterio_col is None and crit_candidates:
            criterio_col = crit_candidates[0]

        if obj_col is None or criterio_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_3.xlsx')
            with pd.ExcelWriter(out_path) as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(
                f'Table 3 fallback written (column not found) for program: {program} | obj_col={obj_col} criterio_col={criterio_col}')
            return

        # Mantener combinaciones únicas objetivo-criterio (para no duplicar criterios)
        base = (df[[obj_col, criterio_col]]
                .dropna()
                .drop_duplicates()
                .astype({obj_col: str, criterio_col: str}))

        # Conteo de criterios por objetivo
        counts = base.groupby(obj_col)[criterio_col].nunique().rename('__n')

        # Expandir a "un criterio por fila"
        long_df = base.copy()
        long_df['__n'] = long_df[obj_col].map(counts)

        # Orden opcional por objetivo y nombre de criterio
        long_df = long_df.sort_values([obj_col, criterio_col]).reset_index(drop=True)

        out_df = long_df.rename(columns={obj_col: 'Objetivos de aprendizaje', criterio_col: 'Nombre del criterio',
                                         '__n': 'Número de criterios'})[
            ['Objetivos de aprendizaje', 'Número de criterios', 'Nombre del criterio']
        ]

        # Dejar valores solo en la primera fila de cada objetivo; poner nulos en las siguientes
        dup_mask = out_df['Objetivos de aprendizaje'].duplicated(keep='first')
        out_df.loc[dup_mask, ['Objetivos de aprendizaje', 'Número de criterios']] = pd.NA

        # Fila total (suma de únicos por objetivo)
        total_criterios = int(counts.sum())
        total_row = pd.DataFrame([
            {'Objetivos de aprendizaje': 'Total criterios', 'Número de criterios': total_criterios,
             'Nombre del criterio': ''}
        ])
        out_df = pd.concat([out_df, total_row], ignore_index=True)

        out_path = os.path.join(folder_path, f'{program}_tabla_3.xlsx')
        with pd.ExcelWriter(out_path) as xw:
            out_df.to_excel(xw, index=False, sheet_name='Tabla 3')
        log.info(f'Table 3 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 3: {e}')


def table_4(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 4: Promedio por Competencia y Periodo.
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        per_col = next((c for c in cols if c.strip().lower().startswith(
            'semestre') or 'semestre o ciclo' in c.lower() or c.lower().startswith('periodo')), None)
        comp_col = next((c for c in cols if 'competencia' in c.lower()), None)
        score_col = next((c for c in cols if 'puntaje criterio' in c.lower()), None)
        if per_col is None or comp_col is None or score_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_4.xlsx')
            with pd.ExcelWriter(out_path, engine='xlsxwriter') as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(f'Table 4 fallback written (column not found) for program: {program}')
            return
        pv = (df.groupby([per_col, comp_col])[score_col]
              .mean().unstack())
        # Redondeo 2 decimales
        pv = pv.round(2)
        out_path = os.path.join(folder_path, f'{program}_tabla_4.xlsx')
        pv.to_excel(out_path)
        log.info(f'Table 4 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 4: {e}')


def table_5(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 5: Promedio por Criterio dentro de Objetivo y Periodo.
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        per_col = next((c for c in cols if c.strip().lower().startswith(
            'semestre') or 'semestre o ciclo' in c.lower() or c.lower().startswith('periodo')), None)
        obj_col = next((c for c in cols if 'objetivo de aprendizaje' in c.lower()), None)
        crit_col = next(
            (c for c in cols if 'código y nombre del criterio' in c.lower() or 'nombre del criterio' in c.lower()),
            None)
        score_col = next((c for c in cols if 'puntaje criterio' in c.lower()), None)
        if per_col is None or obj_col is None or crit_col is None or score_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_5.xlsx')
            with pd.ExcelWriter(out_path) as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(f'Table 5 fallback written (column not found) for program: {program}')
            return
        tmp = df[[per_col, obj_col, crit_col, score_col]].dropna()
        pv = (tmp.groupby([obj_col, crit_col, per_col])[score_col]
              .mean().round(2).unstack(fill_value=np.nan))
        out_path = os.path.join(folder_path, f'{program}_tabla_5.xlsx')
        styled = pv.style.format(precision=2).background_gradient(cmap='RdYlGn', axis=None)
        styled.to_excel(out_path, sheet_name='Tabla 5')
        log.info(f'Table 5 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 5: {e}')


def table_6(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 6: Promedio por PERIODO (diagnóstico escritura).
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        # detectar periodo/cohorte
        per_col = next(
            (c for c in cols if
             c.strip().upper() == 'PERIODO' or 'cohorte' in c.lower() or c.lower().startswith('periodo')),
            None
        )
        if per_col is None:
            per_col = next(
                (c for c in cols if 'semestre o ciclo' in c.lower() or c.strip().lower().startswith('semestre')), None)

        # detectar PROMEDIO o usar Puntaje criterio
        prom_col = next((c for c in cols if c.strip().upper() == 'PROMEDIO' or 'promedio escritura' in c.lower()), None)
        score_col = next((c for c in cols if 'puntaje criterio' in c.lower()), None)

        if per_col is None or (prom_col is None and score_col is None):
            out_path = os.path.join(folder_path, f'{program}_tabla_6.xlsx')
            with pd.ExcelWriter(out_path) as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Fallback')
            log.warning(f'Table 6 fallback written (no period/score columns) for program: {program}')
            return

        # Si no hay PROMEDIO, calculamos desde Puntaje criterio
        value_col = prom_col if prom_col is not None else score_col

        # (Opcional) si quieres que sea solo de escritura, descomenta y ajusta palabras clave:
        # mask = df.columns.str.contains('objetivo|criterio|competencia', case=False).any()
        # if mask:
        #     filtro = df.apply(lambda r: any('escrit' in str(v).lower() for v in r[['Objetivo de aprendizaje','Código y nombre del criterio','Competencia'] if x in df.columns]), axis=1)
        #     tmp = df.loc[filtro, [per_col, value_col]].dropna()
        # else:
        #     tmp = df[[per_col, value_col]].dropna()

        tmp = df[[per_col, value_col]].dropna()
        tabla = tmp.groupby(per_col)[value_col].mean().round(2).reset_index()
        tabla.columns = ['Periodo', 'Promedio']

        out_path = os.path.join(folder_path, f'{program}_tabla_6.xlsx')
        with pd.ExcelWriter(out_path) as xw:
            tabla.to_excel(xw, index=False, sheet_name='Tabla 6')
        log.info(f'Table 6 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 6: {e}')


def table_7(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 7: Promedio por Criterios de Evaluación por Periodos Académicos (heatmap con Styler).
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        per_col = next((c for c in cols if c.strip().lower().startswith(
            'semestre') or 'semestre o ciclo' in c.lower() or c.lower().startswith('periodo')), None)
        obj_col = next((c for c in cols if 'objetivo de aprendizaje' in c.lower()), None)
        crit_col = next(
            (c for c in cols if 'código y nombre del criterio' in c.lower() or 'nombre del criterio' in c.lower()),
            None)
        score_col = next((c for c in cols if 'puntaje criterio' in c.lower()), None)
        if per_col is None or obj_col is None or crit_col is None or score_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_7.xlsx')
            with pd.ExcelWriter(out_path) as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(f'Table 7 fallback written (column not found) for program: {program}')
            return
        tmp = df[[obj_col, crit_col, per_col, score_col]].dropna()
        pv = pd.pivot_table(tmp, index=[obj_col, crit_col], columns=per_col, values=score_col, aggfunc='mean').round(2)
        out_path = os.path.join(folder_path, f'{program}_tabla_7.xlsx')
        styled = pv.style.format(precision=2).background_gradient(cmap='RdYlGn', axis=None)
        styled.to_excel(out_path, sheet_name='Tabla 7')
        log.info(f'Table 7 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 7: {e}')


def table_8(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 8: Resultados (Promedio) por Competencia, por Cohortes (PERIODO) con columna 'Promedio'.
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        coh_col = next((c for c in cols if c.strip().upper() == 'PERIODO' or 'cohorte' in c.lower()), None)
        comp_col = next((c for c in cols if 'competencia' in c.lower()), None)
        score_col = next((c for c in cols if 'puntaje criterio' in c.lower()), None)
        if coh_col is None or comp_col is None or score_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_8.xlsx')
            with pd.ExcelWriter(out_path, engine='xlsxwriter') as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(f'Table 8 fallback written (column not found) for program: {program}')
            return
        pv = (df.groupby([coh_col, comp_col])[score_col].mean().unstack())
        pv['Promedio'] = pv.mean(axis=1)
        pv = pv.round(2)
        # Agregar fila de promedio general
        mean_row = pv.mean(axis=0).to_frame().T
        mean_row.index = ['Promedio']
        out_df = pd.concat([pv, mean_row])
        out_df.index = [f'Cohorte {idx}' if idx != 'Promedio' else idx for idx in out_df.index]
        out_path = os.path.join(folder_path, f'{program}_tabla_8.xlsx')
        out_df.to_excel(out_path)
        log.info(f'Table 8 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 8: {e}')


def table_9(df: pd.DataFrame, folder_path: str, program: str):
    """
    Tabla 9: Resultados (Promedio μ y Desv. σ) por Objetivo de aprendizaje, por Cohortes.
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the tables.
    :param program: The program name.
    :return: None
    """
    try:
        cols = df.columns
        # Buscar una columna de cohorte/periodo de ingreso razonable
        coh_col = next(
            (c for c in cols if c.strip().upper() in {'PERIODO', 'COHORTE'}
             or 'cohorte' in c.lower()
             or c.lower().startswith('periodo')),
            None
        )
        if coh_col is None:
            # usa el periodo de aplicación si no hay cohorte de ingreso
            coh_col = next(
                (c for c in cols if 'semestre o ciclo' in c.lower() or c.strip().lower().startswith('semestre')), None)

        obj_col = next((c for c in cols if 'objetivo de aprendizaje' in c.lower()), None)
        score_col = next((c for c in cols if 'puntaje criterio' in c.lower()), None)

        if coh_col is None or obj_col is None or score_col is None:
            out_path = os.path.join(folder_path, f'{program}_tabla_9.xlsx')
            with pd.ExcelWriter(out_path) as xw:
                df.head(50).to_excel(xw, index=False, sheet_name='Datos')
            log.warning(f'Table 9 fallback written (column not found) for program: {program}')
            return

        tmp = df[[coh_col, obj_col, score_col]].dropna()
        mean_pv = tmp.pivot_table(index=coh_col, columns=obj_col, values=score_col, aggfunc='mean')
        std_pv = tmp.pivot_table(index=coh_col, columns=obj_col, values=score_col, aggfunc='std')

        # Intercalar columnas μ y σ
        cols_order = []
        for obj in mean_pv.columns:
            cols_order.extend([(obj, 'μ'), (obj, 'σ')])

        out_cols = pd.MultiIndex.from_tuples(cols_order)
        out_df = pd.DataFrame(index=mean_pv.index, columns=out_cols, dtype=float)
        for obj in mean_pv.columns:
            out_df[(obj, 'μ')] = mean_pv[obj]
            out_df[(obj, 'σ')] = std_pv[obj]

        out_df = out_df.sort_index().round(2)
        avg = out_df.mean(axis=0).to_frame().T
        avg.index = ['Promedio']
        final_df = pd.concat([out_df, avg])

        out_path = os.path.join(folder_path, f'{program}_tabla_9.xlsx')
        with pd.ExcelWriter(out_path) as xw:
            final_df.to_excel(xw, sheet_name='Tabla 9')
        log.info(f'Table 9 generated for program: {program}')
    except Exception as e:
        log.error(f'Error in Table 9: {e}')


# ================================================ GRAPH GENERATION ==================================================

def generate_graphs(pdf: pd.DataFrame, folder_path: str, program: str):
    df = pdf.drop(columns=['programa']).copy()
    graph_1(df, folder_path, program)
    graph_2(df, folder_path, program)


def graph_1(df: pd.DataFrame, folder_path: str, program: str):
    """
    Generate Graph 1: Number of evaluations per period (unique students).
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the graphs.
    :param program: The program name.
    :return: None
    """
    # localizar columnas
    cols = df.columns
    per_col = next((c for c in cols if
                    c.strip().lower().startswith('semestre') or 'semestre o ciclo' in c.lower() or c.lower().startswith(
                        'periodo')), None)
    student_col = next((c for c in cols if 'código del estudiante' in c.lower() or c.lower() == 'codigo'), None)
    if per_col is None or student_col is None:
        # Fallback simple
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'No hay columnas de periodo/estudiante', ha='center', va='center')
        ax.axis('off')
    else:
        tmp = df[[per_col, student_col]].dropna().drop_duplicates()
        counts = tmp.groupby(per_col)[student_col].nunique().sort_index()
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.bar(range(len(counts)), counts.values)
        ax.set_xticks(range(len(counts)))
        ax.set_xticklabels(counts.index.astype(str))
        ax.set_title('Número de evaluaciones AOL MM')
        ax.set_xlabel('Periodo - semestre')
        ax.set_ylabel('Número de estudiantes evaluados en AOL MM')
        # Etiquetas encima
        for rect, val in zip(bars, counts.values):
            ax.text(rect.get_x() + rect.get_width() / 2, rect.get_height() + 0.5, f"{int(val)}", ha='center',
                    va='bottom')
        ax.margins(y=0.1)
        fig.tight_layout()
    out_path = os.path.join(folder_path, f'{program}_figura_1.png')
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    log.info(f'Graph 1 generated for program: {program}')


def graph_2(df: pd.DataFrame, folder_path: str, program: str):
    """
    Generate Graph 2: Number of evaluations by cohort of entry (PERIODO).
    :param df: DataFrame filtered by program.
    :param folder_path: Path to save the graphs.
    :param program: The program name.
    :return: None
    """
    cols = df.columns
    coh_col = next((c for c in cols if c.strip().upper() == 'PERIODO' or 'cohorte' in c.lower()), None)
    student_col = next((c for c in cols if 'código del estudiante' in c.lower() or c.lower() == 'codigo'), None)
    fig, ax = plt.subplots(figsize=(10, 6))
    if coh_col and student_col:
        tmp = df[[coh_col, student_col]].dropna().drop_duplicates()
        counts = tmp.groupby(coh_col)[student_col].nunique().sort_index()
        bars = ax.barh(range(len(counts)), counts.values)
        ax.set_yticks(range(len(counts)))
        ax.set_yticklabels([f'Cohorte {c}' for c in counts.index])
        ax.invert_yaxis()
        ax.set_xlabel('Número de estudiantes evaluados AOL MM')
        ax.set_title('Estudiantes evaluados en AOL desagregado por cohorte de ingreso')
        for rect, val in zip(bars, counts.values):
            ax.text(rect.get_width() + 0.5, rect.get_y() + rect.get_height() / 2, f"{int(val)}", va='center')
        fig.tight_layout()
    else:
        ax.text(0.5, 0.5, 'No hay columnas PERIODO/Código estudiante', ha='center', va='center')
        ax.axis('off')
    out_path = os.path.join(folder_path, f'{program}_figura_2.png')
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    log.info(f'Graph 2 generated for program: {program}')


# ================================================ ENTRY POINT ========================================================

if __name__ == '__main__':
    generate_tables_graphs()
