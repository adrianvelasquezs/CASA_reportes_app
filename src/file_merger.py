# file_merger.py
#
# @author: Adrian Esteban Velasquez Solano
# @date: 10-2025
#
# In collaboration with CASA - Centro de Aseguramiento del Aprendizaje
# Universidad de los Andes
# Facultad de Administración
# Bogotá D.C., Colombia
#
# Description: This script merges multiple MS Excel files into a single consolidated file.
# There are two files: `base.xlsx` and `admitidos.xlsx` located in the `data` folder.
# The script reads both files, merges them based on the column corresponding to the student ID, and guarantees
# that all records from `base.xlsx` are included in the final consolidated file, as well as the matching
# start date records (Cohorte Real) and program list from `admitidos.xlsx`.

# ================================================ IMPORTS ============================================================

import pandas as pd
import os
import shutil
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
        # STUDENT_MAP_FILE is no longer needed
        # 'STUDENT_MAP_FILE': '../data/procesada/student_program_map.csv'
    })()

# ================================================ CONSTANTS ==========================================================

log = logger.Logger()


# ================================================ MAIN FUNCTION ======================================================

def generate_consolidated_file() -> bool:
    """
    Generate a consolidated Excel file by merging base and admitidos files.
    :return: True if the file was generated successfully, False otherwise.
    """
    try:
        # Load files
        base_df, admitidos_df = load_files()
        # Create processed folder if it doesn't exist
        create_processed_folder()
        # Create the student-program map for the report generator (REMOVED)
        # create_student_program_map(admitidos_df)

        # Merge DataFrames on the student ID column
        # This function now also adds the program map
        consolidated_df = merge_dataframes(base_df, admitidos_df)

        # Clean the consolidated DataFrame
        consolidated_df = clean_data(consolidated_df)
        # Save the consolidated DataFrame to an Excel file
        consolidated_df.to_excel(paths.CONSOLIDATED_FILE, index=False)
    except Exception as e:
        log.error(f'Error generating consolidated file: {e}')
        return False
    log.info('Consolidated file generated successfully.')
    return True


# =============================================== AUXILIARY FUNCTIONS =================================================

def load_files() -> tuple:
    """
    Load the base and admitidos Excel files into DataFrames.
    :return: A tuple containing the base DataFrame and the admitidos DataFrame.
    """
    base_df = pd.read_excel(paths.BASE_FILE)
    admitidos_df = pd.read_excel(paths.ADMITIDOS_FILE)
    log.info('Files loaded successfully.')
    return base_df, admitidos_df


def create_processed_folder() -> None:
    """
    Create the processed folder if it doesn't exist.
    :return: None
    """
    if os.path.exists(paths.PROCESSED_DIR):
        try:
            shutil.rmtree(paths.PROCESSED_DIR)
            log.info(f'Existing processed folder removed: {paths.PROCESSED_DIR}')
        except Exception as e:
            log.error(f'Failed to remove processed folder {paths.PROCESSED_DIR}: {e}')
            raise
    os.makedirs(paths.PROCESSED_DIR, exist_ok=True)
    log.info(f'Processed folder created at {paths.PROCESSED_DIR}')


def to_str_period(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return str(int(x))
        except Exception:
            return str(x)
    return str(x)


def last_digit_to_zero(s):
    if s is None:
        return None
    s = s.strip()
    return s[:-1] + '0' if len(s) >= 1 else '0'


def date_to_periodo(date_series: pd.Series) -> pd.Series:
    """
    Converts a Series of dates into the YYYYPP format.
    - Months 1-7 (Jan-Jul) -> 10
    - Months 8-12 (Aug-Dec) -> 20
    """
    try:
        # Convert to datetime, handling errors by setting to NaT
        dates = pd.to_datetime(date_series, errors='coerce')

        # Initialize period series
        periodo = pd.Series(index=dates.index, dtype='Int64')

        # Months 1-7 (Jan-Jul)
        mask_10 = (dates.dt.month >= 1) & (dates.dt.month <= 7)
        periodo[mask_10] = (dates[mask_10].dt.year * 100 + 10).astype('Int64')

        # Months 8-12 (Aug-Dec)
        mask_20 = (dates.dt.month >= 8) & (dates.dt.month <= 12)
        periodo[mask_20] = (dates[mask_20].dt.year * 100 + 20).astype('Int64')

        # Log NaNs
        if dates.isna().any() and not date_series.isna().all():
            log.warning("Some 'Fecha inicio de clases' dates were invalid and could not be converted to Periodo.")

        return periodo

    except Exception as e:
        log.error(f"Error in date_to_periodo conversion: {e}")
        return pd.Series(index=date_series.index, dtype='Int64')


def merge_dataframes(base_df: pd.DataFrame, admitidos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge two DataFrames on the student ID column.
    1. Gets largest PERIODO (Cohorte Real) per student from admitidos.
    2. Gets list of all programs per student from admitidos.
    3. Merges both into base_df.
    :param base_df: Base DataFrame.
    :param admitidos_df: Admitidos DataFrame.
    :return: Merged DataFrame.
    """

    # --- Ensure merge keys are consistent string types ---
    base_df['Código del estudiante'] = base_df['Código del estudiante'].astype(str).str.strip()
    admitidos_df['CODIGO'] = admitidos_df['CODIGO'].astype(str).str.strip()

    # --- 1. Get Cohorte Real (largest PERIODO) ---
    adm_cohorte = admitidos_df[['CODIGO', 'PERIODO']].copy()
    adm_cohorte['PERIODO'] = pd.to_numeric(adm_cohorte['PERIODO'], errors='coerce').astype('Int64')
    adm_cohorte = adm_cohorte.dropna(subset=['CODIGO', 'PERIODO'])
    adm_agg_cohorte = adm_cohorte.groupby('CODIGO', as_index=False)['PERIODO'].max()
    adm_agg_cohorte = adm_agg_cohorte.rename(columns={'PERIODO': 'Cohorte Real'})
    adm_agg_cohorte['Cohorte Real'] = adm_agg_cohorte['Cohorte Real'].astype('int64')

    # --- 2. Get Student-Program Map ---
    log.info('Creating student-program map...')
    program_mapping = {
        'E-AFIN': 'AFIN',
        'E-IMER': 'IMER',
        'M-MERC': 'MM',
        'M-FINZ': 'MF',
        'M-GAMB': 'MGA',
        'M-MGPD': 'MDP',
        'M-GSUM': 'MSCM',
        'M-MBAV': 'MBAV',
        'M-MBAE': 'EMBA',
        'M-MMBA': 'MBATP',
        'M-GEST': 'MGEST',
        'M-EMBA': 'EMBA',
        'M-MATP': 'MBATP'
    }

    student_map_df = admitidos_df[['CODIGO', 'PROGRAMA']].copy()
    student_map_df['programa_mapped'] = student_map_df['PROGRAMA'].map(program_mapping)

    # Log unmapped programs
    original_programs = set(student_map_df['PROGRAMA'].dropna().unique())
    unmapped_programs = {p for p in original_programs if p not in program_mapping}
    if unmapped_programs:
        log.warning(f"Unmapped programs found in `{paths.ADMITIDOS_FILE}`: {unmapped_programs}.")

    # Aggregate programs per student
    def aggregate_programs(subdf: pd.DataFrame) -> str:
        mapped = sorted({p for p in subdf['programa_mapped'].dropna().unique()})
        if mapped:
            return ';'.join(mapped)
        original = sorted({p for p in subdf['PROGRAMA'].dropna().unique()})
        return ';'.join(original) if original else None

    student_map_agg = student_map_df.groupby('CODIGO', as_index=False).apply(
        lambda g: pd.Series({'programas del estudiante': aggregate_programs(g)})
    )
    student_map_agg = student_map_agg.dropna(subset=['programas del estudiante'])

    # --- 3. Merge all data ---

    # Merge with base; left join preserves all base records
    df = base_df.merge(
        adm_agg_cohorte,
        left_on='Código del estudiante',
        right_on='CODIGO',
        how='left'
    ).drop(columns=['CODIGO'])

    # Merge the program map
    df = df.merge(
        student_map_agg,
        left_on='Código del estudiante',
        right_on='CODIGO',
        how='left'
    ).drop(columns=['CODIGO'])

    # Fill students not in admitidos.xlsx with "N/A"
    df['programas del estudiante'] = df['programas del estudiante'].fillna("N/A")

    log.info('Merging completed successfully with Cohorte Real and Program Map.')
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the DataFrame by removing duplicates and handling missing values.
    :param df: DataFrame to clean.
    :return: Cleaned DataFrame.
    """
    # Put all column names to lowercase and strip spaces
    df.columns = df.columns.str.lower()

    # Remove duplicates and rows with null values in critical columns
    df = df.drop_duplicates()
    df = df[df['cohorte real'].notnull() & df['puntaje criterio'].notnull()]

    # Remove codes from objetivo de aprendizaje and código y nombre del criterio
    df['periodo'] = df['semestre o ciclo'].apply(to_str_period).astype("int64")
    df = df.drop(columns=['semestre o ciclo'])
    df['objetivo de aprendizaje'] = remove_codes(df['objetivo de aprendizaje'])
    df['código y nombre del criterio'] = remove_codes(df['código y nombre del criterio'])

    # Rename columns for clarity
    df = df.rename(columns={'código y nombre del criterio': 'nombre del criterio'})
    log.info(f'Column "código y nombre del criterio" renamed to "nombre del criterio"')

    # Standardize competencia column values
    df['competencia'] = df['competencia'].apply(lambda x: x.strip().upper() if isinstance(x, str) else x)
    # Check validity of competencia column
    check_competencia_validity(df)

    # Remove redundant information from nombre del criterio
    df['nombre del criterio'] = remove_redundant_criteria(df['nombre del criterio'])
    log.info('Redundant information removed from "nombre del criterio" column.')

    log.info('Data cleaning completed successfully.')
    return df


def remove_codes(sr: pd.Series) -> pd.Series:
    """
    Remove codes from the beginning of the strings in the given Series.
    :param sr: Series to process.
    :return: Series with codes removed.
    """
    if sr.name == 'objetivo de aprendizaje':
        sr = sr.str.split(' ')
        # Remove the first token only when the first token contains a '-'
        sr = sr.apply(
            lambda x: ' '.join(x[1:]) if isinstance(x, list) and x and '-' in str(x[0]) else (
                ' '.join(x) if isinstance(x, list) else x)
        )
    elif sr.name == 'código y nombre del criterio':
        sr = sr.str.split('|')
        if len(sr) > 1:
            sr = sr.apply(lambda x: ' '.join(x[1:]) if isinstance(x, list) and len(x) > 1 else x[0])
    log.info(f'Codes removed from column: {sr.name}')
    return sr


def check_competencia_validity(df: pd.DataFrame) -> None:
    """
    Checks the validity of the 'competencia' column values and logs warnings.
    :param df: DataFrame to check (must have 'competencia' column).
    :return: None
    """
    # NOTE: Adjust this set with all valid 'Competencia' codes for your project.
    # I am using the codes seen in the 'base.xlsx' snippet ('CO', 'PC', 'TD')
    # and from the 'Diccionario' ('ET', 'CO-E', 'CO-O').
    valid_competencias = {'ET', 'CO-E', 'CO-O', 'PC', 'TD', 'CO', 'IT', 'LI', 'AI', 'TE', 'PG'}

    # Find unique values in the 'competencia' column that are not in the valid set
    actual_competencias = set(df['competencia'].dropna().astype(str).str.strip().str.upper().unique())
    invalid_competencias = actual_competencias - valid_competencias

    if invalid_competencias:
        log.warning(f"Found unexpected 'competencia' values: {invalid_competencias}")
    else:
        log.info("All 'competencia' values appear valid.")


def remove_redundant_criteria(sr: pd.Series) -> pd.Series:
    """
    Remove redundant information from the 'nombre del criterio' column.
    :param sr: Series to process.
    :return: Series with redundant information removed.
    """
    sr = sr.str.split('.')
    if len(sr) > 1:
        sr = sr.apply(lambda x: x[0].strip())
    return sr

# ================================================ ENTRY POINT ========================================================

if __name__ == '__main__':
    generate_consolidated_file()