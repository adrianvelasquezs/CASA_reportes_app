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
# start date records from `admitidos.xlsx`.

# ================================================ IMPORTS ============================================================

import pandas as pd
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
        # Create the student-program map for the report generator
        create_student_program_map(admitidos_df)
        # Merge DataFrames on the student ID column
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
    os.makedirs(paths.PROCESSED_DIR, exist_ok=True)
    log.info('Processed folder created.')


def merge_dataframes(base_df: pd.DataFrame, admitidos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge two DataFrames on the student ID column.
    :param base_df: Base DataFrame.
    :param admitidos_df: Admitidos DataFrame.
    :return: Merged DataFrame.
    """
    adm = admitidos_df[['CODIGO', 'PERIODO']].copy()

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

    adm['PERIODO'] = adm['PERIODO'].apply(to_str_period).apply(last_digit_to_zero).astype("int64")

    df = base_df.merge(
        adm,
        left_on='Código del estudiante',
        right_on='CODIGO',
        how='left'
    ).rename(columns={'PERIODO': 'Cohorte Real'}).drop(columns=['CODIGO'])

    log.info('Merging completed successfully.')
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
    df['objetivo de aprendizaje'] = remove_codes(df['objetivo de aprendizaje'])
    df['código y nombre del criterio'] = remove_codes(df['código y nombre del criterio'])

    # Rename columns for clarity
    df = df.rename(columns={'código y nombre del criterio': 'nombre del criterio'})
    log.info(f'Column "código y nombre del criterio" renamed to "nombre del criterio"')

    # Standardize competencia column values
    df['competencia'] = df['competencia'].apply(lambda x: x.strip().upper() if isinstance(x, str) else x)
    # Check validity of competencia column
    check_competencia_validity(df)

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
        if len(sr) > 0:
            sr = sr.apply(lambda x: ' '.join(x[1:]) if isinstance(x, list) else x)
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


def create_student_program_map(admitidos_df: pd.DataFrame) -> None:
    """
    Creates and saves a simple map of student codes to their admitted program.
    It also standardizes the program names based on the provided mapping.
    :param admitidos_df: The DataFrame loaded from 'admitidos.xlsx'.
    :return: None
    """
    try:
        log.info('Creating student-program map...')

        # Define the program mapping based on the provided table
        # 'Código de programas según Banner' -> 'Otras siglas utilizadas'
        program_mapping = {
            'E-AFIN': 'AFIN',
            'E-IMER': 'IMER',
            'M-MERC': 'MMER',  # Using MMER as it appears in base.xlsx
            'M-FINZ': 'MFIN',  # Using MFIN as it is more specific
            'M-GAMB': 'MGA',
            'M-MGPD': 'MDP',
            'M-GSUM': 'MSCM',
            'M-MBAV': 'MBAOnline',
            'M-MBAE': 'EMBA',
            'M-MMBA': 'MBATP',  # This matches 'MBATP' in base.xlsx
            'M-GEST': 'MGEST'
        }

        # Select, rename, and clean columns
        student_map_df = admitidos_df[['CODIGO', 'PROGRAMA']].copy()
        student_map_df.columns = ['código del estudiante', 'programa']

        # Apply the mapping
        original_programs = set(student_map_df['programa'].unique())
        student_map_df['programa'] = student_map_df['programa'].map(program_mapping)

        # Log any programs that were not in the mapping
        unmapped_programs = {
            p for p in original_programs
            if p not in program_mapping and pd.notna(p)
        }
        if unmapped_programs:
            log.warning(f"Unmapped programs found in '{paths.ADMITIDOS_FILE}': "
                        f"{unmapped_programs}. These will be 'NaN' in the map.")

        # Ensure student codes are strings to match 'base.xlsx'
        student_map_df['código del estudiante'] = student_map_df['código del estudiante'].astype(str)

        # Remove any duplicates to create a clean 1-to-1 map (student-to-program)
        student_map_df = student_map_df.drop_duplicates(subset=['código del estudiante'])

        # Drop any rows where the program could not be mapped (became NaN)
        student_map_df = student_map_df.dropna(subset=['programa'])

        # Define the output path
        output_path = paths.STUDENT_MAP_FILE

        # Save the map to the processed folder
        student_map_df.to_csv(output_path, index=False)
        log.info(f'Student-program map saved to {output_path}')

    except Exception as e:
        log.error(f'Error creating student-program map: {e}')


# ================================================ ENTRY POINT ========================================================

if __name__ == '__main__':
    generate_consolidated_file()
