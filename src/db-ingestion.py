#!/usr/bin/env python
# coding: utf-8

import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from mysql.connector import connect, Error, IntegrityError
from pathlib import Path



def create_connection(db_config: dict):
    """
    Conexion a la base de datos

    params
    ------
      * db_config: dict
        - credenciales de la base de datos

    returns:
        - conexion a la db
    """
    conn = None
    try:
        conn = connect(**db_config)   # < --
        if conn.is_connected():
            print("Conectadisimo")
    except Error as e:
        print(f"Error: {e}")
    return conn

def crear_cursor(conn):
    """
    Crear el cursor en la base de datos

    params
    ------
      * conn:
        - conexion de la base de datos

    returns: 
        - cursor de la base de datos
    """
    try:
        cursor = conn.cursor(buffered=True)
    except Error as e:
        print(f"Error: {e}")
    return cursor

def insert_or_ignore(conn, cursor, query: str, data: tuple) -> None:
    """
    Funcion para insertar los datos, evitar duplicados
    y corregir tipo de dato para el numero de cuentas

    params
    ------
      * conn: - conexion con la base de datos
      * cursor: - cursor de la base de datos
      * query: str
        - Sentencia INSERT de SQL para la db
      * data: tuple
        - valor que se va a insertar, tupla (valor, ) de ser de un solo campo 

    returns:
        - None
    """
    # Importante que el `dato` sea una tupla en forma de `(value,)`
    # si es que los datos vienen de una sola columna
    try:
        data = tuple(int(x) if isinstance(x, (np.integer)) else x for x in data) # para Nro_Cuenta
        cursor.execute(query, data)
        conn.commit()
    except IntegrityError as e:
        print(f"Error: {e}")

def get_all_tables(cursor) -> list:
    """
    Agarra los nombres de todas las tablas
    de la base de datos

    params
    ------
      * cursor:
        - cursor de la base de datos

    returns:
        - lista de nombres de tablas
    """
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    return [table[0] for table in tables]

def ingest_table(conn, cursor, df: pd.DataFrame, table: str, col_name: str) -> None:
    """
    Inserta los datos a la base de datos

    params
    ------
      * conn:
        - conexion a la db
      * cursor:
        - cursor de la db
      * df: pd.DataFrame
        - datos que van a ser insertados
      * table: str
        - tabla que seleccionar
      * col_name: str
        - columna a seleccionar
    
    returns:
        - None
    """
    count = 0
    unique_table = df[col_name].unique()
    for value in unique_table:
        query = f"""
        INSERT INTO `{table}` (`{col_name}`) VALUES (%s)
        ON DUPLICATE KEY UPDATE `{col_name}`=VALUES(`{col_name}`);
        """
        insert_or_ignore(conn, cursor, query, (value,)) ## placeholder (%s)
        count += 1
    print(f"Cantidad de filas insertadas en {table}: {count}")

# funciona para mapear las foreign keys con la columna pertinente
def fetch_fk(cursor, id: str, col: str, table: str) -> dict:
    """
    Agarra las foreign keys y valor de una tabla 
    para despues ser referenciadas

    params
    ------
      * cursor:
        - cursor de la db
      * id: str
        - id de la tabla
      * col: str
        - columna de la tabla
      * table: str
        - nombre de la tabla

    returns:
        - diccionario en forma {col: id}
    """
    try:
        cursor.execute(f"SELECT `{id}`, `{col}` FROM `{table}`")
    except Exception as e:
        print(f"Error: {e}")

    mapping = {col: id for id, col in cursor.fetchall()}
    print(f"Fecth fkeys from {table}")
    return mapping


def ingest_proveedores(conn, cursor, df_proveedores: pd.DataFrame, mappings_fk_proveedor: dict) -> None:
    """
    Ingresa los datos a la tabla proveedores y las foreign keys de las tabla relacionadas

    params
    ------
      * conn:
        - conexion a la db
      * cursor:
        - cursor de la db
      * df_proveedores: pd.DataFrame
        - dataframe con los datos de los proveedores 
      * mappings_fk_proveedor: dict
        - diccionario con las foreign keys

    returns:
        - None
    """
    count = 0
    # Step 6: Insert unique data into proveedores
    categoria_mapping, contribuyente_mapping, razon_mapping, ciudades_mapping = mappings_fk_proveedor
    for _, row in df_proveedores.iterrows():
        # Agarramos la foreign key que mapee con el valor de la fila en cuestion
        id_categoria = categoria_mapping.get(row['categoria'])
        id_contribuyente = contribuyente_mapping.get(row['contribuyente'])
        id_razon = razon_mapping.get(row['razon'])
        id_ciudad = ciudades_mapping.get(row['ciudad'])
    
        query = """
        INSERT INTO proveedores (numero, nombre, cuit, contacto, mail, telefono, id_categoria, id_contribuyente, id_razon, id_ciudad)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            nombre = VALUES(nombre), 
            cuit = VALUES(cuit), 
            contacto = VALUES(contacto), 
            mail = VALUES(mail), 
            telefono = VALUES(telefono), 
            id_categoria = VALUES(id_categoria), 
            id_contribuyente = VALUES(id_contribuyente), 
            id_razon = VALUES(id_razon), 
            id_ciudad = VALUES(id_ciudad);
        """
        data = (
            row['numero'], row['nombre'], row['cuit'], row['contacto'], 
            row['mail'], row['telefono'], id_categoria, 
            id_contribuyente, id_razon, id_ciudad
        )
    
        # Insert into proveedores table
        insert_or_ignore(conn, cursor, query, data)
        count += 1
        #insert_or_ignore(query, (ciudad,))
    print(f"Cantidad de filas insertadas: {count}")


def ingest_gastos(conn, cursor, df: pd.DataFrame, mappings_fk_gastos) -> None:
    """
    Ingresa los datos a la tabla gastos y las foreign keys de la tabla relacionadas (proveedores, cuentas)

    params
    ------
      * conn:
        - conexion a la db
      * cursor:
        - cursor de la db
      * df: pd.DataFrame
        - dataframe de donde se sacan los gastos 
      * mappings_fk_gastos: tuple
        - tupla con las foreign keys

    returns:
        - None
    """
    count = 0
    # Step 8: Insert unique data into gastos --> todo el df ya que son todas las transacciones
    proveedor_mapping, cuenta_mapping = mappings_fk_gastos
    for _, row in df.iterrows():  # This should iterate through the full dataset
        # Retrieve foreign key ids from pre-fetched mappings
        id_proveedor = proveedor_mapping.get(row['numero'])
        id_cuenta = cuenta_mapping.get(row['nro_cuenta'])
    
        # Only insert if foreign keys are found
        if id_proveedor is not None and id_cuenta is not None:
            query = """
            INSERT INTO gastos (importe, fecha, id_proveedor, id_cuenta)
            VALUES (%s, %s, %s, %s);
            """
            data = (row['importe'], row['fecha'], id_proveedor, id_cuenta)
            insert_or_ignore(conn, cursor, query, data)
            count += 1
        else:
            print(f"Foreign key not found for {row['numero']} or {row['nro_cuenta']}")
    print(f"Cantidad de filas insertadas: {count}")


def ingest_donantes(conn, cursor, df_donantes: pd.DataFrame, mappings_fk_donantes) -> None:
    """
    Ingresa los datos a la tabla donantes y las foreign keys de las tabla relacionadas

    params
    ------
      * conn:
        - conexion a la db
      * cursor:
        - cursor de la db
      * df_donantes: pd.DataFrame
        - dataframe con los datos de los donantes 
      * mappings_fk_donantes: tuple
        - tupla con las foreign keys

    returns:
        - None
    """
    # to test
    df_donantes = df_donantes.reset_index(drop=True)
    count = 0
    # Step 6: Insert unique data into proveedores
    frecuencia_mapping, contribuyente_mapping, razon_mapping, tipo_mapping, pais_mapping = mappings_fk_donantes 
    for _, row in df_donantes.iterrows():
        # Agarramos la foreign key que mapee con el valor de la fila en cuestion
        id_frecuencia = frecuencia_mapping.get(row['frecuencia'])
        id_contribuyente = contribuyente_mapping.get(row['contribuyente'])
        id_razon = razon_mapping.get(row['razon'])
        id_tipo = tipo_mapping.get(row['tipo'])
        id_pais = pais_mapping.get(row['pais'])
    
        query = """
        INSERT INTO donantes (numero, nombre, cuit, contacto, mail, telefono, activo, id_frecuencia, id_contribuyente, id_razon, id_tipo, id_pais)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            nombre = VALUES(nombre), 
            cuit = VALUES(cuit), 
            contacto = VALUES(contacto), 
            mail = VALUES(mail), 
            activo = VALUES(activo), 
            telefono = VALUES(telefono), 
            id_frecuencia = VALUES(id_frecuencia), 
            id_contribuyente = VALUES(id_contribuyente), 
            id_razon = VALUES(id_razon), 
            id_tipo = VALUES(id_tipo), 
            id_pais = VALUES(id_pais);
        """
        data = (
            row['numero'], row['nombre'], row['cuit'], row['contacto'], 
            row['mail'], row['telefono'], row['activo'], id_frecuencia, 
            id_contribuyente, id_razon, id_tipo, id_pais
        )
    
        # Insert into proveedores table
        insert_or_ignore(conn, cursor, query, data)
        count += 1
    print(f"Cantidad de filas insertadas: {count}")
        

def ingest_ingresos(conn, cursor, df: pd.DataFrame, mappings_fk_ingresos) -> None:
    """
    Ingresa los datos a la tabla ingresos y las foreign keys de la tabla relacionadas (donantes, cuentas)

    params
    ------
      * conn:
        - conexion a la db
      * cursor:
        - cursor de la db
      * df: pd.DataFrame
        - dataframe de donde se sacan los ingresos 
      * mappings_fk_ingresos: tuple
        - tupla con las foreign keys

    returns:
        - None
    """
    count = 0
    # Step 8: Insert unique data into ingresos --> todo el df ya que son todas las transacciones
    proveedor_mapping, cuenta_mapping = mappings_fk_ingresos
    for _, row in df.iterrows():  # This should iterate through the full dataset
        # Retrieve foreign key ids from pre-fetched mappings
        id_donante = proveedor_mapping.get(row['numero'])
        id_cuenta = cuenta_mapping.get(row['nro_cuenta'])
    
        # Only insert if foreign keys are found
        if id_donante is not None and id_cuenta is not None:
            query = """
            INSERT INTO ingresos (importe, fecha, id_donante, id_cuenta)
            VALUES (%s, %s, %s, %s);
            """
            data = (row['importe'], row['fecha'], id_donante, id_cuenta)
            insert_or_ignore(conn, cursor, query, data)
            count += 1
        else:
            print(f"Foreign key not found for {row['numero']} or {row['nro_cuenta']}")
    print(f"Cantidad de filas insertadas: {count}")

def load_datasets(path_proveedores: Path, path_donantes: Path) -> tuple:
    """
    Carga los datasets de proveedores y donantes desde archivos CSV

    params
    ------
      * path_proveedores: Path
        - ruta al archivo CSV de proveedores
      * path_donantes: Path
        - ruta al archivo CSV de donantes

    returns:
        - tuple con (dataframe de proveedores, dataframe de donantes)
    """
    df_proveedores = pd.read_csv(path_proveedores)
    df_donantes = pd.read_csv(path_donantes)
    np.random.seed(42)
    
    return df_proveedores, df_donantes


def setup_database_connection() -> tuple:
    """
    Configura la conexión a la base de datos y el cursor

    params
    ------

    returns:
        - tuple con (conexion, cursor)
    """
    load_dotenv()
    db_config = {
        "host": os.getenv("DATABASE_HOST"),
        "user": os.getenv("DATABASE_USER"),
        "password": os.getenv("DATABASE_PASSWORD"),
        "database": os.getenv("DATABASE_NAME")
    }
    
    conn = create_connection(db_config)
    cursor = crear_cursor(conn)
    
    return conn, cursor


def handle_proveedor_dimension_tables(conn, cursor, df) -> dict:
    """
    Inserta datos en las tablas de dimensión para proveedores y devuelve los mapeos de columnas

    params
    ------
      * conn:
        - conexión a la base de datos
      * cursor:
        - cursor de la base de datos
      * df: pd.DataFrame
        - dataframe de proveedores

    returns:
        - diccionario con mapeo de tablas a columnas
    """
    # Print column names to debug
    print("proveedor dataframe columns:", df.columns.tolist())
    
    # Define column mapping for proveedor dimension tables with correct CSV column names
    table_to_col = {
        "categoria_proveedores": "categoria",
        "tipo_contribuyentes": "contribuyente",
        "razones_sociales": "razon",
        "ciudades": "ciudad",
        "cuentas": "nro_cuenta"
    }
    
    # Insert data into dimension tables
    for table, col in table_to_col.items():
        ingest_table(conn, cursor, df, table, col)
    
    return table_to_col


def process_proveedor_data(conn, cursor, df, table_to_col) -> None:
    """
    Procesa los datos de proveedores: prepara las claves foráneas e inserta en las tablas de proveedores y gastos

    params
    ------
      * conn:
        - conexión a la base de datos
      * cursor:
        - cursor de la base de datos
      * df: pd.DataFrame
        - dataframe de proveedores
      * table_to_col: dict
        - diccionario con mapeo de tablas a columnas

    returns:
        - None
    """
    # Get unique proveedores
    df_proveedores = df.drop_duplicates(subset='numero')
    
    # Prepare foreign keys for proveedor table
    list_fk = ["id_categoria", "id_contribuyente", "id_razon", "id_ciudad"]
    table_cols = tuple(table_to_col.items())
    
    # Fetch all foreign key mappings
    categoria_mapping = fetch_fk(cursor, list_fk[0], table_cols[0][1], table_cols[0][0])
    contribuyente_mapping = fetch_fk(cursor, list_fk[1], table_cols[1][1], table_cols[1][0])
    razon_mapping = fetch_fk(cursor, list_fk[2], table_cols[2][1], table_cols[2][0])
    ciudades_mapping = fetch_fk(cursor, list_fk[3], table_cols[3][1], table_cols[3][0])
    
    # Package foreign keys for proveedor insertion
    mappings_fk_proveedor = (categoria_mapping, contribuyente_mapping, razon_mapping, ciudades_mapping)
    
    # Insert proveedor data
    ingest_proveedores(conn, cursor, df_proveedores, mappings_fk_proveedor)
    
    # Prepare foreign keys for expenses
    proveedor_mapping = fetch_fk(cursor, "id", "numero", "proveedores")
    cuenta_mapping = fetch_fk(cursor, "id_cuenta", "nro_cuenta", "cuentas")
    mappings_fk_gastos = (proveedor_mapping, cuenta_mapping)
    
    # Insert expense data
    ingest_gastos(conn, cursor, df, mappings_fk_gastos)


def handle_donante_dimension_tables(conn, cursor, df) -> dict:
    """
    Inserta datos en las tablas de dimensión para donantes y devuelve los mapeos de columnas

    params
    ------
      * conn:
        - conexión a la base de datos
      * cursor:
        - cursor de la base de datos
      * df: pd.DataFrame
        - dataframe de donantes

    returns:
        - diccionario con mapeo de tablas a columnas
    """
    # Print column names to debug
    print("donante dataframe columns:", df.columns.tolist())
    
    # Create a copy of the dataframe with renamed columns to use database-friendly names
    df_db = df.copy()
    col_mapping = {
        "Frecuencia": "frecuencia",
        "Tipo de Contribuyente": "contribuyente",
        "Razon Social": "razon",
        "Tipo": "tipo",
        "Pais": "pais",
        "Nro de Cuenta": "nro_cuenta"
    }
    df_db.rename(columns=col_mapping, inplace=True)
    
    # Define column mapping for donante dimension tables using database-friendly names
    table_to_col = {
        "frecuencias": "frecuencia",
        "tipo_contribuyentes": "contribuyente",
        "razones_sociales": "razon",
        "tipo_donantes": "tipo",
        "paises": "pais",
        "cuentas": "nro_cuenta"
    }
    
    # Insert data into dimension tables
    for table, col in table_to_col.items():
        if col in df_db.columns:
            ingest_table(conn, cursor, df_db, table, col)
        else:
            print(f"Warning: Column '{col}' not found in donante dataframe. Skipping table '{table}'")
    
    return table_to_col


def process_donante_data(conn, cursor, df) -> None:
    """
    Procesa los datos de donantes: prepara las claves foráneas e inserta en las tablas de donantes e ingresos

    params
    ------
      * conn:
        - conexión a la base de datos
      * cursor:
        - cursor de la base de datos
      * df: pd.DataFrame
        - dataframe de donantes

    returns:
        - None
    """
    # Create a copy of the dataframe with renamed columns to use database-friendly names
    df_db = df.copy()
    col_mapping = {
        "Numero": "numero",
        "Nombre": "nombre",
        "CUIT": "cuit",
        "Contacto": "contacto",
        "Correo Electronico": "mail",
        "Telefono": "telefono",
        "Activo": "activo",
        "Frecuencia": "frecuencia",
        "Tipo de Contribuyente": "contribuyente",
        "Razon Social": "razon",
        "Tipo": "tipo",
        "Pais": "pais",
        "Importe": "importe",
        "Nro de Cuenta": "nro_cuenta",
        "Fecha": "fecha"
    }
    df_db.rename(columns=col_mapping, inplace=True)
    
    # Using the correct donante number column name from the dataframe
    numero_col = 'numero'
    
    # Get unique donantes
    df_donantes = df_db.drop_duplicates(subset=numero_col)
    
    # Fetch foreign keys for donante table - using the database column names
    frecuencia_mapping = fetch_fk(cursor, id="id_frecuencia", col="frecuencia", table="frecuencias")
    contribuyente_mapping = fetch_fk(cursor, id="id_contribuyente", col="contribuyente", table="tipo_contribuyentes")
    razon_mapping = fetch_fk(cursor, id="id_razon", col="razon", table="razones_sociales")
    tipo_mapping = fetch_fk(cursor, id="id_tipo", col="tipo", table="tipo_donantes")
    pais_mapping = fetch_fk(cursor, id="id_pais", col="pais", table="paises")
    
    # Package foreign keys for donante insertion
    mappings_fk_donantes = (frecuencia_mapping, contribuyente_mapping, razon_mapping, tipo_mapping, pais_mapping)
    
    # Insert donante data
    ingest_donantes(conn, cursor, df_donantes, mappings_fk_donantes)
    
    # Prepare foreign keys for incomes
    donantes_mapping = fetch_fk(cursor, "id", "numero", "donantes")
    cuenta_mapping = fetch_fk(cursor, "id_cuenta", "nro_cuenta", "cuentas")
    mappings_fk_ingresos = (donantes_mapping, cuenta_mapping)
    
    # Insert income data
    ingest_ingresos(conn, cursor, df_db, mappings_fk_ingresos)


def close_connection(conn, cursor) -> None:
    """
    Cierra la conexión a la base de datos y el cursor

    params
    ------
      * conn:
        - conexión a la base de datos
      * cursor:
        - cursor de la base de datos

    returns:
        - None
    """
    if conn:
        cursor.close()
        conn.close()
        print("Desconectando Base de datos...")


def main() -> None:
    """
    Función principal para orquestar el proceso de ingestión de datos a la base de datos

    params
    ------

    returns:
        - None
    """
    repo_root = Path(__file__).resolve().parent.parent.parent
    data_prov = repo_root / "data" / "cleaned" / "proveedores-clean.csv"
    data_donan = repo_root / "data" / "cleaned" / "donantes-clean.csv"
    # Step 1: Load datasets
    df_proveedores, df_donantes = load_datasets(data_prov, data_donan)
    
    # Step 2: Setup database connection
    conn, cursor = setup_database_connection()
    
    # Display available tables
    all_tables = get_all_tables(cursor)
    print(f"Tablas\n: {all_tables}\n")
    
    # Step 3: Process proveedor data
    table_to_col = handle_proveedor_dimension_tables(conn, cursor, df_proveedores)
    process_proveedor_data(conn, cursor, df_proveedores, table_to_col)
    
    # Step 4: Process donante data
    handle_donante_dimension_tables(conn, cursor, df_donantes)
    process_donante_data(conn, cursor, df_donantes)
    
    # Step 5: Close database connection
    close_connection(conn, cursor)

if __name__ == "__main__":
    main()
