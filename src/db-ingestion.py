#!/usr/bin/env python
# coding: utf-8

import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from mysql.connector import connect, Error, IntegrityError
from pathlib import Path

def main() -> None:
    pwd = Path.cwd()
    data_prov = pwd.parent / "data" / "cleaned" / "proveedores-clean.csv"
    data_donan = pwd.parent / "data" / "cleaned" / "donantes-clean.csv"
    df = pd.read_csv(data_prov)
    df2 = pd.read_csv(data_donan)
    np.random.seed(42)

    # ### Conectando a la base de datos que esta en Docker

    load_dotenv()
    db_config = {
        "host": os.getenv("DATABASE_HOST"),   ## gateway of container
        "user": os.getenv("DATABASE_USER"),
        "password": os.getenv("DATABASE_PASSWORD"),
        "database": os.getenv("DATABASE_NAME")
    }
    
    # creamos la conexion
    conn = create_connection(db_config)
    # creamos el cursor
    cursor = crear_cursor(conn)

    rename_cols = {"Numero Proveedor": "numero",
                       "Nombre Proveedor": "nombre",
                       "CUIT": "cuit",
                       "Contacto": "contacto",
                       "Correo Electronico": "mail",
                       "Telefono": "telefono",
                       "Categoria Proveedor": "categoria",
                       "Tipo de Contribuyente": "contribuyente",
                       "Razon Social": "razon",
                       "Ciudad": "ciudad",
                       "Nro_Cuenta": "nro_cuenta",
                       "Importe": "importe",
                       "Fecha": "fecha",}

    df.rename(columns=rename_cols, inplace=True)
    df.columns
    
    # solo para ver los nombres de las tablas
    all_tables = get_all_tables(cursor)
    print(f"Tablas\n: {all_tables}\n")
    
    # los datos deberian venir de un archivo de configuracion imagino
    # que columna va en que tabla
    table_to_col = {"categoria_proveedores": "categoria",
                    "tipo_contribuyentes": "contribuyente",
                    "razones_sociales": "razon",
                    "ciudades": "ciudad",
                    "cuentas": "nro_cuenta"}

    ########################################
    ####### insertar datos en tablas #######
    ########################################
    
    for table, col in table_to_col.items():
        ingest_table(conn, cursor, df, table, col)

        
    ################################################################################
    ################################# Proveedores ##################################
    ################################################################################

    ###############################################################
    ####### preparar foreign keys para la tabla proveedores #######
    ###############################################################

    # datos unicos de los proveedores basado en el numero
    df_proveedores = df.drop_duplicates(subset='numero')

    # complicandola de chill --- es bastante ilegible pero queria probar hacerlo de esta manera
    # traer las fk de todas las tablas que tengan
    list_fk = ["id_categoria", "id_contribuyente", "id_razon", "id_ciudad"]
    table_cols: tuple = tuple(table_to_col.items())


    # Agarramos todas las foreign keys y las guardamos para hacerlo mas rapido
    # Fetch id_categoria    
    categoria_mapping = fetch_fk(cursor, list_fk[0], table_cols[0][1], table_cols[0][0])
    # # Fetch id_contribuyente
    contribuyente_mapping = fetch_fk(cursor, list_fk[1], table_cols[1][1], table_cols[1][0])
    # # Fetch id_razon
    razon_mapping = fetch_fk(cursor, list_fk[2], table_cols[2][1], table_cols[2][0])
    # # Fetch id_ciudad
    ciudades_mapping = fetch_fk(cursor, list_fk[3], table_cols[3][1], table_cols[3][0])
    
    # tupla para pasar como argumento
    mappings_fk_proveedor: tuple = (categoria_mapping, contribuyente_mapping, razon_mapping, ciudades_mapping)

    ##########################################
    ####### insertar datos proveedores #######
    ##########################################

    ingest_proveedores(conn, cursor, df_proveedores, mappings_fk_proveedor)

    ##########################################################
    ####### preparar foreign keys para la tabla gastos #######
    ##########################################################


    # Fetch id_proveedor --> se llama 'id'
    proveedor_mapping = fetch_fk(cursor, "id", "numero", "proveedores")
    # Fetch id_cuenta
    cuenta_mapping = fetch_fk(cursor, "id_cuenta", "nro_cuenta", "cuentas")
    # tupla de parametros
    mappings_fk_gastos = (proveedor_mapping, cuenta_mapping)

    ##########################################
    ####### insertar datos proveedores #######
    ##########################################

    ingest_gastos(conn, cursor, df, mappings_fk_gastos)



    ################################################################################
    ################################### Donantes ###################################
    ################################################################################

    # Hay que reenombrar tambien las columnas matcheando las de las tablas

    rename_cols = {"Numero": "numero",
                       "Nombre": "nombre",
                       "CUIT": "cuit",
                       "Contacto": "contacto",
                       "Correo Electronico": "mail",
                       "Telefono": "telefono",
                       "Frecuencia": "frecuencia",
                       "Tipo de Contribuyente": "contribuyente",
                       "Tipo": "tipo",
                       "Activo": "activo",
                       "Razon Social": "razon",
                       "Pais": "pais",
                       "Nro de Cuenta": "nro_cuenta",
                       "Importe": "importe",
                       "Fecha": "fecha",}

    df2.rename(columns=rename_cols, inplace=True)
    df2.columns

    ######################################################################################
    ### Insertar datos en las tablas para despues relacionarla con la tabla `donantes` ###
    ######################################################################################

    # los datos deberian venir de un archivo de configuracion imagino
    # tabla: columna
    table_to_col = {"frecuencias": "frecuencia",
                    "tipo_contribuyentes": "contribuyente",
                    "razones_sociales": "razon",
                    "tipo_donantes": "tipo",
                    "paises": "pais",
                    "cuentas": "nro_cuenta"}

    for table, col in table_to_col.items():
        ingest_table(conn, cursor, df2, table, col)

    ##########################################################
    ####### preparar foreign keys para la tabla donantes #####
    ##########################################################

    # datos unicos de los proveedores basado en el numero
    df_donantes = df2.drop_duplicates(subset='numero')

    # Fetch foreign keys  
    # Hay que volver a fetchear `categoria`, `contribuyente` y `razon social` ya fuero los datos fueron actualizados.  
    #  Al igual que `tipo_donantes`, `pais` y `frecuencia` que son tablas nuevas `Donantes`

    frecuencia_mapping = fetch_fk(cursor, id="id_frecuencia", col="frecuencia", table="frecuencias")
    contribuyente_mapping = fetch_fk(cursor, id="id_contribuyente", col="contribuyente", table="tipo_contribuyentes")
    razon_mapping = fetch_fk(cursor, id="id_razon", col="razon", table="razones_sociales")
    tipo_mapping = fetch_fk(cursor, id="id_tipo", col="tipo", table="tipo_donantes")
    pais_mapping = fetch_fk(cursor, id="id_pais", col="pais", table="paises")
    
    mappings_fk_donantes = (frecuencia_mapping, contribuyente_mapping, razon_mapping, tipo_mapping, pais_mapping)

    ##########################################
    ####### insertar datos proveedores #######
    ##########################################

    ingest_donantes(conn, cursor, df_donantes, mappings_fk_donantes)
    
    ##########################################################
    ####### preparar foreign keys para la tabla ingresos #####
    ##########################################################

    # Fetch id_proveedor --> se llama 'id'
    donantes_mapping = fetch_fk(cursor, "id", "numero", "donantes")
    # Fetch id_cuenta
    cuenta_mapping = fetch_fk(cursor, "id_cuenta", "nro_cuenta", "cuentas")
    
    mappings_fk_ingresos = (donantes_mapping, cuenta_mapping)
    
    #######################################
    ####### insertar datos ingresos #######
    #######################################

    ingest_ingresos(conn, cursor, df2, mappings_fk_ingresos)

    # Close the cursor and connection
    if conn:
        cursor.close()
        conn.close()
        print("Desconectando Base de datos...")


def create_connection(db_config: dict):
    """
    Conexion a la base de datos

    params
    ------
        * db_config: dict - credenciales de la base de datos
    returns:
        conexion a la db
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
        * conn: - conexion de la base de datos

    returns: 
        - None
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
        * cursor: - cursor de la base de datos

    returns:
        - lista de nombres
    """
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    return [table[0] for table in tables]

def ingest_table(conn, cursor, df: pd.DataFrame, table: str, col_name: str) -> None:
    """
    Inserta los datos a la base de datos

    params
    ------
        * conn: - conexion a la db
        * cursor: - cursor de la db
        * df: DataFrame - datos que van a ser insertados
        * table: str - tabla que seleccionar
        * col_name: str - columna a seleccionar
    
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
        * cursor: - cursor de la db
        * id: str - id de la tabla
        * col: str - columna de la tabla
        * table: str - nombre de la tabla

    returns: dict
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
        * conn: - conexion a la db
        * cursor: - curosr de la db
        * df_proveedores: DataFrame - dataframe con los datos de los proveedores 
        * mappings_fk_proveedor: dict - diccionario con las foreign keys

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
        * conn: - conexion a la db
        * cursor: - curosr de la db
        * df: DataFrame - dataframe de donde se sacan los gatos 
        * mappings_fk_gastos: dict - diccionario con las foreign keys

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
        * conn: - conexion a la db
        * cursor: - curosr de la db
        * df_proveedores: DataFrame - dataframe con los datos de los proveedores 
        * mappings_fk_donantes: dict - diccionario con las foreign keys

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
        * conn: - conexion a la db
        * cursor: - curosr de la db
        * df: DataFrame - dataframe de donde se sacan los ingresos 
        * mappings_fk_ingresos: dict - diccionario con las foreign keys

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

if __name__ == "__main__":
    main()
