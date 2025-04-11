#!/usr/bin/env python
# coding: utf-8

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def load_data(ruta_datos):
    """Carga los datos de donantes desde un archivo CSV."""
    df = pd.read_csv(ruta_datos)
    print(f"Datos cargados con forma: {df.shape}")
    return df

def clean_contactos(df):
    """Limpia e imputa valores nulos en la columna Contacto."""
    cond = df["Contacto"] == " "
    cond2 = df["Contacto"] == "-"
    cond3 = df["Contacto"] == ""
    
    df.loc[cond | cond2 | cond3, "Contacto"] = np.nan
    
    # Rellenar valores nulos con el valor anterior
    df["Contacto"] = df["Contacto"].ffill()
    
    return df

def format_razon_social(df):
    """Corrige los formatos de Razón Social uniformizando nomenclaturas."""
    df = df.replace({"Razon Social": {"SRL": "S.R.L", "SA": "S.A", "SAS": "S.A.S"}})
    return df

def fill_tipo(df):
    """Rellena valores nulos en la columna Tipo utilizando forward fill."""
    col = "Tipo"
    df[col] = df[col].ffill()
    return df

def estandarizar_contribuyente(df):
    """Estandariza los valores en la columna Tipo de Contribuyente."""
    tipoc = "Tipo de Contribuyente"
    df_uc = unique_col(df, [tipoc])
    cur = df_uc[tipoc][3]  # "IVA Responsable Inscripto"
    rep = df_uc[tipoc][0]  # "Responsable Inscripto"
    df[tipoc] = df[tipoc].str.replace(cur, rep)
    return df

def handle_duplicates_cuit(df):
    """Maneja duplicados en la columna CUIT modificando un valor específico."""
    df2 = df.drop_duplicates(subset="Numero")
    dup = df2[df2["CUIT"].duplicated()]
    
    if not dup.empty:
        cui = dup["CUIT"].iloc[0]
        # Reemplazar un 8 por un 0 en el CUIT en el índice 499
        replaced_str = df.loc[499, "CUIT"].replace('8', '0')
        # Verificar que no exista ya
        if (df['CUIT'] == replaced_str).sum() == 0:
            df.loc[499, "CUIT"] = replaced_str
    
    return df

def convert_data_types(df):
    """Convierte las columnas Fecha y Activo a los tipos de datos correctos."""
    # Renombrar Alta a Fecha
    df.rename(columns={"Alta": "Fecha"}, inplace=True)
    
    # Convertir a datetime
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    
    # Convertir Activo a booleano
    df["Activo"] = df["Activo"].str.upper().map({"SI": True, "NO": False})
    
    return df

def choose_final_columns(df):
    """Selecciona las columnas relevantes para el dataset final."""
    df_final = df[["Numero", "Nombre", "Tipo",
                  "Contacto", "Correo Electronico",
                  "Telefono", "Razon Social",
                  "Tipo de Contribuyente",
                  "CUIT", "Fecha", "Activo",
                  "Frecuencia", "Importe",
                  "Nro de Cuenta", "Pais"]]
    return df_final

def rename_final_columns(df):
    """Renombra las columnas al formato final deseado."""
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
    
    df.rename(columns=rename_cols, inplace=True)
    return df

def export_data(df, ruta_salida):
    """Exporta los datos limpios a un archivo CSV."""
    # Asegurar que el directorio exista
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(ruta_salida, index=False)
    print(f"Datos exportados a {ruta_salida}")
    return df

def unique_col(df, cols):
    """Devuelve un diccionario con los valores únicos de cada columna."""
    cols_dict = {col: df[col].unique() for col in cols}
    return cols_dict

def nunique_col(df):
    """Crea un DataFrame con el conteo de valores únicos por columna."""
    cols = [(col, df[col].nunique()) for col in df.columns]
    df_t = pd.DataFrame(cols, columns=["Columna", "Unicos"])
    return df_t

def verify_data(df):
    """Verifica y muestra información sobre el dataset limpio."""
    print("\nInformación del dataset limpio:")
    df.info()
    
    columns = ['tipo', 'razon', 'contribuyente', 'frecuencia', 'pais']
    print("\nValores únicos por columna:")
    print(unique_col(df, columns))
    
    print("\nConteo de valores únicos por columna:")
    print(nunique_col(df))

def main(args=None):
    """Función principal que ejecuta la limpieza de datos de donantes."""
    # Configurar semilla para reproducibilidad
    np.random.seed(42)
    
    # Configurar rutas
    repo_root = Path(__file__).resolve().parent.parent.parent
    
    # Determinar rutas según argumentos
    if args and args.test:
        data_dir = repo_root / "data" / "test"
    else:
        data_dir = repo_root / "data" / "merged"
    
    data_path = data_dir / "donantes_final-merged.csv"
    
    
    # Cargar datos
    print(f"Cargando datos desde {data_path}")
    df = load_data(data_path)
    
    # Limpiar datos
    print("Limpiando e imputando valores en Contacto...")
    df = clean_contactos(df)
    
    print("Corrigiendo formato de Razón Social...")
    df = format_razon_social(df)
    
    print("Rellenando valores nulos en Tipo...")
    df = fill_tipo(df)
    
    print("Estandarizando valores en Tipo de Contribuyente...")
    df = estandarizar_contribuyente(df)
    
    print("Manejando duplicados en CUIT...")
    df = handle_duplicates_cuit(df)
    
    print("Convirtiendo tipos de datos...")
    df = convert_data_types(df)
    
    print("Seleccionando columnas relevantes...")
    df_final = choose_final_columns(df)
    
    print("Renombrando columnas al formato final...")
    df_final = rename_final_columns(df_final)
    
    # Ruta de salida
    data_dir_out = repo_root / "data" / "cleaned"
    data_clean_out = data_dir_out / "donantes-clean.csv"

    # Exportar datos
    print("Exportando datos limpios...")
    df_exportado = export_data(df_final, data_clean_out)
    
    # Verificar si se solicita
    if args and args.verificar:
        print("Verificando datos exportados...")
        df_verificacion = pd.read_csv(data_clean_out)
        verify_data(df_verificacion)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Limpieza de datos de donantes")
    parser.add_argument("-t", "--test", action="store_true", help="Usar datos de prueba")
    parser.add_argument("-v", "--verificar", action="store_true", help="Verificar datos después de exportar")
    args = parser.parse_args()
    main(args)
