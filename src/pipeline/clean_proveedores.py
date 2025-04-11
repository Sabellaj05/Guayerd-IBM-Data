#!/usr/bin/env python
# coding: utf-8

import argparse
import os
from pathlib import Path
import pandas as pd
import numpy as np

def load_data(ruta_datos):
    """Carga los datos de proveedores desde un archivo CSV."""
    df = pd.read_csv(ruta_datos)
    print(f"Datos cargados con forma: {df.shape}")
    return df

def clean_categorias(df):
    """Corrige errores en la columna Categoria Proveedor."""
    df["Categoria Proveedor"] = df["Categoria Proveedor"].str.replace("Servicos", "Servicios")
    return df

def fix_provider_numbers(df):
    """Estandariza los números de proveedor reemplazando 'D' por 'P'."""
    mask = df["Numero Proveedor"].str.startswith("D")
    df.loc[mask, "Numero Proveedor"] = df.loc[mask, "Numero Proveedor"].str.replace("D", "P")
    return df

def format_dates(df):
    """Convierte la columna Fecha a formato datetime estandarizado."""
    fc = "Fecha"
    df[fc] = pd.to_datetime(df[fc], format='mixed', dayfirst=True).dt.strftime('%Y/%m/%d')
    df[fc] = pd.to_datetime(df[fc])
    return df

def drop_unnecessary_columns(df):
    """Elimina columnas que no son necesarias para el dataset final."""
    df = df.drop(["Pais", "Maps", "Ciudad-maps"], axis=1)
    df.rename(columns={"Corre Electronico": "Correo Electronico"}, inplace=True)
    return df

def standardize_razon_social(df):
    """Estandariza los nombres de razón social."""
    df["Razon Social"] = df["Razon Social"].str.rstrip(".").str.replace("Sociedad Anónima", "S.A")
    return df

def rename_final_columns(df):
    """Renombra las columnas al formato final deseado."""
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
    return df

def export_data(df, ruta_salida):
    """Exporta los datos limpios a un archivo CSV."""
    # Asegurar que el directorio exista
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(ruta_salida, index=False)
    print(f"Datos exportados a {ruta_salida}")
    return df

def main(args=None):
    """Función principal que ejecuta la limpieza de datos de proveedores."""
    # Configurar semilla para reproducibilidad
    np.random.seed(42)
    
    # Configurar rutas
    repo_root = Path(__file__).resolve().parent.parent.parent
    
    # Determinar rutas según argumentos
    if args and args.test:
        data_dir = repo_root / "data" / "test"
    else:
        data_dir = repo_root / "data" / "merged"
    
    data_path = data_dir / "proveedores_final-merged.csv"
    
    
    # Cargar datos
    print(f"Cargando datos desde {data_path}")
    df = load_data(data_path)
    
    # Limpiar datos
    print("Limpiando categorías de proveedores...")
    df = clean_categorias(df)
    
    print("Corrigiendo números de proveedor...")
    df = fix_provider_numbers(df)
    
    print("Formateando fechas...")
    df = format_dates(df)
    
    print("Eliminando columnas innecesarias...")
    df = drop_unnecessary_columns(df)
    
    print("Estandarizando nombres de empresas...")
    df = standardize_razon_social(df)
    
    print("Renombrando columnas al formato final...")
    df = rename_final_columns(df)
    
    # Ruta de salida
    data_dir_out = repo_root / "data" / "cleaned"
    data_clean_out = data_dir_out / "proveedores-clean.csv"

    # Exportar datos
    print("Exportando datos limpios...")
    df_exportado = export_data(df, data_clean_out)
    
    # Mostrar información si se solicita
    if args and args.verificar:
        print("\nInformación del dataset limpio:")
        df_exportado.info()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Limpieza de datos de proveedores")
    parser.add_argument("-t", "--test", action="store_true", help="Usar datos de prueba")
    parser.add_argument("-v", "--verificar", action="store_true", help="Verificar datos después de exportar")
    args = parser.parse_args()
    main(args)