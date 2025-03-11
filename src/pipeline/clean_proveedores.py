#!/usr/bin/env python
# coding: utf-8

import os
from pathlib import Path
import pandas as pd
import numpy as np

repo_root = Path(__file__).resolve().parent.parent.parent
data_dir = repo_root / "data" / "merged"
data = data_dir / "proveedores_final-merged.csv"
df = pd.read_csv(data)
np.random.seed(42)
df.sample(3)

# Algo de limpieza
df.info()

df["Categoria Proveedor"] = df["Categoria Proveedor"].str.replace("Servicos", "Servicios")
df["Categoria Proveedor"].unique()

mask = df["Numero Proveedor"].str.startswith("D")
df.loc[mask, "Numero Proveedor"] = df.loc[mask, "Numero Proveedor"].str.replace("D", "P")

df[df["Numero Proveedor"].str.startswith("D")]["Numero Proveedor"]

# datetime
df["Fecha"].sample(2)

fc = "Fecha"
df[fc] = pd.to_datetime(df[fc], format='mixed', dayfirst=True).dt.strftime('%Y/%m/%d')
df[fc] = pd.to_datetime(df[fc])
df[fc].tail()

df = df.drop(["Pais", "Maps", "Ciudad-maps"], axis=1)
df.rename(columns={"Corre Electronico": "Correo Electronico"}, inplace=True)
df.info()

df

# Ya que `S.A` y `Sociedad Anonima` son lo mismo, las juntamos y tambien borramos ese ultimo '.' en los demas
df["Razon Social"].value_counts()

df["Razon Social"].str.rstrip(".").str.replace("Sociedad Anónima", "S.A").value_counts()

df["Razon Social"] = df["Razon Social"].str.rstrip(".").str.replace("Sociedad Anónima", "S.A")


# Final renaming of columns

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

# Export clean data
data_dir = repo_root / "data" / "cleaned"
data_clean = data_dir / "proveedores-clean.csv"
df.to_csv(data_clean, index=False)