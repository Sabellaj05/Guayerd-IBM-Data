#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
import numpy as np

data_dir = Path.cwd().parent / "data" / "merged"
datos = "donantes_final-merged.csv"
data = data_dir / datos
np.random.seed(42)
df = pd.read_csv(data)
df.sample(5)

df.info()

# Vamos a imputar esos valores nulos en vez de eliminarlos
cond = df["Contacto"] == " "
cond2 = df["Contacto"] == "-"
cond3 = df["Contacto"] == ""
df[cond | cond2 | cond3]["Contacto"]

df.loc[cond | cond2 | cond3, "Contacto"] = np.nan
nans = df.loc[cond | cond2 | cond3, "Contacto"]

df["Contacto"] = df["Contacto"].ffill()

list(nans.index)

df.iloc[list(nans.index), 3:5]

# Corregir las razone sociales
print(df["Razon Social"].unique())
print(df.replace({"Razon Social": {"SRL": "S.R.L", "SA": "S.A", "SAS": "S.A.S"}})["Razon Social"].unique())

df = df.replace({"Razon Social": {"SRL": "S.R.L", "SA": "S.A", "SAS": "S.A.S"}})
df["Razon Social"].unique()

mask = df["Tipo"].isna()
nans = df.loc[mask, df.columns[:4]]
nans

col = "Tipo"
df[col] = df[col].ffill()
print(df[col].isna().sum())

# listo
df.iloc[list(nans.index), :4]

columns = ['Tipo', 'Razon Social', 'Tipo de Contribuyente', 'Frecuencia', 'Pais']
def unique_col(df, cols):
    cols = {col: df[col].unique() for col in cols}
    return cols
df_uc = unique_col(df, columns)
df_uc

# Me olvide de cambiar `IVA Responsable Inscripto` a solo `Responsable Inscripto`  
tipoc = "Tipo de Contribuyente"
cur = df_uc[tipoc][3]
rep = df_uc[tipoc][0]
df[tipoc] = df[tipoc].str.replace(cur, rep)

df_uc = unique_col(df, columns)
df_uc["Tipo de Contribuyente"]

def nunique_col(df):
    cols = [(col, df[col].nunique()) for col in df.columns]
    df_t = pd.DataFrame(cols, columns=["Columna", "Unicos"])
    return df_t
df_nuc = nunique_col(df)
df_nuc

# Para que no haya `CUIT` duplicados ya que hay uno solo, en vez de borrarlo lo cambio.  
# Los demas como `contacto` imagino que se pueden duplicar en la realidad. Los telefonos no deberian, pero ya que no son reales dejamos que se dupliquen
df2 = df.drop_duplicates(subset="Numero")
dup = df2[df2["CUIT"].duplicated()]
dup

# vemos quienes lo comparten
cui = dup["CUIT"].iloc[0]
df2[df2["CUIT"] == cui]

# buscamos y cambiamos
replaced_str = df.loc[499, "CUIT"].replace('8', '0')
# nos fijamos que no exista ya
print(f"Cuantos tiene este cuit?: {(df['CUIT'] == replaced_str).sum()}")
# reemplazamos
df.loc[499, "CUIT"] = replaced_str
# deberia dar 138
df["CUIT"].nunique()

# Quedaria solo poner los tipos de datos correcto y elegir las columnas a quedarse  
# La columna `Alta` va a ser la fecha en que se hizo la donacion
df.rename(columns={"Alta": "Fecha"}, inplace=True)
df.columns

print(df["Fecha"].dtype)
df["Fecha"] = pd.to_datetime(df["Fecha"])
print(df["Fecha"].dtype)

print(df["Activo"].dtype)
# cambiamos a valores booleanos y despues casteamos el tipo
df["Activo"] = df["Activo"].str.upper().map({"SI": True, "NO": False})
print(df["Activo"].dtype)

df.info()

df_final = df[["Numero", "Nombre", "Tipo",
               "Contacto", "Correo Electronico",
               "Telefono", "Razon Social",
               "Tipo de Contribuyente",
               "CUIT", "Fecha", "Activo",
               "Frecuencia", "Importe",
               "Nro de Cuenta", "Pais"]]
df_final.info()

# Final renaming of columns

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
unique_col(df_final, columns)

nunique_col(df_final)

data_dir = Path.cwd().parent / "data" / "cleaned"
data_clean = data_dir / "donantes-clean.csv"
df_final.to_csv(data_clean, index=False)

asd = pd.read_csv(data_clean)
asd.info()

nunique_col(asd)

unique_col(asd, columns)
