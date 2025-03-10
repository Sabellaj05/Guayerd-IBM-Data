#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import pandas as pd
import numpy as np


# In[5]:


data_dir = Path.cwd().parent / "data" / "merged"
datos = "donantes_final-merged.csv"
data = data_dir / datos
np.random.seed(42)
df = pd.read_csv(data)
df.sample(5)


# In[6]:


df.info()


# Vamos a imputar esos valores nulos en vez de eliminarlos

# In[7]:


# maybe?
# cond = df["Contacto"] in [" ", "-", ""]
# 
cond = df["Contacto"] == " "
cond2 = df["Contacto"] == "-"
cond3 = df["Contacto"] == ""
df[cond | cond2 | cond3]["Contacto"]


# In[9]:


df.loc[cond | cond2 | cond3, "Contacto"] = np.nan
nans = df.loc[cond | cond2 | cond3, "Contacto"]


# In[10]:


df["Contacto"] = df["Contacto"].ffill()


# In[11]:


list(nans.index)


# In[12]:


df.iloc[list(nans.index), 3:5]


# Corregir las razone sociales

# In[13]:


print(df["Razon Social"].unique())
print(df.replace({"Razon Social": {"SRL": "S.R.L", "SA": "S.A", "SAS": "S.A.S"}})["Razon Social"].unique())


# In[14]:


# lo confirmamos
df = df.replace({"Razon Social": {"SRL": "S.R.L", "SA": "S.A", "SAS": "S.A.S"}})
df["Razon Social"].unique()


# In[15]:


mask = df["Tipo"].isna()
nans = df.loc[mask, df.columns[:4]]
nans


# In[16]:


col = "Tipo"
df[col] = df[col].ffill()
print(df[col].isna().sum())


# In[17]:


# listo
df.iloc[list(nans.index), :4]


# In[18]:


columns = ['Tipo', 'Razon Social', 'Tipo de Contribuyente', 'Frecuencia', 'Pais']
def unique_col(df, cols):
    cols = {col: df[col].unique() for col in cols}
    return cols
df_uc = unique_col(df, columns)
df_uc


# Me olvide de cambiar `IVA Responsable Inscripto` a solo `Responsable Inscripto`  

# In[19]:


tipoc = "Tipo de Contribuyente"
cur = df_uc[tipoc][3]
rep = df_uc[tipoc][0]
df[tipoc] = df[tipoc].str.replace(cur, rep)


# In[20]:


df_uc = unique_col(df, columns)
df_uc["Tipo de Contribuyente"]


# In[21]:


def nunique_col(df):
    cols = [(col, df[col].nunique()) for col in df.columns]
    df_t = pd.DataFrame(cols, columns=["Columna", "Unicos"])
    return df_t
df_nuc = nunique_col(df)
df_nuc


# ---

# Para que no haya `CUIT` duplicados ya que hay uno solo, en vez de borrarlo lo cambio.  
# Los demas como `contacto` imagino que se pueden duplicar en la realidad. Los telefonos no deberian, pero ya que no son reales dejamos que se dupliquen

# In[22]:


# buscamos
df2 = df.drop_duplicates(subset="Numero")
dup = df2[df2["CUIT"].duplicated()]
dup


# In[23]:


# vemos quienes lo comparten
cui = dup["CUIT"].iloc[0]
df2[df2["CUIT"] == cui]


# In[25]:


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

# In[26]:


df.rename(columns={"Alta": "Fecha"}, inplace=True)
df.columns


# In[27]:


print(df["Fecha"].dtype)
df["Fecha"] = pd.to_datetime(df["Fecha"])
print(df["Fecha"].dtype)


# In[28]:


df["Activo"]


# In[29]:


print(df["Activo"].dtype)
# cambiamos a valores booleanos y despues casteamos el tipo
df["Activo"] = df["Activo"].str.upper().map({"SI": True, "NO": False})
print(df["Activo"].dtype)


# In[30]:


df.info()


# In[31]:


df_final = df[["Numero", "Nombre", "Tipo",
               "Contacto", "Correo Electronico",
               "Telefono", "Razon Social",
               "Tipo de Contribuyente",
               "CUIT", "Fecha", "Activo",
               "Frecuencia", "Importe",
               "Nro de Cuenta", "Pais"]]
df_final.info()


# ## Last rename

# In[32]:


unique_col(df_final, columns)


# In[33]:


nunique_col(df_final)


# In[34]:


data_dir = Path.cwd().parent / "data" / "cleaned"
data_clean = data_dir / "donantes-clean.csv"
df_final.to_csv(data_clean, index=False)
get_ipython().system('ls {data_dir}')


# In[35]:


asd = pd.read_csv(data_clean)
asd.info()


# In[36]:


nunique_col(asd)


# In[37]:


unique_col(asd, columns)

