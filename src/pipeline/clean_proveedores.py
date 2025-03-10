#!/usr/bin/env python
# coding: utf-8

# In[4]:


import os
from pathlib import Path
import pandas as pd
import numpy as np


# In[5]:


data_dir = Path.cwd().parent / "data" /  "merged"
datos = "proveedores_final-merged.csv"
data = data_dir / datos
df = pd.read_csv(data)
np.random.seed(42)
df.sample(3)


# ## Algo de limpieza

# In[6]:


df.info()


# In[7]:


df["Categoria Proveedor"] = df["Categoria Proveedor"].str.replace("Servicos", "Servicios")
df["Categoria Proveedor"].unique()


# In[8]:


mask = df["Numero Proveedor"].str.startswith("D")
df.loc[mask, "Numero Proveedor"] = df.loc[mask, "Numero Proveedor"].str.replace("D", "P")


# In[9]:


df[df["Numero Proveedor"].str.startswith("D")]["Numero Proveedor"]


# In[10]:


# datetime
df["Fecha"].sample(2)


# In[11]:


fc = "Fecha"
df[fc] = pd.to_datetime(df[fc], format='mixed', dayfirst=True).dt.strftime('%Y/%m/%d')
df[fc] = pd.to_datetime(df[fc])
df[fc].tail()


# In[12]:


df = df.drop(["Pais", "Maps", "Ciudad-maps"], axis=1)
df.rename(columns={"Corre Electronico": "Correo Electronico"}, inplace=True)
df.info()


# In[13]:


df


# Ya que `S.A` y `Sociedad Anonima` son lo mismo, las juntamos y tambien borramos ese ultimo '.' en los demas

# In[14]:


df["Razon Social"].value_counts()


# In[15]:


df["Razon Social"].str.rstrip(".").str.replace("Sociedad Anónima", "S.A").value_counts()


# In[16]:


df["Razon Social"] = df["Razon Social"].str.rstrip(".").str.replace("Sociedad Anónima", "S.A")


# ## Last rename

# Export clean data

# In[17]:


data_dir = Path.cwd().parent / "data" / "cleaned"
data_clean = data_dir / "proveedores-clean.csv"
df.to_csv(data_clean, index=False)
get_ipython().system('ls {data_dir}')


# In[18]:


test  = pd.read_csv(data_clean)
test.info()

