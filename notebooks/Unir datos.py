#!/usr/bin/env python
# coding: utf-8

# # Agregar y limpiar datos

# In[1]:


import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

pd.set_option('display.max_rows', 500)
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


get_ipython().system('ls ./Datos/10-9-extras/')


# In[3]:


# Dataframes
data_prov_old = "./Datos/10-9-extras/Proveedores - Proveedores.csv"
data_donan_old = "./Datos/10-9-extras/Donantes.xlsx"
data_prov = "./Datos/10-9-extras/10-9 Nuevos_registros_proveedores-copy - Proveedores nuevos.csv"
data_donan = "./Datos/10-9-extras/10-9 donantes_nuevos_registros-copy.xlsx - donantes_nuevos_registros.csv"

# old
dfpo = pd.read_csv(data_prov_old)
dfdo = pd.read_excel(data_donan_old, sheet_name="Donantes")
# new
dfpn = pd.read_csv(data_prov)
dfdn = pd.read_csv(data_donan)


# #### ***NOTA***
# ***fecha de alta como fecha de donacion, codigo area como argentina/buenos aires***
# 
# ---

# ## Viejo vs nuevo

# ### Proveedores

# In[4]:


sp_old = pd.Series(dfpo.columns.to_list())
sp_new = pd.Series(dfpn.columns.to_list())

display_prov = pd.concat([sp_old, sp_new], axis=1)
print(f"prov_old shape: {dfpo.shape}, prov_new shape: {dfpn.shape}\n")
display_prov.columns = ["Viejos", "Nuevos"]
display_prov


# ### Donantes

# In[5]:


sd_old = pd.Series(dfdo.columns.to_list())
sd_new = pd.Series(dfdn.columns.to_list())

display_don = pd.concat([sd_old, sd_new] ,axis=1)
print(f"prov_old shape: {dfdo.shape}, prov_new shape: {dfdn.shape}\n")
display_don.columns = ["Viejos", "Nuevos"]
display_don


# In[6]:


display_don.tail(2)


# La diferencia es en la columna de `Mes` que cree anteriormente y 2 nuevas columnas `Fecha_Donación` y `País`
# 
# ---

# ## Un poco de limpieza de los nuevos datos antes de empezar a unir

# **Proveedores**

# In[7]:


dfpn.info()


# #### **NOTA**  
# ***Samplear distribucion de ciudades, y imputarlas en las columnas que faltan para despues unir los datos, despues generar verdaderos numeros de telefonos en base a la ciudad `+54[area][numero-random-con-sentido]`***
# 
# ---

# Solo la columna Maps esta vacia pero eso lo arreglamos al final

# Un vistazo rapido con [_Data Wrangler_](https://code.visualstudio.com/docs/datascience/data-wrangler) podemos ver que solo hay que limpiar 3 columnas  
# Algunos `'/'` extras en **Categoria proveedor** y **Tipo de contribuyente**, y algunos `'AR'` extras en los numeros de telefono. Tambien dropeamos la columna **Observaciones** ya que no aporta nada

# In[8]:


dfpn_bk = dfpn.copy()

def clean_prov_new(df: pd.DataFrame) -> pd.DataFrame:
  df = df.drop(["Observaciones"], axis=1)
  df.rename(columns={"Categor/a Proveedor": "Categoria Proveedor"}, inplace=True)

  cols_lower = ["Categoria Proveedor", "Tipo de Contribuyente"]
  # aplicamos .lower a nivel de fila en ambas columnas (funciona porque ambas son str)
  df[cols_lower] = df[cols_lower].apply(lambda x: x.str.capitalize())

  ## Piola usando apply pero muy largo, buena oportunidad para mejorar en regex
  # def clean_slash(value):
  #   if "/" in value:
  #     slash = value.index("/")
  #     if value[slash - 1] == "i" or value[slash + 1] == "i":
  #       return value.replace("/", "")
  #     else:
  #       return value.replace("/", "i")
  #   else:
  #     return value
  # df["Categoria Proveedor"] = df["Categoria Proveedor"].apply(clean_slash)
  #

  df["Categoria Proveedor"] = df["Categoria Proveedor"].str.replace(r"/?(i)", "", regex=True)

  df["Tipo de Contribuyente"] = df["Tipo de Contribuyente"].str.replace("/", "")
  df["Teléfono"] = df["Teléfono"].str.replace("AR", "")
  return df


# In[9]:


# check
dfpn_clean = clean_prov_new(dfpn)
dfpn_clean.sort_values(by=["Teléfono"], ascending=False).head()


# In[10]:


dfpn_clean["Categoria Proveedor"].value_counts() \
  .sort_values(ascending=True) \
  .plot(kind="barh",
         title="Proveedores por Categoria")
plt.show()


# **NOTA**: Cambiar los telefonos acorde a los paises

# ---

# ## Empezar a acomodar las columnas

# **Proveedores**

# Primero que nada para evitar unir los datos y que haya datos faltantes en el anterior dataset de 150 registros con los nuevos 373 registros los cuales tienen 3 columnas nuevas, voy a primero agregar esas 3 columnas con 150 registros.  
# 
# El campo `Pais` y `Maps` es simple ya que es solo 1 dato, pero en el campo `Ciudad` voy a agarrar la distribucion de las ciudades de los nuevos datos y generar 150 nuevas para despues poder unir todo y que tenga sentido.

# In[39]:


# Creamos la distribucion para despues samplearla al agregar las ciudades
ciudad_pd = dfpn_clean["Ciudad"].value_counts().reset_index()
ciudad_pd["Probabilidad"] = ciudad_pd["count"] / ciudad_pd["count"].sum()
ciudad_pd.head()


# Ahora usamos `random.choice` agarrando 150 ciudades basadas en la distribucion

# In[41]:


ciudades = ciudad_pd["Ciudad"]
proba = ciudad_pd["Probabilidad"]
size = 150
ciudad_sample = np.random.choice(ciudades, size=size, p=proba)


# Agregamos las 3 columnas para despues unir todo a nivel registro

# In[60]:


# ciudades
ciudad_col = pd.Series(ciudad_sample)
columns = ["Pais", "Maps"]
cantidad = dfpo.shape[0]
# pais y maps
new_cols = dfpn_clean[columns][:150]

# unimos con los datos viejos

dfp_copy = dfpo.copy()
dfp_copy["Ciudad"] = ciudad_col
dfp_merge = pd.concat([dfp_copy, new_cols], axis=1)
# shape
print(dfp_merge.shape)
dfp_merge.info()


# Ahora directamente unimos todo con `dfpn_clean`

# In[61]:


dfpn_clean.info()


# In[59]:


dfpmerge_final = pd.concat([dfpn_clean, dfpmerge])
print(dfpmerge_final.shape)
dfpmerge_final.info()


# Ahora que estan las nuevas filas en las columnas en comun hay que agregar las columnas nuevas
