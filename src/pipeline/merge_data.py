import os
import re
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

curr = Path.cwd()
data_path = curr / "final-data" / "raw"
# Dataframes
data_prov_old = data_path / "proveedores-old.csv"
data_donan_old = data_path / "donantes-old.xlsx"
data_prov = data_path / "proveedores-new.csv"
data_donan = data_path / "donantes-new.csv"

# old
dfpo = pd.read_csv(data_prov_old)
dfdo = pd.read_excel(data_donan_old, sheet_name="Donantes")
# new
dfpn = pd.read_csv(data_prov)
dfdn = pd.read_csv(data_donan)

print(f"Proveedores\ndfpo: {dfpo.shape}\ndfpn: {dfpn.shape}\n\nDonantes\ndfdo: {dfdo.shape}\ndfdn: {dfdn.shape}")


# #### ***NOTA***
# ***fecha de alta como fecha de donacion, codigo area como argentina/buenos aires***
# 

# ## Viejo vs nuevo

# ### Proveedores
sp_old = pd.Series(dfpo.columns.to_list())
sp_new = pd.Series(dfpn.columns.to_list())

display_prov = pd.concat([sp_old, sp_new], axis=1)
print(f"prov_old shape: {dfpo.shape}, prov_new shape: {dfpn.shape}\n")
display_prov.columns = ["Viejos", "Nuevos"]
display_prov

# ### Donantes

sd_old = pd.Series(dfdo.columns.to_list())
sd_new = pd.Series(dfdn.columns.to_list())

display_don = pd.concat([sd_old, sd_new] ,axis=1)
print(f"prov_old shape: {dfdo.shape}, prov_new shape: {dfdn.shape}\n")
display_don.columns = ["Viejos", "Nuevos"]
display_don

display_don.tail(2)

# La diferencia es en la columna de `Mes` que cree anteriormente y 2 nuevas columnas `Fecha_Donación` y `País`
# 

# ## Un poco de limpieza de los nuevos datos antes de empezar a unir

# **Proveedores**
dfpn.info()


# #### **NOTA**  
# ***Samplear distribucion de ciudades, y imputarlas en las columnas que faltan para despues unir los datos, despues generar verdaderos numeros de telefonos en base a la ciudad `+54[area][numero-random-con-sentido]`***
# 

# Solo la columna Maps esta vacia pero eso lo arreglamos al final

# Un vistazo rapido con [_Data Wrangler_](https://code.visualstudio.com/docs/datascience/data-wrangler) podemos ver que solo hay que limpiar 3 columnas  
# Algunos `'/'` extras en **Categoria proveedor** y **Tipo de contribuyente**, y algunos `'AR'` extras en los numeros de telefono. Tambien dropeamos la columna **Observaciones** ya que no aporta nada

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
  cl = "Categoria Proveedor"
  df[cl] = df[cl].str.replace("/", "i").str.replace("Materiiales", "Materiales")

  df["Tipo de Contribuyente"] = df["Tipo de Contribuyente"].str.replace("/", "i")
  df["Teléfono"] = df["Teléfono"].str.replace("AR", "")
  return df

# check
dfpn_clean = clean_prov_new(dfpn)
dfpn_clean.sort_values(by=["Teléfono"], ascending=False).head()

dfpn_clean["Categoria Proveedor"].unique()

dfpn_clean["Categoria Proveedor"].value_counts() \
  .sort_values(ascending=True) \
  .plot(kind="barh",
         title="Proveedores por Categoria")
plt.show()


def clean_importe(importe):
    # Remove the currency symbol (if present)
    importe_cleaned = re.sub(r'[^\d,]', '', importe)
    # Replace periods with nothing
    importe_cleaned = importe_cleaned.replace('.', '')
    # Now replace the comma with a period to standardize decimal point
    importe_cleaned = importe_cleaned.replace(',', '.')
    # Convert to float for consistency
    return float(importe_cleaned)

dfpo["Importe"] = dfpo["Importe"].apply(clean_importe)
dfpo["Importe"] = dfpo["Importe"].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

dfpn_clean["Importe"] = dfpn_clean["Importe"].apply(clean_importe)
dfpn_clean["Importe"] = dfpn_clean["Importe"].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
dfpn_clean["Importe"].head(10)

# ## Empezar a acomodar las columnas

# **Proveedores**

# Primero que nada para evitar unir los datos y que haya datos faltantes en el anterior dataset de 150 registros con los nuevos 373 registros los cuales tienen 3 columnas nuevas, voy a primero agregar esas 3 columnas con 150 registros.  
# 
# El campo `Pais` y `Maps` es simple ya que es solo 1 dato, pero en el campo `Ciudad` voy a agarrar la distribucion de las ciudades de los nuevos datos y generar 150 nuevas para despues poder unir todo y que tenga sentido.


# Creamos la distribucion para despues samplearla al agregar las ciudades
ciudad_pd = dfpn_clean["Ciudad"].value_counts().reset_index()
ciudad_pd["Probabilidad"] = ciudad_pd["count"] / ciudad_pd["count"].sum()
ciudad_pd.head()

# Ahora usamos `random.choice` agarrando 150 ciudades basadas en la distribucion

ciudades = ciudad_pd["Ciudad"]
proba = ciudad_pd["Probabilidad"]
size = 150
ciudad_sample = np.random.choice(ciudades, size=size, p=proba)


# funcion por si despues sirve
def sample_dist(df: pd.DataFrame,
                 column: str,
                   size: int) -> np.array:

    dist = df[column].value_counts().reset_index()
    dist["Probabilidad"] = dist["count"] / dist["count"].sum()
    sample = dist[column]
    proba = dist["Probabilidad"]
    size = size
    final_sample = np.random.choice(sample, size=size, p=proba)
    return final_sample


# Agregamos las 3 columnas para despues unir todo a nivel registro

ciudad_col = pd.Series(ciudad_sample)
columns = ["Pais", "Maps"]
cantidad = dfpo.shape[0]
# pais y maps
new_cols = dfpn_clean[columns][:cantidad]

# unimos con los datos viejos

dfp_copy = dfpo.copy()
dfp_copy["Ciudad"] = ciudad_col
# unimos las nuevas columnas
dfp_merge = pd.concat([dfp_copy, new_cols], axis=1)
# shape
print(dfp_merge.shape)
dfp_merge.iloc[:, -4:].head()

# capitalize
cl = "Categoria Proveedor"
dfp_merge[cl] = dfp_merge[cl].apply(lambda x: x.capitalize())
dfp_merge[cl].head()


# Ahora directamente unimos todo con `dfpn_clean` a nivel registro  
# y terminamos con shape **(registros: 523, campos: 15)**

dfpmerge_final = pd.concat([dfpn_clean, dfp_merge], ignore_index=True)
print(dfpmerge_final.shape)


# Ahora agregamos las coordenadas basado en la ciudad, para eso voy a usar la API _Nominatim_ de [OpenStreetMaps](https://www.openstreetmap.org/). Para ser un poco responsable voy a cachear las ciudades encontradas y poner un tiempo entre las requests. (Esto lo hago tambien para aprender de paso a usar esta API)

# previo hay que instalar la libreria geopy `conda install -c conda-forge geopy`
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="proveedores-test")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# para cachear las encontradas
coor_city = {}

# usar el cache para evitar el mal uso de la API
def get_coords(city):
    if city in coor_city:
        return coor_city[city]
    else:
        location = geocode(city)
        if location:
        # tupla de coordenadas
            coords = (location.latitude, location.longitude)
        # cacheamos la encontrada
            coor_city[city] = coords
            return coords
        else:
            return None

dfpmerge_final["Maps"] = dfpmerge_final["Ciudad"].apply(get_coords)
print(dfpmerge_final["Maps"].isna().sum())


dfpmerge_final["Ciudad-maps"] = dfpmerge_final["Ciudad"] + ", " + dfpmerge_final["Pais"]
dfpmerge_final["Ciudad-maps"].head()


# Toque final que olvide para el importe

# #### **Donantes**

dfdo.head()

# En este caso la unica columna nueva es `Pais` por lo tanto solo habria que samplear eso, agregar la columna a los datos viejos y unir, siempre algo de limpieza previa por las dudas

# La columna `Mes` la saco y agrego de vuelta al final completa, y la columna `cargo` no parece ser relevante al faltar datos y estar vacia en los nuevos datos asi que tambien la saco

dfdo_bk = dfdo.copy()
dfdo.drop(["Mes", "Cargo"], axis=1, inplace=True)
# lo mismo con los nuevos datos
dfdn.drop("Cargo", axis=1, inplace=True)


# Leve limpieza

col = "Razon Social"
dfdo[col] = dfdo[col].replace("-", np.nan)
dfdn[col] = dfdn[col].replace("-", np.nan)


# Formatear bien importe

dfdo["Importe"] = dfdo["Importe"].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

dfdn["Importe"] = dfdn["Importe"].apply(clean_importe)
dfdn["Importe"] = dfdn["Importe"].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

dfdn["Importe"].head()


dfdo["Importe"].head()


# Samplear e imputar tambien valores de `SRL` que sirven para el dashboard

# cantidad de datos a imputar
srl_size = dfdo["Razon Social"].isna().sum()
# agarramos los valores no nulos para samplear
srl_datos = dfdn[~dfdn[col].isna()]
# tuki
srl_sample = sample_dist(srl_datos, "Razon Social", size=srl_size)


# Ahora hay que reemplazar por los nulos en ambos datos viejos `dfdo` y nuevos `dfdn`

# imputamos los 66 registros
dfdo.loc[dfdo[col].isna(), col] = srl_sample
# contamos cuantos nulos a imputar
srl_size_n = dfdn[col].isna().sum()
# imputamos esa cantidad
dfdn.loc[dfdn[col].isna(), col] = srl_sample[:srl_size_n]


# Checkeamos

print(f"dfdo nulos: {dfdo[col].isna().sum()}")
dfdo[col].value_counts()


print(f"dfdn nulos: {dfdn[col].isna().sum()}")
dfdn[col].value_counts()


# Lo mismo con los paises

sample_size = dfdo.shape[0]
# sample
paises = sample_dist(dfdn, "País", sample_size)
dfdo["Pais"] = paises

dfdo.info()


# Solo resta unir a nivel registros ambos datasets, pero antes hay que poner bien los tipos de datos ya que uno tiene `datetime` y otro numeros.  
# Pasamos todo a `datetime` y mandamos fechas la azar entre la fecha mas baja y mas alta de los nuevos datos

# Primero la pasamos a datetime ignorando los Nan con 'coerce'
dfdn["Baja"] = pd.to_datetime(dfdn["Baja"], errors='coerce')
dfdn["Alta"] = pd.to_datetime(dfdn["Alta"], errors='coerce', format='mixed')


# intervalo
fecha_min = dfdn["Baja"].min()
fecha_max = dfdn["Baja"].max()

# cantidad a generar
n_fechas = dfdo[~dfdo["Baja"].isna()]["Baja"].count()
# me entero que existe 'notna()'
# n_fechas = dfdo[dfdo["Baja"].notna()]["Baja"].count()
import random
def random_fecha(start, end):
    # diferencia de dias entre ambas fechas
    intervalo_fecha = (end - start).days
    # cantidad de dias al azar entre esa diferencia
    rand = random.randint(0, intervalo_fecha) 
    # agarramos la fecha inicial y le sumamos el numero generado
    # al ser ambos objetos date se formatea bien
    fecha = start + pd.to_timedelta(rand, unit='d')
    return fecha
# lista de N fechas 
fechas_random = [random_fecha(fecha_min, fecha_max) for _ in range(n_fechas)]

# reemplazamos en dfdo

dfdo.loc[dfdo["Baja"].notna(), "Baja"] = fechas_random
dfdo["Baja"] = pd.to_datetime(dfdo["Baja"], errors='coerce')
dfdo["Baja"].head()


# Agregamos tambien mas filas a la nueva columna `Fecha_Donacion` para que tenga la misma dimension al momento agregarla

donacion_sample = sample_dist(dfdn, "Fecha_Donación", dfdo.shape[0])
sample_dona = pd.DataFrame(donacion_sample, columns=["Fecha_Donación"])
donaciones_fecha = pd.concat([dfdn["Fecha_Donación"], sample_dona["Fecha_Donación"]], ignore_index=True)
# datetime
donaciones_fecha = pd.to_datetime(donaciones_fecha, format='mixed')


# Checkeemos que los nombre de las columnas sean igual y unimos los datos y luego creamos la columna `Mes` anteriormente borrada y `Fecha_Donacion`

dfdo_copy2 = dfdo.copy()
dfdo.rename(columns={"Número": "Numero",
                      "Teléfono": "Telefono",
                      "Pais": "País"}, inplace=True)


dfdn_trim = dfdn.drop(["Fecha_Donación"], axis=1)
dfd_final = pd.concat([dfdo, dfdn_trim], ignore_index=True)


# Agregamos las columnas

# para mapear
meses = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

dfd_final["Mes"] = dfd_final["Alta"].dt.month.map(meses)
dfd_final.head()


dfd_final.info()


# Mas limpieza

# mas claridad
dfp_final = dfpmerge_final.copy()

tipo_c = "Tipo de Contribuyente"
categoria = "Categoria Proveedor"
# clean
dfp_final[categoria] = dfp_final[categoria].str.replace("Servicos", "Servicios")
# more clean
dfp_final[tipo_c] = dfp_final[tipo_c].str.replace("Responsable inscriito", "Responsable inscripto")
dfp_final[tipo_c] = dfp_final[tipo_c].str.replace("Iva responsable", "Responsable inscripto")
dfp_final[tipo_c] = dfp_final[tipo_c].str.replace("Responsabile inscripto", "Responsable inscripto")
dfp_final[tipo_c] = dfp_final[tipo_c].apply(lambda x: x.capitalize())
print(dfp_final[tipo_c].unique())


# **Renombrar por ultima vez**

dfp_final.rename(columns={"Número Proveedor": "Numero Proveedor",
                  "Correo Electrónico": "Corre Electronico",
                  "Razón Social": "Razon Social",
                  "Teléfono": "Telefono"},
                  inplace=True)
dfd_final.rename(columns={"País": "Pais",
                          "Correo Electrónico": "Correo Electronico"},
                          inplace=True)


dfp_final.info()


dfp_final.head()


# ### Exportar como csv

# Define a function to clean and convert to numeric
def to_numeric_importe(importe):
    # Remove any non-numeric characters (except for commas)
    importe_cleaned = importe.replace('.', '')  # Remove thousands separator
    importe_cleaned = importe_cleaned.replace(',', '.')  # Replace decimal comma with a dot
    return importe_cleaned

# Apply the cleaning function
dfd_final['Importe'] = dfd_final['Importe'].apply(to_numeric_importe)
dfp_final['Importe'] = dfp_final['Importe'].apply(to_numeric_importe)

# Convert to numeric
dfd_final['Importe'] = pd.to_numeric(dfd_final['Importe'], errors='coerce')
dfp_final['Importe'] = pd.to_numeric(dfp_final['Importe'], errors='coerce')

#  verify
print(dfd_final['Importe'].head())
print(dfp_final['Importe'].head())

# Check the data types to confirm conversion
print(dfd_final['Importe'].dtype)
print(dfp_final['Importe'].dtype)


path = curr / "final-data" / "merged"
if not os.path.exists(path):
    os.makedirs(path)

donantes = "donantes_final-merged.csv"
proveedores = "proveedores_final-merged.csv"

dfd_final.to_csv(os.path.join(path, donantes), index=False)
dfp_final.to_csv(os.path.join(path, proveedores), index=False)
