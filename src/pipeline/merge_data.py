import os
from argparse import ArgumentParser
import re
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import random
import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

# Function to load data
def load_data(data_path):
    """Load data from CSV and Excel files."""
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
    return dfpo, dfdo, dfpn, dfdn


# Function to clean provider data
def clean_providers_data(dfpn):
    dfpn_bk = dfpn.copy()

    def clean_prov_new(df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(["Observaciones"], axis=1)
        df.rename(columns={"Categor/a Proveedor": "Categoria Proveedor"}, inplace=True)

        cols_lower = ["Categoria Proveedor", "Tipo de Contribuyente"]
        df[cols_lower] = df[cols_lower].apply(lambda x: x.str.capitalize())

        cl = "Categoria Proveedor"
        df[cl] = df[cl].str.replace("/", "i").str.replace("Materiiales", "Materiales")

        df["Tipo de Contribuyente"] = df["Tipo de Contribuyente"].str.replace("/", "i")
        df["Teléfono"] = df["Teléfono"].str.replace("AR", "")
        return df

    dfpn_clean = clean_prov_new(dfpn)
    return dfpn_clean


# Function to clean donor data
def clean_donors_data(dfdo, dfdn):
    dfdo_bk = dfdo.copy()
    dfdo.drop(["Mes", "Cargo"], axis=1, inplace=True)
    dfdn.drop("Cargo", axis=1, inplace=True)

    col = "Razon Social"
    dfdo[col] = dfdo[col].replace("-", np.nan)
    dfdn[col] = dfdn[col].replace("-", np.nan)
    
    # Fill null values with values from adjacent rows (forward fill then backward fill)
    # This approach fills nulls with real company names instead of a generic placeholder
    dfdo[col] = dfdo[col].ffill().bfill()
    dfdn[col] = dfdn[col].ffill().bfill()

    dfdo["Importe"] = dfdo["Importe"].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    dfdn["Importe"] = dfdn["Importe"].apply(clean_importe)
    dfdn["Importe"] = dfdn["Importe"].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    return dfdo, dfdn


# Function to transform data
def transform_data(dfpn_clean, dfpo):
    ciudad_pd = dfpn_clean["Ciudad"].value_counts().reset_index()
    ciudad_pd["Probabilidad"] = ciudad_pd["count"] / ciudad_pd["count"].sum()

    ciudades = ciudad_pd["Ciudad"]
    proba = ciudad_pd["Probabilidad"]
    size = 150
    ciudad_sample = np.random.choice(ciudades, size=size, p=proba)

    ciudad_col = pd.Series(ciudad_sample)
    columns = ["Pais", "Maps"]
    cantidad = dfpo.shape[0]
    new_cols = dfpn_clean[columns][:cantidad]

    dfp_copy = dfpo.copy()
    dfp_copy["Ciudad"] = ciudad_col
    dfp_merge = pd.concat([dfp_copy, new_cols], axis=1)

    dfp_merge["Categoria Proveedor"] = dfp_merge["Categoria Proveedor"].apply(lambda x: x.capitalize())
    dfpmerge_final = pd.concat([dfpn_clean, dfp_merge], ignore_index=True)

    return dfpmerge_final


# Function to merge donor data
def merge_donor_data(dfdo, dfdn):
    sample_size = dfdo.shape[0]
    paises = sample_dist(dfdn, "País", sample_size)
    dfdo["Pais"] = paises

    dfdn["Baja"] = pd.to_datetime(dfdn["Baja"], errors='coerce')
    dfdn["Alta"] = pd.to_datetime(dfdn["Alta"], errors='coerce', format='mixed')

    fecha_min = dfdn["Baja"].min()
    fecha_max = dfdn["Baja"].max()

    n_fechas = dfdo[~dfdo["Baja"].isna()]["Baja"].count()

    def random_fecha(start, end):
        intervalo_fecha = (end - start).days
        rand = random.randint(0, intervalo_fecha)
        fecha = start + pd.to_timedelta(rand, unit='d')
        return fecha

    fechas_random = [random_fecha(fecha_min, fecha_max) for _ in range(n_fechas)]
    dfdo.loc[dfdo["Baja"].notna(), "Baja"] = fechas_random
    dfdo["Baja"] = pd.to_datetime(dfdo["Baja"], errors='coerce')

    donacion_sample = sample_dist(dfdn, "Fecha_Donación", dfdo.shape[0])
    sample_dona = pd.DataFrame(donacion_sample, columns=["Fecha_Donación"])
    donaciones_fecha = pd.concat([dfdn["Fecha_Donación"], sample_dona["Fecha_Donación"]], ignore_index=True)
    donaciones_fecha = pd.to_datetime(donaciones_fecha, format='mixed')

    dfdo.rename(columns={"Número": "Numero",
                          "Teléfono": "Telefono",
                          "Pais": "País"}, inplace=True)

    dfdn_trim = dfdn.drop(["Fecha_Donación"], axis=1)
    dfd_final = pd.concat([dfdo, dfdn_trim], ignore_index=True)

    meses = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
        7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }

    dfd_final["Mes"] = dfd_final["Alta"].dt.month.map(meses)

    return dfd_final, donaciones_fecha


# Function to export data
def export_data(dfd_final, dfp_final, path):
    if not os.path.exists(path):
        os.makedirs(path)

    donantes = "donantes_final-merged.csv"
    proveedores = "proveedores_final-merged.csv"

    dfd_final.to_csv(os.path.join(path, donantes), index=False)
    dfp_final.to_csv(os.path.join(path, proveedores), index=False)


# Function to clean importe
def clean_importe(importe):
    importe_cleaned = re.sub(r'[^\d,]', '', importe)
    importe_cleaned = importe_cleaned.replace('.', '')
    importe_cleaned = importe_cleaned.replace(',', '.')
    return float(importe_cleaned)


# Function to sample distribution
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

def final_renaming(dfd: pd.DataFrame, dfp: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Last minute cleaning and casting
    tipo_c = "Tipo de Contribuyente"
    categoria = "Categoria Proveedor"
    dfp[categoria] = dfp[categoria].str.replace("Servicos", "Servicios")
    dfp[tipo_c] = dfp[tipo_c].str.replace("Responsable inscriito", "Responsable inscripto")
    dfp[tipo_c] = dfp[tipo_c].str.replace("Iva responsable", "Responsable inscripto")
    dfp[tipo_c] = dfp[tipo_c].str.replace("Responsabile inscripto", "Responsable inscripto")
    dfp[tipo_c] = dfp[tipo_c].apply(lambda x: x.capitalize())

    dfp.rename(columns={"Número Proveedor": "Numero Proveedor",
                        "Correo Electrónico": "Corre Electronico",
                        "Razón Social": "Razon Social",
                        "Teléfono": "Telefono"},
               inplace=True)
    dfd.rename(columns={"País": "Pais",
                        "Correo Electrónico": "Correo Electronico"},
               inplace=True)
    return dfd, dfp

def importe_to_numeric(dfd: pd.DataFrame, dfp: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

    # Convert Importe to numeric
    def to_numeric_importe(importe):
        # Remove any non-numeric characters (except for commas)
        importe_cleaned = importe.replace('.', '')  # Remove thousands separator
        importe_cleaned = importe_cleaned.replace(',', '.')  # Replace decimal comma with a dot
        return importe_cleaned

    # Apply the cleaning function
    dfd['Importe'] = dfd['Importe'].apply(to_numeric_importe)
    dfp['Importe'] = dfp['Importe'].apply(to_numeric_importe)

    # Convert to numeric
    dfd['Importe'] = pd.to_numeric(dfd['Importe'], errors='coerce')
    dfp['Importe'] = pd.to_numeric(dfp['Importe'], errors='coerce')

    return dfd, dfp

def main() -> None:

    # Set the root path
    # this file/pipeline/src/root
    root_path = Path(__file__).resolve().parent.parent.parent
    
    # for final data
    final_data_path = root_path / "data" / "merged"

    # Load data from data/raw
    data_path = root_path / "data" / "raw"
    dfpo, dfdo, dfpn, dfdn = load_data(data_path)

    # Clean provider data
    dfpn_clean = clean_providers_data(dfpn)

    # Clean donor data
    dfdo, dfdn = clean_donors_data(dfdo, dfdn)

    # Transform data
    dfpmerge_final = transform_data(dfpn_clean, dfpo)

    # Note: We've removed the retrieve_coordinates function that was using the Nominatim API
    # to avoid timeouts and unnecessary API calls. The Maps column will remain as is
    # and will be removed later in the cleaning process anyway.
    
    # Set Maps column to None (it will be removed during cleaning)
    dfpmerge_final["Maps"] = None

    # Merge donor data
    dfd_final, donaciones_fecha = merge_donor_data(dfdo, dfdn)

    # Final cleanup and renaming
    dfp_final = dfpmerge_final.copy()

    dfd_final, dfp_final = final_renaming(dfd_final, dfp_final)

    # Last numeric conversion
    dfd_final, dfp_final = importe_to_numeric(dfd_final, dfp_final)

    # Export data
    export_data(dfd_final, dfp_final, final_data_path)

if __name__ == "__main__":
    main()