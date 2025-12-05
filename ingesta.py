import pandas as pd
import os
import re

BASE_PATH = r"C:/Users/layca/Desktop/TRABAJO FINAL/"

print("Iniciando proceso de ingesta...")

# ============================
# RUT Validation
# ============================
def validar_rut(rut):
    rut = rut.replace(".", "").replace("-", "").upper()
    if not re.match(r"^\d+[-]{0,1}[0-9K]$", rut):
        return False
    cuerpo = rut[:-1]
    dv = rut[-1]

    suma = 0
    multiplicador = 2

    for c in reversed(cuerpo):
        suma += int(c) * multiplicador
        multiplicador = 9 if multiplicador == 7 else multiplicador + 1

    resto = suma % 11
    dvr = str(11 - resto)
    dvr = "0" if dvr == "11" else "K" if dvr == "10" else dvr

    return dv == dvr


# ============================
# LEER CSV
# ============================
print("Leyendo CSV...")
df_csv = pd.read_csv(BASE_PATH + "clientes_info.csv")
print("Columnas del CSV:", df_csv.columns)

# ============================
# LEER TXT
# ============================
print("Leyendo TXT...")
df_txt = pd.read_csv(BASE_PATH + "clientes_extra.txt",
                     header=None,
                     names=["codigo", "canal", "codigo_app", "fecha_registro"])
print("Columnas del TXT:", df_txt.columns)

# ============================
# LEER SQL
# ============================
print("Leyendo SQL...")
sql_file = BASE_PATH + "clientes.sql"
rows = []

with open(sql_file, "r", encoding="utf-8") as f:
    for line in f:
        if "INSERT INTO" in line:
            valores = line.split("VALUES")[1].strip().strip("();")
            valores = [v.strip().strip("'") for v in valores.split(",")]
            rows.append(valores)

df_sql = pd.DataFrame(rows, columns=[
    "codigo", "nombre", "apellido", "comuna",
    "rut", "fecha_nacimiento", "religion"
])

df_sql["codigo"] = df_sql["codigo"].astype(int)

print("Columnas del SQL:", df_sql.columns)

# ============================
# VALIDACIONES
# ============================
print("Validando datos...")

df_sql["fecha_nacimiento"] = pd.to_datetime(df_sql["fecha_nacimiento"], errors="coerce")
df_txt["fecha_registro"] = pd.to_datetime(df_txt["fecha_registro"], errors="coerce")
df_sql["rut_valido"] = df_sql["rut"].apply(validar_rut)
df_sql["campos_ok"] = df_sql[["nombre", "apellido", "comuna", "rut"]].notnull().all(axis=1)

# ============================
# UNIÓN DE DATASETS
# ============================
print("Uniendo archivos...")

# 1. SQL + TXT (ambos tienen "codigo")
df_total = df_sql.merge(df_txt, on="codigo", how="left")

# 2. CSV (tiene codigo_cliente → renombrarlo a codigo)
df_csv = df_csv.rename(columns={"codigo_cliente": "codigo"})

# unir
df_total = df_total.merge(df_csv, on="codigo", how="left")

# ============================
# GUARDAR EN BRONZE
# ============================
output_dir = BASE_PATH + "bronze/ventas/"
os.makedirs(output_dir, exist_ok=True)

df_total.to_csv(output_dir + "clientes_bronze.csv", index=False)

print("Proceso finalizado. Archivo listo en /bronze/ventas/")
