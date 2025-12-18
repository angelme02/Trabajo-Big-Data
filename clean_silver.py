import csv
from datetime import datetime
import os
import re

print("=== PROCESO DE LIMPIEZA SIMPLE (SILVER LAYER) ===")

# Configurar rutas
BRONZE_PATH = "bronze/ventas/clientes_bronze.csv"
SILVER_PATH = "silver/ventas/"

# Crear directorio silver si no existe
os.makedirs(SILVER_PATH, exist_ok=True)

# Función para normalizar texto
def normalize_text(text):
    if text is None or text == "":
        return text
    return ' '.join([word.capitalize() for word in str(text).strip().split()])

# Función para calcular edad
def calcular_edad(fecha_str):
    if not fecha_str or fecha_str == "":
        return None
    try:
        fecha_nac = datetime.strptime(str(fecha_str), '%Y-%m-%d')
        hoy = datetime.now()
        edad = hoy.year - fecha_nac.year
        if (hoy.month, hoy.day) < (fecha_nac.month, fecha_nac.day):
            edad -= 1
        return edad
    except:
        return None

# Función para categorizar edad
def categorizar_edad(edad):
    if edad is None:
        return "NO_REGISTRADA"
    if edad < 18:
        return "MENOR"
    elif 18 <= edad <= 35:
        return "JOVEN"
    elif 36 <= edad <= 60:
        return "ADULTO"
    else:
        return "ADULTO_MAYOR"

# Función para categorizar gasto
def categorizar_gasto(monto_str):
    if not monto_str or monto_str == "":
        return "NO_REGISTRADO"
    try:
        monto = float(monto_str)
        if monto < 100000:
            return "BAJO"
        elif 100000 <= monto < 500000:
            return "MEDIO"
        else:
            return "ALTO"
    except:
        return "NO_REGISTRADO"

print("1. Leyendo datos de bronze...")
try:
    # Leer archivo CSV
    with open(BRONZE_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames
        print(f"✅ Datos cargados: {len(rows)} registros")
        print(f"Columnas: {fieldnames}")
except FileNotFoundError:
    print(f"❌ ERROR: No se encuentra el archivo {BRONZE_PATH}")
    print("Ejecuta primero 'ingesta.py' para generar los datos bronze")
    exit(1)

print("\n2. Procesando datos...")
processed_rows = []
for i, row in enumerate(rows):
    # Normalizar texto en columnas específicas
    text_columns = ["nombre", "apellido", "comuna", "religion", 
                    "canal", "tarjeta_beneficios", "tipo_alimentacion"]
    
    for col in text_columns:
        if col in row:
            row[col] = normalize_text(row[col])
    
    # Manejar nulos en columnas categóricas
    categorical_defaults = {
        "religion": "No especificada",
        "tipo_alimentacion": "No especificada",
        "canal": "NO_REGISTRADO",
        "tarjeta_beneficios": "NO_REGISTRADO",
        "codigo_app": "NO_REGISTRADO"
    }
    
    for col, default in categorical_defaults.items():
        if col in row and (row[col] is None or row[col] == ""):
            row[col] = default
    
    # Convertir fechas
    if "fecha_nacimiento" in row:
        try:
            fecha = datetime.strptime(row["fecha_nacimiento"], '%Y-%m-%d')
            row["fecha_nacimiento"] = fecha.strftime('%Y-%m-%d')
        except:
            row["fecha_nacimiento"] = ""
    
    if "fecha_registro" in row:
        try:
            fecha = datetime.strptime(row["fecha_registro"], '%Y-%m-%d')
            row["fecha_registro"] = fecha.strftime('%Y-%m-%d')
        except:
            row["fecha_registro"] = ""
    
    # Crear columnas derivadas
    if "fecha_nacimiento" in row:
        edad = calcular_edad(row["fecha_nacimiento"])
        row["edad"] = str(edad) if edad is not None else ""
        row["categoria_edad"] = categorizar_edad(edad)
    
    if "promedio_compras" in row:
        row["categoria_gasto"] = categorizar_gasto(row["promedio_compras"])
    
    processed_rows.append(row)

print(f"✅ Datos procesados: {len(processed_rows)} registros")

# Actualizar fieldnames con nuevas columnas
if "edad" not in fieldnames:
    fieldnames.append("edad")
if "categoria_edad" not in fieldnames:
    fieldnames.append("categoria_edad")
if "categoria_gasto" not in fieldnames:
    fieldnames.append("categoria_gasto")

print("\n3. Guardando en silver...")
csv_path = SILVER_PATH + "clientes_silver.csv"
with open(csv_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(processed_rows)

print(f"✅ CSV guardado: {csv_path}")

print("\n" + "=" * 50)
print("🎉 PROCESO COMPLETADO EXITOSAMENTE")
print("=" * 50)

# Mostrar resumen
print(f"\n📊 RESUMEN:")
print(f"Total registros: {len(processed_rows)}")

# Estadísticas simples
canales = {}
categorias_edad = {}
for row in processed_rows:
    canal = row.get("canal", "NO_REGISTRADO")
    canales[canal] = canales.get(canal, 0) + 1
    
    categoria = row.get("categoria_edad", "NO_REGISTRADA")
    categorias_edad[categoria] = categorias_edad.get(categoria, 0) + 1

print("\n📈 Distribución por canal:")
for canal, count in canales.items():
    print(f"  {canal}: {count}")

print("\n📈 Distribución por categoría de edad:")
for categoria, count in categorias_edad.items():
    print(f"  {categoria}: {count}")

print("\n👁️  Primeras 3 filas:")
for i in range(min(3, len(processed_rows))):
    print(f"\nFila {i+1}:")
    for key in ["codigo", "nombre", "apellido", "edad", "categoria_edad"]:
        if key in processed_rows[i]:
            print(f"  {key}: {processed_rows[i][key]}")
