#!/usr/bin/env python3
"""
Script para probar el FileProcessor copiando todos los archivos de tests/ a data/input
y mostrando el análisis de cada uno de ellos.
"""

import os
import shutil
from src.file_processor import FileProcessor

# --------------- 1. Copiar archivos CSV desde tests/ a data/input ---------------

# Asegúrate que data/input existe
os.makedirs('data/input', exist_ok=True)

# Lista de archivos csv en tests/
test_dir = 'tests'
input_dir = 'data/input'
csv_files = [f for f in os.listdir(test_dir) if f.endswith('.csv')]

# Borrar archivos anteriores en data/input (opcional, útil para pruebas limpias)
for f in os.listdir(input_dir):
    if f.endswith('.csv'):
        os.remove(os.path.join(input_dir, f))

# Copiar archivos, mostrar mensaje por cada uno
for fname in csv_files:
    src = os.path.join(test_dir, fname)
    dst = os.path.join(input_dir, fname)
    shutil.copy(src, dst)
    print(f"Archivo de TEST copiado: {fname}")

print(f"\n➡️  Analizando archivos: {csv_files}")
print("=" * 50)

# --------------- 2. Procesar y mostrar mensaje al analizar cada archivo ---------------

# Instanciar el procesador apuntando a data/input
processor = FileProcessor(input_folder=input_dir)

# Redefinir temporalmente la función read_csv_file para que printee el nombre antes de leer
original_read_csv_file = processor.read_csv_file

def wrapped_read_csv_file(filepath):
    print(f"\n🔎 Analizando {os.path.basename(filepath)} ...")
    return original_read_csv_file(filepath)

processor.read_csv_file = wrapped_read_csv_file

# Procesar todos los archivos (esto combina automáticamente los válidos)
data = processor.process_all_files()

# Comprobar si hubo éxito
if data is not None:
    print(f"\n✅ ¡Archivos combinados exitosamente!")
    print(f"Registros totales: {len(data)}")
    print("Primeros 3 registros:\n", data.head(3))
else:
    print("\n❌ Ningún archivo válido fue procesado.")

print("\n✔️  Proceso terminado.")
