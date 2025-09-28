#!/usr/bin/env Python3
"""
Prueba SOLO del FileProcessor - para verificar que funciona antes de continuar
"""

# Primero copiar el archivo de datos a la carpeta correcta
import shutil # shell utilies. Copiar/mover archivos
import os 
from src.file_processor import FileProcessor

def test_file_processor():
    print('📐 PROBANDO FILE PROCESSOR')
    print('=' * 50)

    # Preparo el entorno
    os.makedirs('data/input', exist_ok=True)
    shutil.copy("tests/sample_data_ok1.csv", "data/input/sample_data_ok1.csv")
    print("📃 Archivo copiado.")

    # Probar procesador
    processor = FileProcessor('data/input')
    data = processor.process_all_files()

    if data is not None:
        print(f'\n✅ ¡ÉXITO! Registros: {len(data)}')
        print(f'Columnas: {list(data.columns)}')
        print('\nPrimeras 3 filas:')
        print(data.head(3))
    else:
        print('❌ Error en el procesamiento')

if __name__ == "__main__":
    test_file_processor()