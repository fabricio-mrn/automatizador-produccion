#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
file_processor.py - Módulo para procesamiento de archivos CSV de producción

Este módulo contiene la clase FileProcessor que se encarga de:
1. Leer archivos CSV desde una carpeta específica
2. Validar que los datos tengan la estructura correcta
3. Combinar múltiples archivos en un solo DataFrame
4. Manejar errores de forma elegante

Autor: Fabricio Moreno
Fecha: Septiembre 2025
"""
import pandas as pd
import os
from datetime import datetime

class FileProcessor:
    """
    Clase que encapsula toda la lógica de procesamiento de archivos.
    """

    def __init__(self, input_folder="data/input"):
        """
        Constructor de la clase.

        Args:
            input_folder (str): Ruta donde están los archivos CSV a procesar
        """

        self.input_folder = input_folder

        # Crear carpeta si no existe
        if not os.path.exists(input_folder):
            os.makedirs(input_folder)
            print(f"📁 Carpeta creada: {input_folder}")

        print(f"🔧 FileProcessor inicializado para carpeta: {input_folder}")


    def read_csv_file(self, filepath):
        """
        Lee un archivo CSV específico y lo convierte en DataFrame.

        Args:
            filepath (str): Ruta completa al archivo CSV

        Returns:
            pd.DataFrame or None: Los datos en formato tabla, o None si hay error
        """

        try:
            print(f"📖 Intentando leer: {filepath}")

            # Leer CSV con configuraciones específicas para dar robustez
            df = pd.read_csv(
                filepath,
                encoding='utf-8', # Para que pandas no falle. Archivos con acentos o caracteres especiales
                sep=',', # Fuerzo la separación por ','                
                skipinitialspace=True # Elimina espacios después del separador.
            )

            # Convertir fecha a datetime si exite
            if 'fecha' in df.columns:
                df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')

                invalid_dates = df['fecha'].isna().sum()
                if invalid_dates > 0:
                    print(f'⚠️ {invalid_dates} Atención, fechas inválidas encontradas')

            print('✅ Archivo leído exitosamente')
            print(f'📊 Filas: {len(df)}')
            print(f'📊 Columnas: {len(df.columns)}')
            #print(f'📊 Columnas disponibles: {list(df.columns)}')

            return df
        
        except FileNotFoundError:
            print(f'❌ ERROR: Archivo no encontrado - {filepath}')
            return None
        
        except pd.errors.EmptyDataError:
            print(f'❌ ERROR: El archivo está vacío - {filepath} ')
            return None
        
        except pd.errors.ParserError as e:
            print(f'❌ ERROR: No se pudo interpretar el CSV - {filepath}')
            print(f'💡 DETALLE: {e}')
            return None
        
        except Exception as e:
            print(f'❌ ERROR INESPERADO al leer {filepath}: {type(e).__name__}: {e}')
            return None
        
    def validate_data(self, df):
        """
        Valida que el DataFrame tenga la estructura esperada

        Args:
            df (pd.DataFrame): DataFrame a válidar

        Returns:
            bool: True si es válido, False si hay problemas
        """

        print(f'🔍 Iniciando validación de datos...')
        
        if df is None:
            print(f'❌ VALIDACIÓN FALLIDA: DataFrame es None')
            return False
        
        if df.empty:
            print('❌ VALIDACIÓN FALLIDA: DataFrame está vacío')
            return False
        
        # Columnas obligatorias
        required_colums = ['fecha', 'turno', 'maquina', 'produccion_unidades', 'operario']
        
        # Verificar columnas faltantes
        missing_colums = [col for col in required_colums if col not in df.columns]

        if missing_colums:
            print(f'❌ VALIDACIÓN FALLIDA: Faltan las siguientes columnas obligatorias: {missing_colums}')
            print(f'📄 Columnas visibles: {list(df.columns)}')
            return False
        
        print('✅ Proceso de verificacion completado')
        return True
    

    def process_all_files(self):
        """
        Procesa todos los archivos CSV en la carpeta de entrada.
        
        Returns:
            pd.DataFrame or None: Datos combinados de todos los archivos válidos
        """

        print(f'\n🔍️ Buscando archivos CSV en: {self.input_folder}')

        try:
            # Listar solo archivos CSV
            all_files = os.listdir(self.input_folder) # Lista TODOS los archivos en la carpeta
            csv_files = [f for f in all_files if f.endswith('.csv')] # Selecciona solo los archivos csv
            
            print(f"📁 Archivos encontrados: {len(all_files)}")
            print(f"📄 Archivos CSV: {len(csv_files)}")
            
            if not csv_files:
                print("⚠️ No se encontraron archivos CSV para procesar")
                print(f"💡 SUGERENCIA: Copia tu archivo CSV a {self.input_folder}")
                return None
            
            print(f"📋 Archivos a procesar: {csv_files}")
            
        except FileNotFoundError:
            print(f"❌ ERROR: La carpeta {self.input_folder} no existe")
            return None
        except PermissionError:
            print(f"❌ ERROR: Sin permisos para acceder a {self.input_folder}")
            return None
        
        # Creo listas para almacenar resultados
        valid_dataframes = []
        processed_files = []
        failed_files = []

        # Procesar cada archivo individualmente
        for filename in csv_files:
            print(f"\n📁 Procesando: {filename}")
            filepath = os.path.join(self.input_folder, filename) # Une carpeta + archivo 

            # Intentar leer y validar el archivo
            df = self.read_csv_file(filepath) # Llamo a la otra funciñon para leer el CVS
            processed_files.append(filename)

            if df is not None and self.validate_data(df):
                # Agregar metadatos útiles como dos columnas nuevas
                df['archivo_origen'] = filename
                df['fecha_procesamiento'] = datetime.now()

                valid_dataframes.append(df)
                print(f'✅{filename} procesado correctamente')

            else:
                failed_files.append(filename)
                print(f'❌{filename} falló en el procesamiento')

        # Mostrar resumen del procesamiento
        print(f'\n📑 RESUMEN DE LOS PROCESMIENTOS:')
        print(f'📃 Archivos procesados: {len(processed_files)}')
        print(f'✅ Archivos éxitosos: {len(valid_dataframes)}')
        print(f'❌ Archivos fallidos: {len(failed_files)}')

        if failed_files:
            print(f'📋️ Archivos fallidos: {failed_files}')

        # Combinar todos los DataFrames válidos
        if valid_dataframes:
            print(f' Combinando {len(valid_dataframes)} archivos...')

            combined_df = pd.concat(valid_dataframes, ignore_index=True) # Concateno df válidos y renumera las filas desde 0

            print(f'🎉 PROCESO COMPLETADO:')
            print(f'📑Total de registros: {len(combined_df)}')
            print(f'📅Rango de fechas: {combined_df['fecha'].min()} - {combined_df['fecha'].max()}')
            print(f'🏭️Total de atomizadores: {combined_df['maquina'].nunique()}')
            print(f'🕒️Turnos: {combined_df['turno'].unique()}')

            return combined_df
        else:
            print(f'✋🏽 Ningun archivo pudo ser procesado correctamente.')
            return None
        