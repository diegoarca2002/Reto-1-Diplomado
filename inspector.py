"""
Bronze Ingestor Pipeline
Diplomado Gestión de Datos 2026
Procesa archivos de landing/ clasificándolos según su contenido
"""

from pathlib import Path
import shutil
import logging
from datetime import datetime

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestor.log'),
        logging.StreamHandler()  # También imprime en consola
    ]
)

def crear_carpetas_destino():
    """Crea las carpetas bronze/ y bad_data/ si no existen"""
    try:
        Path("bronze").mkdir(parents=True, exist_ok=True)
        Path("bad_data").mkdir(parents=True, exist_ok=True)
        logging.info("Carpetas de destino verificadas/creadas exitosamente")
    except Exception as e:
        logging.critical(f"Error crítico al crear carpetas: {e}")
        raise

def procesar_archivo(archivo):
    """
    Procesa un archivo individual clasificándolo por tamaño
    
    Args:
        archivo (Path): Ruta del archivo a procesar
        
    Returns:
        bool: True si procesó exitosamente, False en caso contrario
    """
    try:
        logging.info(f"Iniciando proceso: {archivo.name}")
        
        # Verificar el tamaño del archivo
        tamanio = archivo.stat().st_size
        logging.info(f"Tamaño detectado: {tamanio} bytes")
        
        # Clasificar según el tamaño
        if tamanio > 0:
            # Archivo con contenido -> Bronze
            destino = Path("bronze") / archivo.name
            shutil.move(str(archivo), str(destino))
            logging.info(f"✓ Procesado: {archivo.name} -> Bronze ({tamanio} bytes)")
            return True
        else:
            # Archivo vacío -> Bad Data
            destino = Path("bad_data") / archivo.name
            shutil.move(str(archivo), str(destino))
            logging.warning(f"✗ Rechazado: {archivo.name} -> Bad Data (0 bytes)")
            return True
            
    except FileNotFoundError:
        logging.error(f"Error: El archivo {archivo.name} no se encuentra")
        return False
        
    except PermissionError:
        logging.error(f"Error: Sin permisos para mover {archivo.name}")
        return False
        
    except Exception as e:
        logging.error(f"Error inesperado con {archivo.name}: {type(e).__name__} - {e}")
        return False
        
    finally:
        logging.info(f"Finalizado intento para {archivo.name}")

def ejecutar_pipeline():
    """
    Función principal que ejecuta el pipeline completo de ingesta
    """
    inicio = datetime.now()
    logging.info("=" * 60)
    logging.info("INICIANDO BRONZE INGESTOR PIPELINE")
    logging.info("=" * 60)
    
    try:
        # Crear carpetas de destino
        crear_carpetas_destino()
        
        # Obtener carpeta landing
        carpeta_landing = Path("landing")
        
        # Verificar que landing/ existe
        if not carpeta_landing.exists():
            logging.error("La carpeta landing/ no existe. Creándola...")
            carpeta_landing.mkdir(parents=True, exist_ok=True)
            logging.warning("Carpeta landing/ creada pero está vacía")
            return
        
        # Obtener todos los archivos (no directorios) en landing/
        archivos = [f for f in carpeta_landing.iterdir() if f.is_file()]
        
        if not archivos:
            logging.warning("No se encontraron archivos en landing/")
            return
        
        logging.info(f"Archivos encontrados: {len(archivos)}")
        
        # Contadores para resumen
        procesados = 0
        rechazados = 0
        errores = 0
        
        # Procesar cada archivo
        for archivo in archivos:
            try:
                resultado = procesar_archivo(archivo)
                
                if resultado:
                    # Verificar a dónde fue movido
                    if (Path("bronze") / archivo.name).exists():
                        procesados += 1
                    else:
                        rechazados += 1
                else:
                    errores += 1
                    
            except Exception as e:
                logging.error(f"Error crítico no capturado en {archivo.name}: {e}")
                errores += 1
                continue  # Continuar con el siguiente archivo
        
        # Resumen final
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        
        logging.info("=" * 60)
        logging.info("RESUMEN DE EJECUCIÓN")
        logging.info("=" * 60)
        logging.info(f"Total de archivos procesados: {len(archivos)}")
        logging.info(f"  ✓ Movidos a Bronze: {procesados}")
        logging.info(f"  ✗ Movidos a Bad Data: {rechazados}")
        logging.info(f"  ⚠ Errores: {errores}")
        logging.info(f"Duración: {duracion:.2f} segundos")
        logging.info("=" * 60)
        
        # Verificar que landing/ quedó vacía
        archivos_restantes = list(carpeta_landing.glob("*"))
        if archivos_restantes:
            logging.warning(f"ADVERTENCIA: Quedan {len(archivos_restantes)} archivos en landing/")
        else:
            logging.info("✓ Carpeta landing/ vacía - Pipeline completado exitosamente")
            
    except Exception as e:
        logging.critical(f"Error fatal en el pipeline: {e}")
        raise

if __name__ == "__main__":
    ejecutar_pipeline()
