import os
import subprocess
import time
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
import logging
from bcp_wsl import BCPExporter
from config import Config
import os
import subprocess
import time
from pathlib import Path
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ===EXPORT BCP (équivalent code PowerShell) ============

def export_mssql_bcp_old():
    """
    Export SQL Server → CSV avec BCP
    Équivalent: bcp "SELECT..." queryout "file.csv" -c -t "," -S server -d db -U user -P pwd
    """
    
    logger.info("=" * 80)
    logger.info("📤 Export BCP depuis SQL Server")
    logger.info("=" * 80)
    
    # Créer le répertoire
    Config.OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Supprimer le fichier s'il existe
    if Config.OUTPUT_PATH.exists():
        Config.OUTPUT_PATH.unlink()
        logger.info(f"🗑️  Fichier existant supprimé: {Config.OUTPUT_PATH}")
    
    # Commande BCP
    bcp_command = [
        Config.BCP_PATH,
        Config.QUERY,
        "queryout",
        str(Config.OUTPUT_PATH),
        "-c",                           # Character data
        "-t", Config.DELIMITER,         # Field terminator
        "-S", Config.MSSQL_SERVER,      # Server
        "-d", Config.MSSQL_DATABASE,    # Database
        "-U", Config.MSSQL_USER,        # Username
        "-P", Config.MSSQL_PASSWORD,    # Password
    ]
    
    logger.info(f"🔄 Exécution BCP: {Config.QUERY[:50]}...")
    start_time = time.time()
    
    try:
        # Exécuter BCP
        result = subprocess.run(
            bcp_command,
            capture_output=True,
            text=True,
            check=True
        )
        
        duration = time.time() - start_time
        file_size_mb = Config.OUTPUT_PATH.stat().st_size / (1024 * 1024)
        
        logger.info(f"✅ Export BCP terminé en {duration:.2f}s")
        logger.info(f"   Fichier: {Config.OUTPUT_PATH}")
        logger.info(f"   Taille: {file_size_mb:.2f} MB")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Erreur BCP: {e.stderr}")
        raise


def export_mssql_bcp():
    """Export BCP avec support WSL"""
    logger.info("=" * 80)
    logger.info("📤 Export BCP depuis SQL Server")
    logger.info("=" * 80)
    
    # Créer le répertoire
    Config.OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Supprimer le fichier existant
    if Config.OUTPUT_PATH.exists():
        Config.OUTPUT_PATH.unlink()
        logger.info(f"🗑️  Fichier existant supprimé: {Config.OUTPUT_PATH}")

    # Créer l'exporter
    exporter = BCPExporter(
        server=Config.MSSQL_SERVER,
        database=Config.MSSQL_DATABASE,
        username=Config.MSSQL_USER,
        password=Config.MSSQL_PASSWORD,
        use_wsl=Config.USE_WSL
    )
    
    logger.info(f"🔄 Exécution BCP: {Config.QUERY[:50]}...")
    start_time = time.time()
    
    try:
        # Export (retourne success, duration, size)
        success, bcp_duration, file_size_mb = exporter.export(
            query=Config.QUERY,
            output_path=Config.OUTPUT_PATH,
            delimiter=Config.DELIMITER
        )
        
        # Durée totale de la fonction
        total_duration = time.time() - start_time
        
        # info
        logger.info(f"✅ Export BCP terminé en {total_duration:.2f}s")
        logger.info(f"   Temps BCP: {bcp_duration:.2f}s")
        logger.info(f"   Fichier: {Config.OUTPUT_PATH}")
        logger.info(f"   Taille: {file_size_mb:.2f} MB")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Erreur BCP: {e}")
        raise