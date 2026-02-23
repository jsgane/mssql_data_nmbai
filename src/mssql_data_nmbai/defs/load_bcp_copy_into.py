import os
import subprocess
import time
from pathlib import Path
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv
import logging
from bcp_wsl import BCPExporter
from confing import Config, export_mssql_bcp
from mssql import export_mssql_bcp
from snowflake_dest import setup_snowflake,upload_to_stage,copy_into_table

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# PIPELINE COMPLET ================================================

def run_pipeline():
    """
    Exécution complète du pipeline ultra-rapide
    Reproduction exacte du script PowerShell
    """
    
    start_time = time.time()
    
    logger.info("\n" + "=" * 80)
    logger.info("🚀 PIPELINE ULTRA-RAPIDE MSSQL → SNOWFLAKE")
    logger.info("   Méthode: BCP + COPY INTO (comme PowerShell)")
    logger.info("=" * 80 + "\n")
    
    try:
        # 1. Export BCP
        export_mssql_bcp()
        
        # 2. Setup Snowflake
        setup_snowflake()
        
        # 3. Upload
        upload_to_stage()
        
        # 4. COPY INTO
        result = copy_into_table()
        
        # Durée totale
        total_duration = time.time() - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
        logger.info(f"   Durée totale: {total_duration:.2f}s")
        logger.info(f"   Lignes: {result['rows_loaded']:,}")
        logger.info(f"   Vitesse: {result['rows_loaded'] / total_duration:.0f} lignes/sec")
        logger.info("=" * 80 + "\n")
        
        return result
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR PIPELINE: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()