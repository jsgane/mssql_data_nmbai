import os
import subprocess
import time
from pathlib import Path
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv
import logging
from bcp_wsl import BCPExporter
from confing import Config
from mssql import export_mssql_bcp
from snowflake_dest import setup_snowflake,upload_to_stage,copy_into_table

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# PIPELINE COMPLET ================================================

def run_pipeline(table_name: str = "AI_V_Inventory_Parts_Ops"):
    """
    Exécution complète du pipeline
    Reproduction du script PowerShell
    """
    
    start_time = time.time()
    
    logger.info("\n" + "=" * 80)
    logger.info("🚀 PIPELINE MSSQL → SNOWFLAKE")
    logger.info("   Méthode: BCP + COPY INTO (comme PowerShell)")
    logger.info("=" * 80 + "\n")
    
    try:
        # 1. Export BCP
        export_mssql_bcp(table_name)
        # Setup Snowflake (Créer file_format, stage et table)
        setup_snowflake(table_name)
        # Upload dans le staging (On a utilisé CSV mais peut être changé en parquet dans snowflake_dest.py)
        upload_to_stage()
        # COPY INTO stage -> table
        result = copy_into_table(table_name)        
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
    run_pipeline(table_name = "AI_V_Inventory_Parts_Ops")