import os
import subprocess
import time
from pathlib import Path
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv
import logging
from mssql_data_nmbai.defs.config import Config, BCPExporter, export_mssql_bcp
##from mssql import export_mssql_bcp
from mssql_data_nmbai.defs.snowflake_dest import setup_snowflake,upload_to_stage,copy_into_table

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# PIPELINE COMPLET ================================================

def extract_mssql_data(
    snowflake_database: str,
    snowflake_schema: str, 
    mssql_table_name: str, 
    snowflake_table_name: str,
    logger
):
    """
    Exécution complète du pipeline
    Reproduction du script PowerShell
    """
    
    start_time = time.time()
    
    logger.info("\n" + "=" * 80)
    logger.info("🚀 PIPELINE MSSQL → SNOWFLAKE")
    logger.info("📤➡️❄️  Méthode: BCP + COPY INTO")
    logger.info("=" * 80 + "\n")
    
    try:
        # 1. Export BCP
        export_mssql_bcp(table_name = mssql_table_name, logger = logger)
        # Setup Snowflake (Créer file_format, stage et table)
        setup_snowflake(
            snowflake_database = snowflake_database,
            snowflake_schema = snowflake_schema, 
            mssql_table_name = "V_Inventory_Parts_Ops",
            snowflake_table_name = "AI_V_Inventory_Parts_Ops",
            logger = logger
        )
        result = upload_to_snowflake(
            snowflake_database = snowflake_database,
            snowflake_schema = snowflake_schema, 
            snowflake_table_name = "AI_V_Inventory_Parts_Ops",
            logger = logger
        )


        # Upload dans le staging (On a utilisé CSV mais peut être changé en parquet dans snowflake_dest.py)
        #upload_to_stage(logger = logger)
        # COPY INTO stage -> table
        #result = copy_into_table(table_name = snowflake_table_name, logger = logger)        
        # Durée totale
        total_duration = time.time() - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
        logger.info(f"🕒 Temps total Snowflake: {total_duration:.2f}s")
        logger.info(f"📊 Total lignes insérées: {result['rows_loaded']:,}")
        logger.info(f"⚡ Débit: {result['rows_loaded'] / total_duration:.0f} rows/sec")
        logger.info("=" * 80 + "\n")
        
        return result
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR PIPELINE: {e}")
        raise


if __name__ == "__main__":
    extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        mssql_table_name = "V_Inventory_Parts_Ops",
        snowflake_table_name = "AI_V_Inventory_Parts_Ops",
        logger = logger
    )