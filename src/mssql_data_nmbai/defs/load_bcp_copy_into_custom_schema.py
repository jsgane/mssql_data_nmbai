import os
import subprocess
import time
from pathlib import Path
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv
import logging
from config import Config, BCPExporter, export_mssql_bcp
##from mssql import export_mssql_bcp
from snowflake_dest import setup_snowflake,upload_to_stage,copy_into_table

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# PIPELINE COMPLET ================================================

def extract_mssql_data(
    mssql_table_name: str = "V_Inventory_Parts_Ops", 
    snowflake_table_name: str = "AI_V_Inventory_Parts_Ops", 
    table_schema: str = None
):
    """
    Exécution complète du pipeline
    Reproduction du script PowerShell
    """
    
    start_time = time.time()
    
    logger.info("\n" + "=" * 80)
    logger.info("🚀 PIPELINE MSSQL → SNOWFLAKE")
    logger.info("📤➡️❄️  Méthode: BCP + COPY INTO (PowerShell)")
    logger.info("=" * 80 + "\n")
    
    try:
        # 1. Export BCP
        export_mssql_bcp(table_name = table_name)
        # Setup Snowflake (Créer file_format, stage et table)
        setup_snowflake(table_name = target_name, custom_schema = table_schema)
        # Upload dans le staging (On a utilisé CSV mais peut être changé en parquet dans snowflake_dest.py)
        upload_to_stage()
        # COPY INTO stage -> table
        result = copy_into_table(table_name = target_name)        
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
    custom_schema_v_inv_part = ''' 
       Sequentiel_fifo NUMBER(38,0) NULL,
       Code_Societe VARCHAR(10) NOT NULL,
       Libelle_Societe VARCHAR(50) NOT NULL,
       Code_Agence VARCHAR(10) NULL,
       Libelle_Agence VARCHAR(50) NULL,
       Code_Constructeur VARCHAR(10) NOT NULL,
       Libelle_Constructeur VARCHAR(50) NOT NULL,
       Code_Produit VARCHAR(35) NULL,
       Libelle_Produit VARCHAR(200) NULL,
       Date_Entree_Stock TIMESTAMP_NTZ NULL,
       Valeur_Stock_Total_EUR NUMBER(38,4) NULL,
       Quantite_Allouee NUMBER(38,0) NULL,
       Quantite_Non_Allouee NUMBER(38,0) NULL,
       PMP_EUR NUMBER(25,2) NULL,
       Type_Stock VARCHAR(10) NOT NULL,
       Libelle_Type_Stock VARCHAR(50) NOT NULL,
       Return_Code NUMBER(38,0) NULL,
       Age_Stock NUMBER(17,0) NULL,
       Qte_En_Stock NUMBER(10,2) NULL,
       NB_Demands_12m NUMBER(38,0) NULL        
    '''
    table_schema = custom_schema_v_inv_part
    extract_mssql_data(
        table_name = "V_Inventory_Parts_Ops",
        target_name= "AI_V_Inventory_Parts_Ops", 
        table_schema = table_schema
    )