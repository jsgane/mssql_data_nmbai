import os
import subprocess
import time
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv
import logging
from config import Config
import os
import subprocess
import time
from pathlib import Path
import logging
from dotenv import load_dotenv
from typing import Optional, Tuple, List

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#####  CRÉATION SNOWFLAKE (File Format, Stage, Table)==============

def get_snowflake_connection():
    """Créer connexion Snowflake"""
    return snowflake.connector.connect(
        account=Config.SF_ACCOUNT,
        user=Config.SF_USER,
        password=Config.SF_PASSWORD,
        warehouse=Config.SF_WAREHOUSE,
        database=Config.SF_DATABASE,
        schema=Config.SF_SCHEMA,
        role=Config.SF_ROLE,
    )


def create_file_format(cursor):
    """
    Créer le format de fichier CSV s'il n'existe pas déjà --- on pourrait aussi utiliser parquet à la place
    Équivalent: CREATE OR REPLACE FILE FORMAT...
    """
    
    logger.info("🔧 Création du file format CSV...")
    
    sql = f"""
    CREATE FILE FORMAT IF NOT EXISTS {Config.FILE_FORMAT_NAME}
        TYPE = CSV
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', '')
        EMPTY_FIELD_AS_NULL = TRUE
    """
    
    cursor.execute(sql)
    logger.info(f"✅ File format {Config.FILE_FORMAT_NAME} créé")


def create_stage(cursor):
    """
    Créer le stage interne s'il n'existe pas déjà dans le schéma
    Équivalent: CREATE OR REPLACE STAGE...
    """
    
    logger.info("🔧 Création du stage...")
    
    sql = f"""
    CREATE STAGE IF NOT EXISTS {Config.STAGE_NAME}
        FILE_FORMAT = {Config.FILE_FORMAT_NAME}
    """
    
    cursor.execute(sql)
    logger.info(f"✅ Stage {Config.STAGE_NAME} créé")


def create_snowflake_table(
    cursor,
    database: str,
    schema: str,
    table_name: str,
    table_schema: Optional[str] = None,
    create_mode: str = "IF NOT EXISTS"
) -> None:
    """
    Créer une table Snowflake avec schéma personnalisable
    
    Args:
        cursor: Snowflake cursor
        database: Nom de la base de données
        schema: Nom du schéma
        table_name: Nom de la table
        table_schema: DDL des colonnes
        create_mode: Mode de création ("IF NOT EXISTS", "OR REPLACE", "")
    
    Examples:
        # Utilisation avec schéma par défaut
        create_snowflake_table(cursor, "NEEMBA", "EQUIPEMENT", "my_table")
        
        # Utilisation avec schéma personnalisé
        custom_schema = '''
            id BIGINT PRIMARY KEY,
            name VARCHAR(100),
            created_at TIMESTAMP_NTZ
        '''
        create_snowflake_table(
            cursor, 
            "NEEMBA", 
            "EQUIPEMENT", 
            "table_name",
            table_schema=custom_schema
        )
        
        # Mode OR REPLACE
        create_snowflake_table(
            cursor,
            "NEEMBA",
            "EQUIPEMENT",
            "table_name",
            create_mode="OR REPLACE"
        )
    """
    
    logger.info(f"🔧 Création de la table {database}.{schema}.{table_name}...")
        
    # Construire la requête SQL
    if create_mode == "IF NOT EXISTS":
        sql = f"""
            CREATE TABLE {create_mode} {database}.{schema}.{table_name} (
                {table_schema.strip()}
            )
        """
    else:
        sql = f"""
            CREATE {create_mode} TABLE "{database}"."{schema}"."{table_name}" (
                {table_schema.strip()}
            )
        """
    
    # Exécuter
    cursor.execute(sql)
    
    logger.info(f"✅ Table {table_name} créée avec succès")
    logger.info(f"   Mode: CREATE TABLE {create_mode}")
    logger.info(f"   Localisation: {database}.{schema}.{table_name}")


### Créer format de fichier, stage et table dans snowflake==============
def setup_snowflake(table_name: str, custom_schema: str = None):
    """
    Setup des objets Snowflake
    """
    
    logger.info("=" * 80)
    logger.info("🔧 Setup Snowflake")
    logger.info("=" * 80)
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        create_file_format(cursor)
        create_stage(cursor)
        if custom_schema == None:

            custom_schema_v_equ = '''
                EQCAT_SK NUMBER(38,0) NOT NULL,
                TPSD_SK NUMBER(38,0) NOT NULL,
                EQCAT_EQUR_SK NUMBER(38,0) NOT NULL,
                EQCAT_TIE_SK NUMBER(38,0) NOT NULL,
                FLAG_12R NUMBER(38,0) NOT NULL,
                
                DATE_RETOUR_CAT TIMESTAMP_NTZ,
                SEMAINE_RETOUR VARCHAR(2),
                ANNEE_SEMAINE_RETOUR VARCHAR(6),
                MOIS_RETOUR VARCHAR(2) NOT NULL,
                ANNEE_MOIS_RETOUR VARCHAR(6),
                TRIMESTRE_RETOUR VARCHAR(25) NOT NULL,
                ANNEE_TRIMESTREQ_RETOUR VARCHAR(7),
                NUM_MOIS_TRIMESTRE_RETOUR NUMBER(38,0),
                SEMESTRE_RETOUR VARCHAR(25) NOT NULL,
                ANNEE_RETOUR VARCHAR(25),
                
                EQCAT_SERIALNO VARCHAR(50) NOT NULL,
                EQCAT_SERIALNO_PREFIX VARCHAR(3),
                EQCAT_COSNTRUCTEUR VARCHAR(3) NOT NULL,
                EQCAT_MANUFACTURER_CODE VARCHAR(10),
                EQCAT_PRODUCT_FAMILY VARCHAR(50),
                EQCAT_PRODUCT_FAMILY_ABBREVIATION VARCHAR(5),
                EQCAT_MODEL VARCHAR(50),
                EQCAT_ENGINE_ARRANGEMENT VARCHAR(50),
                EQCAT_CUSTOMER_NUMBER VARCHAR(10),
                EQCAT_RA VARCHAR(4),
                EQCAT_CUSTOMER_NAME VARCHAR(100),
                EQCAT_PARENT_CUSTOMER_NUMBER VARCHAR(10),
                EQCAT_PARENT_CUSTOMER_NAME VARCHAR(100),
                EQCAT_DIVISION VARCHAR(5),
                EQCAT_INDUSTRY_VERTICAL VARCHAR(50),
                EQCAT_INDUSTRY VARCHAR(10),
                EQCAT_SALES_REP_NUMBER VARCHAR(30),
                EQCAT_SALES_REP_NAME VARCHAR(100),
                EQCAT_SALES_REP_TYPE VARCHAR(50),
                EQCAT_PRODUCT_SUPPORT_SEGMENTATION VARCHAR(10),
                EQCAT_PRINCIPLE_WORK_CODE_DESCRIPTION VARCHAR(50),
                EQCAT_APPLICATION_CODE_DESCRIPTION VARCHAR(30),
                EQCAT_TERRITORY_INDICATOR VARCHAR(30),
                EQCAT_CURRENT_EQUIPMENT VARCHAR(10),
                EQCAT_INCLUDED_EXCLUDED VARCHAR(30),
                EQCAT_REASON_FOR_EXCLUSION VARCHAR(100),
                EQCAT_CURRENT_ACTIVITY_INDICATOR VARCHAR(30),
                EQCAT_PARKED_STATUS VARCHAR(30),
                
                DATE_PARKED_SMU TIMESTAMP_NTZ,
                SEMAINE_PARKED_SMU VARCHAR(2),
                ANNEE_SEMAINE_PARKED_SMU VARCHAR(6),
                MOIS_TEXTE_PARKED_SMU VARCHAR(2) NOT NULL,
                ANNEE_MOIS_PARKED_SMU VARCHAR(6),
                TRIMESTRE_PARKED_SMU VARCHAR(25) NOT NULL,
                ANNEE_TRIMESTREQ_PARKED_SMU VARCHAR(7),
                NUM_MOIS_TRIMESTRE_PARKED_SMU NUMBER(38,0),
                SEMESTRE_PARKED_SMU VARCHAR(25) NOT NULL,
                ANNEE_PARKED_SMU VARCHAR(25),
                
                EQCAT_PARKING_DEALER_REGION VARCHAR(100),
                EQCAT_UTILIZATION_RATE FLOAT,
                EQCAT_SMU_TYPE VARCHAR(10),
                EQCAT_UTILIZATION_TYPE VARCHAR(30),
                EQCAT_SMU NUMBER(38,0),
                EQCAT_LAST_REPORTED_SMU NUMBER(38,0),
                
                IN_SERVICE_DATE TIMESTAMP_NTZ,
                IN_SERVICE_SEMAINE VARCHAR(2),
                IN_SERVICE_ANNEE_SEMAINE VARCHAR(6),
                IN_SERVICE_MOIS VARCHAR(2) NOT NULL,
                IN_SERVICE_ANNEE_MOIS VARCHAR(6),
                IN_SERVICE_TRIMESTRE VARCHAR(25) NOT NULL,
                IN_SERVICE_ANNEE_TRIMESTREQ VARCHAR(7),
                TPS_NUM_MOIS_TRIMESTRE NUMBER(38,0),
                TPS_SEMESTRE_TEXTE VARCHAR(25) NOT NULL,
                TPS_ANNEE_TEXTE VARCHAR(25),
                
                DATE_LAST_REPORTED_SMU TIMESTAMP_NTZ,
                SEMAINE_LAST_REPORTED_SMU VARCHAR(2),
                ANNEE_SEMAINE_LAST_REPORTED_SMU VARCHAR(6),
                MOIS_LAST_REPORTED_SMU VARCHAR(2) NOT NULL,
                ANNEE_MOIS_LAST_REPORTED_SMU VARCHAR(6),
                TRIMESTRE_LAST_REPORTED_SMU VARCHAR(25) NOT NULL,
                ANNEE_TRIMESTREQ_LAST_REPORTED_SMU VARCHAR(7),
                NUM_MOIS_TRIMESTRE_LAST_REPORTED_SMU NUMBER(38,0),
                SEMESTRE_LAST_REPORTED_SMU VARCHAR(25) NOT NULL,
                ANNEE_LAST_REPORTED_SMU VARCHAR(25),
                
                DATELAST_INVOICE_DATE TIMESTAMP_NTZ,
                SEMAINE_LAST_INVOICE_DATE VARCHAR(2),
                ANNEE_SEMAINE_LAST_INVOICE_DATE VARCHAR(6),
                MOIS_LAST_INVOICE_DATE VARCHAR(2) NOT NULL,
                ANNEE_MOIS_LAST_INVOICE_DATE VARCHAR(6),
                TRIMESTRE_LAST_INVOICE_DATE VARCHAR(25) NOT NULL,
                ANNEE_TRIMESTREQ_LAST_INVOICE_DATE VARCHAR(7),
                NUM_MOIS_TRIMESTRE_LAST_INVOICE_DATE NUMBER(38,0),
                SEMESTRE_LAST_INVOICE_DATE VARCHAR(25) NOT NULL,
                ANNEE_LAST_INVOICE_DATE VARCHAR(25),
                
                EQCAT_CONTRACT VARCHAR(30),
                EQCAT_DUPLICATE_SERIALNO VARCHAR(10),
                EQCAT_DUPLICATE_DEALER_REGION VARCHAR(100),
                EQCAT_CALCULATION_ERROR_MESSAGE VARCHAR(100),
                EQCAT_CODE_DEVISE VARCHAR(2) NOT NULL,
                
                EQCAT_PART_SALES_PREVIOUS_12M NUMBER(19,6),
                EQCAT_LABOR_SALES_PREVIOUS_12M NUMBER(19,6),
                EQCAT_TOTAL_SALES_PREVIOUS_12M NUMBER(19,6),
                EQCAT_PART_OPPORTUNITY_PREVIOUS_12M NUMBER(19,6),
                EQCAT_LABOR_OPPORTUNITY_PREVIOUS_12M NUMBER(19,6),
                EQCAT_TOTAL_OPPORTUNITY_PREVIOUS_12M NUMBER(19,6),
                EQCAT_PART_OPPORTUNITY_FUTURE_12M NUMBER(19,6),
                EQCAT_LABOR_OPPORTUNITY_FUTURE_12M NUMBER(19,6),
                EQCAT_TOTAL_OPPORTUNITY_FUTURE_12M NUMBER(19,6),
                
                EQCAT_RELATED_SERIAL_NUMBER VARCHAR(30),
                EQCAT_RELATED_MANUFACTURER_CODE VARCHAR(10),
                EQCAT_RELATED_MANUFACTURER_MODEL VARCHAR(30),
                EQCAT_TCH_FILE_LOAD VARCHAR(8000),
                EQCAT_CONNECTED_ASSET VARCHAR(200),
                EQCAT_GPS_LOCATION_REGION VARCHAR(200),
                
                EQCAT_DRA_SK NUMBER(38,0) NOT NULL,
                EQCAT_BOOST_PREVIOUS_12M NUMBER(24,3),
                EQCAT_PRODUCT_GROUP VARCHAR(100),
                EQCAT_PRODUCT_GROUP_CODE VARCHAR(50),
                EQCAT_MONTHLY_UTILIZATION NUMBER(19,6),
                EQCAT_BASE_PREVIOUS_12_M NUMBER(19,6),
                EQCAT_BASE_FUTURE_12_M NUMBER(19,6),
                EQCAT_DUPLICATE_SERIAL_NUMBER VARCHAR(50),
                EQCAT_DEALER_PRINCIPLE_WORK_CODE_DESCRIPTION VARCHAR(150),
                EQCAT_DEALER_APPLICATION_CODE_DESCRIPTION VARCHAR(150),
                EQCAT_PRODUCT_FAMILY_CODE VARCHAR(100),
                
                OP_CAT_SK NUMBER(38,0),
                OP_CAT_EQUR_SK NUMBER(38,0),
                OP_CAT_CUSTOMER_NAME VARCHAR(200),
                OP_CAT_CUSTOMER_NUMBER VARCHAR(200),
                OP_CAT_TIE_SK NUMBER(38,0),
                OP_CAT_RA VARCHAR(4),
                OP_CAT_DRA_SK NUMBER(38,0),
                OP_CAT_SERIAL_NUMBER VARCHAR(200),
                OP_CAT_SERIAL_NUMBER_PREFIX VARCHAR(200),
                OP_CAT_CONSTRUCTEUR VARCHAR(200),
                OP_CAT_CTR_SK NUMBER(38,0),
                OP_CAT_MODEL VARCHAR(200),
                OP_CAT_MODC_SK NUMBER(38,0),
                OP_CAT_CODE_DEVISE VARCHAR(2),
                OP_CAT_DEV_SK NUMBER(38,0),
                OP_CAT_SMCS_GROUP_CODE_DESCRIPTION VARCHAR(200),
                OP_CAT_SMCS_SUBGROUP_CODE_DESCRIPTION VARCHAR(200),
                OP_CAT_COMPONENT_CODE_DESCRIPTION VARCHAR(200),
                OP_CAT_JOB_CODE_DESCRIPTION VARCHAR(200),
                OP_CAT_LAST_REPORTED_SMU_DATE VARCHAR(20),
                OP_CAT_MODIFIER_CODE_DESCRIPTION VARCHAR(200),
                OP_CAT_WORK_APP_CODE_DESCRIPTION VARCHAR(200),
                OP_CAT_COMP_QTY NUMBER(38,0),
                OP_CAT_TARGET_SMU NUMBER(38,0),
                OP_CAT_TARGET_DATE VARCHAR(20),
                OP_CAT_FIRST_INTERVAL NUMBER(38,0),
                OP_CAT_NEXT_INTERVAL NUMBER(38,0),
                OP_CAT_LABOR_HOURS NUMBER(18,0),
                OP_CAT_LABOR_VALUE NUMBER(18,0),
                OP_CAT_BASE NUMBER(18,0),
                OP_CAT_TOTAL_VALUE NUMBER(18,0),
                OP_CAT_LEAD_SCORE NUMBER(38,0),
                OP_CAT_CONFIDENCE_INDEX_PCTG NUMBER(18,0),
                OP_CAT_CONTRACT VARCHAR(200),
                OP_CAT_STATE VARCHAR(200),
                OP_CAT_COUNTY VARCHAR(200),
                OP_CAT_POSTAL_CODE VARCHAR(200),
                OP_CAT_DIVISION VARCHAR(200),
                OP_CAT_TCH_FILE_LOAD VARCHAR(1200),
                TCH_CREATE_DATE DATE
    
            '''
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

        create_snowflake_table(
            cursor, 
            "NEEMBA", 
            "EQUIPEMENT", 
            table_name,
            table_schema=custom_schema_v_inv_part,
            #create_mode="OR REPLACE"
        )
        
    finally:
        cursor.close()
        conn.close()


# Upload du fichier dans le stage de snowflake avec la commande PUT==============

def upload_to_stage():
    """
    Upload du fichier vers le stage
    Équivalent: PUT file://... @STAGE AUTO_COMPRESS=TRUE
    """
    
    logger.info("=" * 80)
    logger.info("📤 Upload vers Snowflake Stage")
    logger.info("=" * 80)
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # PUT command (utiliser forward slashes)
        file_path = str(Config.OUTPUT_PATH).replace("\\", "/")
        
        sql_put = f"""
        PUT file://{file_path} @{Config.STAGE_NAME}
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE
        """
        
        logger.info(f"🔄 Upload de {Config.OUTPUT_PATH.name}...")
        start_time = time.time()
        
        cursor.execute(sql_put)
        
        duration = time.time() - start_time
        logger.info(f"✅ Upload terminé en {duration:.2f}s")
        
        # Lister les fichiers dans le stage
        cursor.execute(f"LIST @{Config.STAGE_NAME}")
        files = cursor.fetchall()
        logger.info(f"📁 Fichiers dans le stage: {len(files)}")
        
    finally:
        cursor.close()
        conn.close()


# COPY INTO des données dans la table finale==============

def copy_into_table(table_name: str):
    """
    Chargement final avec COPY INTO
    Équivalent: COPY INTO table FROM @STAGE...
    """
    
    logger.info("=" * 80)
    logger.info("📥 COPY INTO Snowflake")
    logger.info("=" * 80)
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        sql_copy = f"""
        COPY INTO {table_name}
        FROM @{Config.STAGE_NAME}
        FILE_FORMAT = (FORMAT_NAME = {Config.FILE_FORMAT_NAME})
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE = TRUE
        """
        
        logger.info(f"🔄 Chargement dans {table_name}...")
        start_time = time.time()
        
        cursor.execute(sql_copy)
        results = cursor.fetchall()
        
        duration = time.time() - start_time
        
        # Analyser les résultats
        total_rows = 0
        total_errors = 0
        
        for row in results:
            file_name = row[0]
            rows_loaded = row[2]
            errors = row[5]
            
            total_rows += rows_loaded
            total_errors += errors
            
            logger.info(f"   ✓ {file_name}: {rows_loaded:,} lignes")
        
        logger.info(f"✅ COPY INTO terminé en {duration:.2f}s")
        logger.info(f"   Lignes chargées: {total_rows:,}")
        logger.info(f"   Erreurs: {total_errors:,}")
        
        # Vérification finale
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        logger.info(f"📊 Total dans la table: {final_count:,}")
        
        return {
            'rows_loaded': total_rows,
            'errors': total_errors,
            'duration': duration
        }
        
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    setup_snowflake(table_name="AI_V_Inventory_Parts_Ops")
    upload_to_stage()
    copy_into_table(table_name="AI_V_Inventory_Parts_Ops")