from sqlalchemy import create_engine, event, pool, text
from sqlalchemy.engine import Engine
import urllib.parse
import os
import time
import logging
import unicodedata
import re
from typing import Dict, List, Tuple


logger = logging.getLogger(__name__)

def get_mssql_engine():

    driver   = os.getenv("MSSQL_DRIVER")
    server   = os.getenv("MSSQL_SERVER")
    port     = os.getenv("MSSQL_PORT")
    database = os.getenv("MSSQL_DATABASE")
    username = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=60;"
    )

    conn_str_encoded = urllib.parse.quote_plus(conn_str)

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={conn_str_encoded}",
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
        connect_args={"timeout": 60},
        fast_executemany=True,
    )

    return engine


## Mapping Mssql to SNOWFLAKE ==========================

def map_mssql_to_snowflake(
    sql_type: str,
    max_length: int = None
) -> str:
    """
    Convertit un type MsSQL en type Snowflake
    
    Args:
        sql_type: Type SQL Server (ex: 'varchar', 'int', 'datetime')
        max_length: Longueur max pour les types string
    
    Returns:
        Type Snowflake équivalent
    """
    
    # Normaliser le type (lowercase)
    sql_type = sql_type.lower()
    
    # Mapping des types
    type_mapping = {
        # Numeric
        'bit': 'BOOLEAN',
        'tinyint': 'NUMBER(3,0)',
        'smallint': 'NUMBER(5,0)',
        'int': 'NUMBER(10,0)',
        'bigint': 'NUMBER(19,0)',
        'decimal': 'NUMBER(38,6)',
        'numeric': 'NUMBER(38,6)',
        'money': 'NUMBER(19,4)',
        'smallmoney': 'NUMBER(10,4)',
        'float': 'FLOAT',
        'real': 'FLOAT',
        
        # Date/Time
        'date': 'DATE',
        'datetime': 'TIMESTAMP_NTZ',
        'datetime2': 'TIMESTAMP_NTZ',
        'smalldatetime': 'TIMESTAMP_NTZ',
        'datetimeoffset': 'TIMESTAMP_TZ',
        'time': 'TIME',
        
        # String
        'char': f'VARCHAR({max_length or 1})',
        'varchar': f'VARCHAR({max_length or 255})',
        'text': 'VARCHAR(16777216)',  # Snowflake max
        'nchar': f'VARCHAR({max_length or 1})',
        'nvarchar': f'VARCHAR({max_length or 255})',
        'ntext': 'VARCHAR(16777216)',
        
        # Binary
        'binary': 'BINARY',
        'varbinary': 'BINARY',
        'image': 'BINARY',
        
        # Other
        'uniqueidentifier': 'VARCHAR(36)',
        'xml': 'VARIANT',
        'geography': 'GEOGRAPHY',
        'geometry': 'GEOMETRY',
    }
    
    # Gérer les types avec longueur
    if sql_type in ['char', 'varchar', 'nchar', 'nvarchar']:
        if max_length == -1:  # MAX
            return 'VARCHAR(16777216)'
        elif max_length:
            return f'VARCHAR({max_length})'
        else:
            return 'VARCHAR(255)'  # Par défaut
    
    # Retourner le type mappé ou VARCHAR par défaut
    return type_mapping.get(sql_type, 'VARCHAR(500)')


## EXTRACTION DU SCHÉMA ==============
def extract_mssql_table_schema(table_name: str) -> List[Tuple[str, str]]:
    """
    Extrait le schéma d'une table SQL Server
    
    Args:
        table_name: Nom de la table (ex: "v_Inventory_Parts_Ops" ou "dbo.MyTable")
    
    Returns:
        Liste de tuples (column_name, snowflake_type)
    
    Example:
        schema = extract_mssql_table_schema("dbo.v_Inventory_Parts_Ops")
        # [('ID', 'NUMBER(10,0)'), ('Name', 'VARCHAR(100)'), ...]
    """
    
    logger.info(f"📋 Extraction du schéma: {table_name}")
    
    # Séparer schéma et table si nécessaire
    if '.' in table_name:
        schema_name, table_only = table_name.split('.', 1)
    else:
        schema_name = 'dbo'
        table_only = table_name
    
    engine = get_mssql_engine()
    
    # ✅ CORRECTION: Utiliser with + text()
    sql_query = text("""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema_name
          AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
    """)
    
    with engine.connect() as conn:
        result = conn.execute(
            sql_query,
            {"schema_name": schema_name, "table_name": table_only}
        )
        
        columns = []
        
        for row in result:
            col_name = row[0]
            sql_type = row[1]
            max_length = row[2]
            precision = row[3]
            scale = row[4]
            is_nullable = row[5]
            
            # Mapper le type
            if sql_type.lower() in ['decimal', 'numeric'] and precision and scale:
                snowflake_type = f'NUMBER({precision},{scale})'
            else:
                snowflake_type = map_mssql_to_snowflake(sql_type, max_length)
            
            # Ajouter NOT NULL si nécessaire
            if is_nullable == 'NO':
                snowflake_type += ' NOT NULL'
            
            columns.append((col_name, snowflake_type))
            
            #logger.info(f" {col_name}: {sql_type} → {snowflake_type}")
        
        if not columns:
            raise ValueError(
                f"❌ Table '{schema_name}.{table_only}' non trouvée ou vide"
            )
        
        logger.info(f"✅ {len(columns)} colonnes extraites")
        
        return columns


## Génére le schéma snowflake ==============

def generate_snowflake_ddl(
    table_name: str,
    snowflake_table: str,
    snowflake_database: str = "NEEMBA",
    snowflake_schema: str = "EQUIPEMENT"
) -> str:
    """
    Génère le DDL Snowflake complet
    
    Args:
        table_name: Nom table Mssql
        snowflake_table: Nom table Snowflake
        snowflake_database: Base Snowflake
        snowflake_schema: Schéma Snowflake
        add_metadata: Ajouter colonnes métadata (_LOADED_AT, etc.)
    
    Returns:
        DDL CREATE TABLE complet
    
    Example:
        ddl = generate_snowflake_ddl(
            "dbo.v_Inventory_Parts_Ops",
            "AI_V_INVENTORY_PARTS_OPS"
        )
        print(ddl)
    """
    
    logger.info(f"🔧 Génération DDL: {snowflake_table}")
    
    # Extraire le schéma
    columns = extract_mssql_table_schema(table_name)
    
    # Construire le DDL
    ddl_lines = [
        f"CREATE OR REPLACE TABLE {snowflake_database}.{snowflake_schema}.{snowflake_table} (",
    ]
    
    # Colonnes de la source
    for col_name, col_type in columns:
        ddl_lines.append(f"    {col_name} {col_type},")
    
    # Enlever la dernière virgule
    ddl_lines[-1] = ddl_lines[-1].rstrip(',')
    
    ddl_lines.append(")")
    
    ddl = "\n".join(ddl_lines)
    
    logger.info(f"✅ DDL généré ({len(columns)} colonnes)")
    
    return ddl


# ============================================================================
# FONCTION HELPER POUR DAGSTER
# ============================================================================

def get_table_ddl_for_dagster(table_name: str, snowflake_table: str) -> str:
    """
    Helper pour obtenir le DDL dans Dagster
    
    Usage dans bcp_assets.py:
        ddl = get_table_ddl_for_dagster(
            "dbo.v_Inventory_Parts_Ops",
            "AI_V_INVENTORY_PARTS_OPS"
        )
    """
    return generate_snowflake_ddl(table_name, snowflake_table)


# ============================================================================
# CLI / TESTS
# ============================================================================

if __name__ == "__main__":
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    ### Test 1: Extraction schéma
    ##print("\n" + "=" * 80)
    ##print("TEST 1: Extraction du schéma")
    ##print("=" * 80 + "\n")
    ##
    ##schema = extract_mssql_table_schema("dbo.v_Inventory_Parts_Ops")
    ##
    ##print("\nSchéma extrait:")
    ##for col_name, col_type in schema:
    ##    print(f"  {col_name:30} {col_type}")
    ##
    # Test 2: Génération DDL
    print("\n" + "=" * 80)
    print("TEST Génération DDL Snowflake")
    print("=" * 80 + "\n")
    
    ddl = generate_snowflake_ddl(
        "dbo.v_Inventory_Parts_Ops",
        "AI_V_INVENTORY_PARTS_OPS"
    )
    
    print(ddl)
    
   