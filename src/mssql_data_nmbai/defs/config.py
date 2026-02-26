import os
import subprocess
import time
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
import logging
#from bcp_wsl import BCPExporter
import logging
from dotenv import load_dotenv
from typing import Tuple, Optional
from typing import Dict, List, Tuple
import urllib.parse
from sqlalchemy import create_engine, event, pool, text
from sqlalchemy.engine import Engine


load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Config:
    USE_WSL = os.getenv("USE_WSL", "false").lower() == "true"
    # SQL Server
    MSSQL_SERVER = os.getenv("MSSQL_SERVER", r"bodsql\bi01")
    MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
    MSSQL_USER = os.getenv("MSSQL_USER")
    MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
    
    # Query
    TABLE_NAME = "v_Inventory_Parts_Ops"
    QUERY = f"SELECT TOP 10000000 * FROM {TABLE_NAME} WITH (NOLOCK)"
    
    # Fichier de sortie
    OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", "/tmp/mssql_export.csv"))    
    DELIMITER = "|"
    
    # Snowflake
    SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "dg63583.eu-west-1")
    SF_USER = os.getenv("SNOWFLAKE_USER", "neemba_user")
    SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "Neemb@Password2025")
    SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "compute_wh")
    SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "NEEMBA")
    SF_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "EQUIPEMENT")
    SF_ROLE = os.getenv("SNOWFLAKE_ROLE", "transform")
    
    # Stage et File Format
    FILE_FORMAT_NAME = "mssql_csv_file_format"
    STAGE_NAME = "MSSQL_DIRECT_STAGE"
    
    # BCP executable path
    #BCP_PATH = r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe"
    ###===========================================================================
    ### # Ajouter bcp (Ubuntu/Debian)
    ### curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    ### curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    ### sudo apt-get update
    ### sudo ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev
    ### 
    ### # Ajouter au PATH
    ### echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
    ### source ~/.bashrc


    BCP_PATH = r"/opt/mssql-tools/bin/bcp"


class BCPExporter:
    """Exporter BCP SQL Server → CSV, compatible WSL et Linux natif."""

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        use_wsl: bool = False,
        bcp_path: str = None,
        trust_server_certificate: bool = True,
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.use_wsl = use_wsl
        self.trust_server_certificate = trust_server_certificate

        # Chemin BCP par défaut
        if bcp_path is None:
            self.bcp_path = "/opt/mssql-tools/bin/bcp" if use_wsl else "bcp"
        else:
            self.bcp_path = bcp_path

        logger.info("🔧 BCPExporter initialisé:")
        logger.info(f"   Mode: {'WSL' if use_wsl else 'Natif'}")
        logger.info(f"   BCP: {self.bcp_path}")
        #logger.info(f"   Serveur: {self.server}")
    
    # ===EXPORT BCP (équivalent code PowerShell) ============
    def windows_to_wsl_path(windows_path: str) -> str:
        """Convertit un chemin Windows en chemin WSL (/mnt/c/... style)."""
        path = Path(windows_path)
        if ":" in str(path):
            drive, rest = str(path).split(":", 1)
            drive = drive.lower()
            rest = rest.replace("\\", "/").lstrip("/")
            return f"/mnt/{drive}/{rest}"
        else:
            return str(path)

    def export(
        self,
        table_name: str,
        output_path: Path,
        query: str = None,
        delimiter: str = "|",
        top_n: int = 10000000,
    ) -> Tuple[bool, float, float]:
        """
        Export BCP SQL Server → CSV à partir du nom de la table.
        - table_name: nom complet avec schéma (ex: v_Inventory_Parts_Ops)
        - output_path: chemin du fichier CSV
        - delimiter: séparateur CSV
        - top_n: nombre maximal de lignes à exporter
        Returns: Tuple (success, duration_seconds, file_size_MB)
        """
        # Construire la requête automatiquement
        if query == None:
            query = f"SELECT TOP {top_n} * FROM {table_name} WITH (NOLOCK)"

        start = time.time()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        #connection_string = r"bodsql\bi01" ##self.server  # ton serveur MSSQL
        server = self.server  # ton serveur MSSQL
        connection_string = f"{server};Encrypt=no;TrustServerCertificate=yes"
        if self.use_wsl:
            wsl_output = windows_to_wsl_path(str(output_path))
            logger.info("🐧 Mode WSL")
            logger.info(f"   Chemin Windows: {output_path}")
            logger.info(f"   Chemin WSL: {wsl_output}")

            cmd = [
                "wsl",
                self.bcp_path,
                query,
                "queryout",
                wsl_output,
                "-c",              # caractère standard
                "-C", "65001",     # UTF-8
                "-t", delimiter,
                "-r", "\\n",
                "-b", "100000",          # Batch de 100k rows
                "-a", "32767",           # Packet size max
                "-S", connection_string,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
            ]
        else:
            logger.info("💻 Mode natif")
            cmd = [
                self.bcp_path,
                query,
                "queryout",
                str(output_path),
                "-c",              # caractère standard
                "-C", "65001",     # UTF-8
                "-t", delimiter,
                "-r", "\n",
                "-b", "100000",          # Batch de 100k rows
                "-a", "32767",           # Packet size max
                "-S", connection_string,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,    
            ]
        # Masquer le mot de passe dans les logs
        cmd_display = cmd.copy()
        if "-S" in cmd_display:
            pwd_index = cmd_display.index("-S") + 1
            cmd_display[pwd_index] = "***"

        if "-P" in cmd_display:
            pwd_index = cmd_display.index("-P") + 1
            cmd_display[pwd_index] = "***"

        if "-U" in cmd_display:
            pwd_index = cmd_display.index("-U") + 1
            cmd_display[pwd_index] = "***"

        if "-d" in cmd_display:
            pwd_index = cmd_display.index("-d") + 1
            cmd_display[pwd_index] = "***"
        logger.info(f"🔄 Commande BCP: {' '.join(cmd_display)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            duration = time.time() - start

            if result.returncode != 0:
                logger.error(f"❌ BCP a échoué (code {result.returncode})")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, output=result.stdout, stderr=result.stderr
                )

            if not output_path.exists():
                raise FileNotFoundError(f"Le fichier de sortie n'a pas été créé: {output_path}")

            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"✅ Export BCP réussi: Durée {duration:.2f}s, Taille {size_mb:.2f} MB")
            return True, duration, size_mb

        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Erreur subprocess: code {e.returncode}")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"❌ Erreur inattendue: {e}")
            raise


def export_mssql_bcp(table_name: str, logger, top_n: int = 10000000) -> bool:
    """Export BCP depuis SQL Server avec support WSL."""
    
    logger.info("=" * 80)
    logger.info(f"📤 Export BCP depuis SQL Server pour la table {table_name}")
    logger.info("=" * 80)
    
    # Créer le répertoire de sortie si nécessaire
    Config.OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Supprimer le fichier existant
    if Config.OUTPUT_PATH.exists():
        Config.OUTPUT_PATH.unlink()
        logger.info(f"🗑️  Fichier existant supprimé: {Config.OUTPUT_PATH}")
    
    # Créer l'exporter BCP
    exporter = BCPExporter(
        server=f"{Config.MSSQL_SERVER}",
        database=Config.MSSQL_DATABASE,
        username=Config.MSSQL_USER,
        password=Config.MSSQL_PASSWORD,
        use_wsl=Config.USE_WSL,
        trust_server_certificate=True
    )
    
    logger.info(f"🔄 Exécution BCP...")
    start_time = time.time()
    
    try:
        # Export BCP (retourne success, durée, taille)
        success, bcp_duration, file_size_mb = exporter.export(
            table_name=table_name,
            output_path=Config.OUTPUT_PATH,
            delimiter=Config.DELIMITER,
            top_n=top_n
        )
        
        total_duration = time.time() - start_time
        
        # Log des informations
        logger.info(f"✅ Export BCP terminé en {total_duration:.2f}s")
        logger.info(f"   Temps BCP: {bcp_duration:.2f}s")
        logger.info(f"   Fichier: {Config.OUTPUT_PATH}")
        logger.info(f"   Taille: {file_size_mb:.2f} MB")
        
        return success
    
    except Exception as e:
        logger.error(f"❌ Erreur BCP: {e}")
        raise

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
                snowflake_type += ' NULL'#' NOT NULL'
            
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
    mssql_table_name: str,
    snowflake_table_name: str,
    snowflake_database: str = "NEEMBA",
    snowflake_schema: str = "EQUIPEMENT"
) -> str:
    """
    Génère le DDL Snowflake
    
    Args:
        mssql_table_name: Nom table Mssql
        snowflake_table: Nom table Snowflake
        snowflake_database: Base Snowflake
        snowflake_schema: Schéma Snowflake
    
    Returns:
        DDL CREATE TABLE
    
    Example:
        ddl = generate_snowflake_ddl(
            "dbo.v_Inventory_Parts_Ops",
            "AI_V_INVENTORY_PARTS_OPS"
        )
        print(ddl)
    """
    
    logger.info(f"🔧 Génération DDL: {snowflake_table_name}")
    
    # Extraire le schéma
    columns = extract_mssql_table_schema(mssql_table_name)
    
    # Construire le DDL
    ddl_lines = [
        f"CREATE OR REPLACE TABLE {snowflake_database}.{snowflake_schema}.{snowflake_table_name} (",
    ]
    
    # Colonnes de la source
    for col_name, col_type in columns:
        col_name = col_name.replace("é","e")
        col_name = col_name.replace("è","e")

        ddl_lines.append(f"    {col_name} {col_type},")
    
    # Enlever la dernière virgule
    ddl_lines[-1] = ddl_lines[-1].rstrip(',')
    
    ddl_lines.append(")")
    
    ddl = "\n".join(ddl_lines)
    
    logger.info(f"✅ DDL généré ({len(columns)} colonnes)\n {ddl}")
    
    return ddl

##### Tests
def test_bcp_connection():
    """Tester la connexion BCP"""
    
    logger.info("🔍 Test de connexion BCP...")
    
    # Requête simple pour tester
    test_query = "SELECT @@VERSION"
    test_output = Path("/tmp/bcp_test.txt")
    logger.info(f"{Config.MSSQL_SERVER}")

    exporter = BCPExporter(
        server=f"{Config.MSSQL_SERVER}",
        database=Config.MSSQL_DATABASE,
        username=Config.MSSQL_USER,
        password=Config.MSSQL_PASSWORD,
        use_wsl=Config.USE_WSL,
        trust_server_certificate=True
    )
    
    try:
        success, duration, size = exporter.export(
            table_name = "dbo.v_Inventory_Parts_Ops",
            query=test_query,
            output_path=test_output,
            delimiter="|"
        )
        
        logger.info("✅ Test de connexion BCP réussi!")
        
        # Afficher le contenu
        if test_output.exists():
            content = test_output.read_text()
            logger.info(f"Résultat: {content[:200]}")
            test_output.unlink()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test de connexion BCP échoué: {e}")
        return False


## EXÉCUTION============================================================

if __name__ == "__main__":
    # D'abord tester la connexion
    if test_bcp_connection():
        # Puis faire l'export complet
        export_mssql_bcp(table_name = "dbo.v_Inventory_Parts_Ops")