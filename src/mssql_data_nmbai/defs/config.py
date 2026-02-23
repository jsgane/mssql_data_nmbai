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


load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Config:
    USE_WSL = os.getenv("USE_WSL", "false").lower() == "true"
    # SQL Server
    MSSQL_SERVER = os.getenv("MSSQL_SERVER")
    MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
    MSSQL_USER = os.getenv("MSSQL_USER")
    MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
    
    # Query
    TABLE_NAME = "dbo.VLinkLocalisation"
    QUERY = f"SELECT TOP 10000000 * FROM {TABLE_NAME} WITH (NOLOCK)"
    
    # Fichier de sortie
    OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", "/tmp/mssql_export.csv"))    
    DELIMITER = ","
    
    # Snowflake
    SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "dg63583.eu-west-1")
    SF_USER = os.getenv("SNOWFLAKE_USER", "neemba_user")
    SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "Neemb@Password2025")
    SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "compute_wh")
    SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "NEEMBA")
    SF_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "EQUIPEMENT")
    SF_ROLE = os.getenv("SNOWFLAKE_ROLE", "transform")
    SF_TABLE = "a_bronze_vlinklocalisation"
    
    # Stage et File Format
    STAGE_NAME = "MSSQL_DIRECT_STAGE"
    FILE_FORMAT_NAME = "mssql_csv_file_format"
    
    # BCP executable path
    #BCP_PATH = r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe"
    BCP_PATH = r"/opt/mssql-tools/bin/bcp"

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
        logger.info(f"   Serveur: {self.server}")

    def export(self, query: str, output_path: Path, delimiter: str = ",") -> Tuple[bool, float, float]:
        """
        Export BCP SQL Server → CSV.
        Returns: Tuple (success, duration_seconds, file_size_MB)
        """
        start = time.time()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.use_wsl:
            # WSL: convertir le chemin
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
                "-c",
                "-t", delimiter,
                "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
            ]
        else:
            # Linux natif
            logger.info("💻 Mode natif")
            cmd = [
                self.bcp_path,
                query,
                "queryout",
                str(output_path),
                "-c",
                "-t", delimiter,
                "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
            ]

        # SSL: trust server certificate si activé
        if self.trust_server_certificate:
            cmd.extend(["-C", "1"])  # équivalent à TrustServerCertificate=yes

        # Masquer le mot de passe dans les logs
        cmd_display = cmd.copy()
        if "-P" in cmd_display:
            pwd_index = cmd_display.index("-P") + 1
            cmd_display[pwd_index] = "***"
        logger.info(f"🔄 Commande BCP: {' '.join(cmd_display)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            duration = time.time() - start

            if result.returncode != 0:
                logger.error(f"❌ BCP a échoué (code {result.returncode})")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)

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
        use_wsl=Config.USE_WSL,
        trust_server_certificate=True
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


def test_bcp_connection():
    """Tester la connexion BCP"""
    
    logger.info("🔍 Test de connexion BCP...")
    
    # Requête simple pour tester
    test_query = "SELECT @@VERSION"
    test_output = Path("/tmp/bcp_test.txt")
    
    exporter = BCPExporter(
        server=Config.MSSQL_SERVER,
        database=Config.MSSQL_DATABASE,
        username=Config.MSSQL_USER,
        password=Config.MSSQL_PASSWORD,
        use_wsl=Config.USE_WSL,
        trust_server_certificate=True
    )
    
    try:
        success, duration, size = exporter.export(
            query=test_query,
            output_path=test_output,
            delimiter=","
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
        export_mssql_bcp()