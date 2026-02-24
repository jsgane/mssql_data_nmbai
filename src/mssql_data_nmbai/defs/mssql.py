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

    def export(
        self,
        table_name: str,
        output_path: Path,
        query: str = None,
        delimiter: str = ",",
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
                "-c",
                "-t", delimiter,
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
                "-c",
                "-t", delimiter,
                "-S", connection_string,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,    
            ]
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


def export_mssql_bcp(table_name: str, top_n: int = 10000000) -> bool:
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
        export_mssql_bcp(table_name = "dbo.v_Inventory_Parts_Ops")