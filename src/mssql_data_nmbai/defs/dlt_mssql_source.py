import dagster as dg
from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
from dlt.sources.sql_database import sql_database
from dlt.extract.resource import DltResource
from sqlalchemy import create_engine, event, pool
from sqlalchemy.engine import Engine
import urllib.parse
import os
import time
import logging
import unicodedata
import re

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

def create_dlt_source(
    table_name: str,
    max_retries: int = 3,
    retry_delay: int = 5
) -> DltResource:
    """
    Créer une source DLT mssql
    """

    for attempt in range(max_retries):
        try:
            engine = get_mssql_engine()
            
            source = sql_database(
                engine,
                backend="pyarrow",
                chunk_size=100_000,
                reflection_level="minimal",
                include_views=True,
                table_names=[table_name], 
            )
            
            # Récupérer la ressource spécifique
            resource = getattr(source, table_name)
            
            logger.info(f"✅ Source DLT créée pour {table_name}")
            return resource.parallelize()
            
        except Exception as e:
            logger.error(f"❌ Tentative {attempt + 1}/{max_retries} échouée pour {table_name}: {e}")
            
            if attempt < max_retries - 1:
                logger.info(f"⏳ Retry dans {retry_delay}s...")
                time.sleep(retry_delay)
                # Fermer l'engine pour éviter les connexions mortes
                if 'engine' in locals():
                    engine.dispose()
            else:
                raise Exception(f"Échec après {max_retries} tentatives pour {table_name}") from e


#####Resources

#####V_Equipment
@dlt.resource(
    name="V_Equipment",
    write_disposition="replace",            
)
def get_equipment_data() -> DltResource:
    resource = create_dlt_source("V_Equipment")
     # Apply hints pour les colonnes décimales
    resource.apply_hints(
        columns={
            "Eqcat_Part_Sales_Previous_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Labor_Sales_Previous_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Total_Sales_Previous_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Part_Opportunity_Previous_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Labor_Opportunity_Previous_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Total_Opportunity_Previous_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Part_Opportunity_Future_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Labor_Opportunity_Future_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Total_Opportunity_Future_12m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Boost_Previous_12m": {"data_type": "decimal", "precision": 38, "scale": 3},
            "Eqcat_Base_Previous_12_m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Eqcat_Base_Future_12_m": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Op_Cat_Labor_Hours": {"data_type": "decimal", "precision": 38, "scale": 0},
            "Op_Cat_Labor_Value": {"data_type": "decimal", "precision": 38, "scale": 0},
            "Op_Cat_Base": {"data_type": "decimal", "precision": 38, "scale": 0},
            "Op_Cat_Total_Value": {"data_type": "decimal", "precision": 38, "scale": 0},
            "Op_Cat_Confidence_Index_pctg": {"data_type": "decimal", "precision": 38, "scale": 0},
        }
    )

    return resource


@dlt.source
def equipment_source():
    return get_equipment_data()

##### V_facture_dashboard_am
@dlt.resource(
    name="V_facture_dashboard_am",
    write_disposition="replace",
)
def get_facture_data() -> DltResource:
    resource = create_dlt_source("V_facture_dashboard_am")
     # Apply hints pour les colonnes décimales
    resource.apply_hints(
        columns={
            "gfd_prix_unitaire_achat_euro": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_prix_unitaire_vente_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_quantite": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_montant_achat_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_montant_vente_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_montant_vente_devise_locale": {"data_type": "decimal", "precision": 38, "scale": 6},
        }
    )

    return resource


@dlt.source
def facture_source():
    return get_facture_data()

##### V_tiers_dashboard_am
@dlt.resource(
    name="V_tiers_dashboard_am",
    write_disposition="replace",
)
def get_tiers_data() -> DltResource:
    resource = create_dlt_source("V_tiers_dashboard_am")
     # Apply hints pour les colonnes décimales
    resource.apply_hints(
        columns={
            "gfd_prix_unitaire_achat_euro": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_prix_unitaire_vente_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_quantite": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_montant_achat_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_montant_vente_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_montant_vente_devise_locale": {"data_type": "decimal", "precision": 38, "scale": 6},
        }
    )

    return resource


@dlt.source
def tiers_source():
    return get_tiers_data()

##### GCM_Retour_Donnees_OLGA

@dlt.resource(
    name="GCM_Retour_Donnees_OLGA",
    write_disposition="replace",
)
def get_gcm_retour_donnees_olga_data() -> DltResource:
    return create_dlt_source("GCM_Retour_Donnees_OLGA")


@dlt.source
def gcm_retour_donnees_olga_source():
    return get_gcm_retour_donnees_olga_data()


##### v_Inventory_Parts_Ops
@dlt.resource(
    name="v_Inventory_Parts_Ops",
    write_disposition="replace",
)
def get_inventory_parts_ops_data() -> DltResource:
    resource = create_dlt_source("v_Inventory_Parts_Ops")
     # Apply hints pour les colonnes décimales
    resource.apply_hints(
        columns={
            "Age_Stock": {"data_type": "decimal", "precision": 38, "scale": 6},
            "Qte_En_Stock": {"data_type": "decimal", "precision": 38, "scale": 6},
        }
    )

    return resource


@dlt.source
def inventory_parts_ops_source():
    return get_inventory_parts_ops_data()

##### V_devis_dashboard_am
@dlt.resource(
    name="V_devis_dashboard_am",
    write_disposition="replace",
)
def get_devis_data() -> DltResource:
    resource = create_dlt_source("V_devis_dashboard_am")
    
    # Apply hints pour les colonnes décimales
    resource.apply_hints(
        columns={
            "quantite_devis": {"data_type": "decimal", "precision": 38, "scale": 6},
            "quantite_restante_a_facturer": {"data_type": "decimal", "precision": 38, "scale": 6},
            "montant_vente_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gcd_quantite_commande": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gcd_montant_vente_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_quantite_facture": {"data_type": "decimal", "precision": 38, "scale": 6},
            "gfd_montant_vente_euros": {"data_type": "decimal", "precision": 38, "scale": 6},
        }
    )
    
    return resource


@dlt.source
def devis_source():
    return get_devis_data()

##### V_commande_dashboard_am
@dlt.resource(
    name="V_commande_dashboard_am",
    write_disposition="replace",
)
def get_commande_data() -> DltResource:
    return create_dlt_source("V_commande_dashboard_am")


@dlt.source
def commande_source():
    return get_commande_data()