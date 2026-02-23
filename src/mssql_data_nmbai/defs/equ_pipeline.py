import dagster as dagster
import dlt
from dlt.common import pendulum
from dlt.sources import incremental
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database
import urllib.parse
import os
from dlt.extract.resource import DltResource
from sqlalchemy import create_engine

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


##### EXTRACT V_facture_dashboard_am #############

@dlt.resource(
    name="V_facture_dashboard_am",
    write_disposition="replace",
)
def get_facture_data() -> DltResource:
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    )
    resource = source_sta.V_facture_dashboard_am
    
    return resource.parallelize()


@dlt.source
def facture_source():
    return get_facture_data

##### EXTRACT V_Equipment #############
@dlt.resource(
    name="V_Equipment",
    write_disposition="replace",
)
def get_quipment_data() -> DltResource:
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    )
    
    resource = source_sta.V_Equipment
    
    return resource.parallelize()


@dlt.source
def equipment_source():
    return get_quipment_data()

##### EXTRACT V_tiers_dashboard_am #############

@dlt.resource(
    name="V_tiers_dashboard_am",
    write_disposition="replace",
)
def get_tiers_data() -> DltResource:
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    )
    resource = source_sta.V_tiers_dashboard_am
    
    return resource.parallelize()


@dlt.source
def tiers_source():
    return get_tiers_data()

##### EXTRACT GCM_Retour_Données_OLGA #############

@dlt.resource(
    name="GCM_Retour_Données_OLGA",
    write_disposition="replace",
)
def get_gcm_retour_donnees_olga_data() -> DltResource:
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    )
    resource = source_sta.GCM_Retour_Données_OLGA
    
    return resource.parallelize()


@dlt.source
def gcm_retour_donnees_olga_source():
    return get_gcm_retour_donnees_olga_data()


##### EXTRACT v_Inventory_Parts_Ops #############

@dlt.resource(
    name="v_Inventory_Parts_Ops",
    write_disposition="replace",
)
def get_inventory_parts_ops_data() -> DltResource:
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    )
    resource = source_sta.v_Inventory_Parts_Ops
    
    return resource.parallelize()


@dlt.source
def inventory_parts_ops_source():
    return get_inventory_parts_ops_data()


##### EXTRACT V_devis_dashboard_am #############
@dlt.resource(
    name="V_devis_dashboard_am",
    write_disposition="replace",

)
def get_devis_data() -> DltResource:
    
    source = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    )

    resource = source.V_devis_dashboard_am

    resource.apply_hints(
        columns={
            "quantite_devis": {"data_type": "decimal", "precision": 14, "scale": 6},
            "quantite_restante_a_facturer": {"data_type": "decimal", "precision": 14, "scale": 6},
            "montant_vente_euros": {"data_type": "decimal", "precision": 18, "scale": 6},
            "gcd_quantite_commande": {"data_type": "decimal", "precision": 14, "scale": 6},
            "gcd_montant_vente_euros": {"data_type": "decimal", "precision": 18, "scale": 6},
            "gfd_quantite_facture": {"data_type": "decimal", "precision": 18, "scale": 6},
            "gfd_montant_vente_euros": {"data_type": "decimal", "precision": 18, "scale": 6},
        }
    )

    return resource.parallelize()


@dlt.source
def devis_source():
    return [get_devis_data()]


##### EXTRACT V_commande_dashboard_am #############

@dlt.resource(
    name="V_commande_dashboard_am",
    write_disposition="replace",
)
def get_commande_data() -> DltResource:
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    )
    resource = source_sta.V_commande_dashboard_am
    
    return resource.parallelize()


@dlt.source
def commande_source():
    return get_commande_data()


