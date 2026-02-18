import dagster as dagster
import dlt
from dlt.common import pendulum
from dlt.sources import incremental
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database
import urllib.parse
import os


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
        "MARS_Connection=yes;"   # allows parallel queries
        "Packet Size=32767;"     # larger network packets
        "Connection Timeout=60;"  # Increase timeout
        "Command Timeout=300;"    # Increase command timeout
    )
    conn_str_encoded = urllib.parse.quote_plus(conn_str)

    engine_url = f"mssql+pyodbc:///?odbc_connect={conn_str_encoded}&arraysize=50000"
    return engine_url



##### EXTRACT V_Equipment #############
@dlt.resource(
    name="V_Equipment",
    write_disposition="replace",
)
def get_quipment_data():
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    ).with_resources("V_Equipment").parallelize()
    
    return source_sta


@dlt.source
def equipment_source():
    return get_quipment_data()

##### EXTRACT V_devis_dashboard_am #############
@dlt.resource(
    name="V_devis_dashboard_am",
    write_disposition="replace",
)
def get_devis_data():
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    ).with_resources("V_devis_dashboard_am").parallelize()
    
    return source_sta


@dlt.source
def devis_source():
    return get_devis_data()


##### EXTRACT V_commande_dashboard_am #############

@dlt.resource(
    name="V_commande_dashboard_am",
    write_disposition="replace",
)
def get_commande_data():
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    ).with_resources("V_commande_dashboard_am").parallelize()
    
    return source_sta


@dlt.source
def commande_source():
    return get_commande_data()


##### EXTRACT V_facture_dashboard_am #############

@dlt.resource(
    name="V_facture_dashboard_am",
    write_disposition="replace",
)
def get_facture_data():
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
    ).with_resources("V_facture_dashboard_am").parallelize()
    
    return source_sta


@dlt.source
def facture_source():
    return get_facture_data()