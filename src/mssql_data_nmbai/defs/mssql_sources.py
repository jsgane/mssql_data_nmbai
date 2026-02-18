import dlt
from dlt.sources.sql_database import sql_database

import urllib.parse
import os

from sqlalchemy import create_engine, text
import logging
logger = logging.getLogger(__name__)


### Get mssql connexion
def get_mssql_engine():

    driver   = os.getenv("MSSQL_DRIVER")
    server   = os.getenv("MSSQL_SERVER")
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
        "MARS_Connection=yes;"
        "Packet Size=32767;"
        "Connection Timeout=60;"
        "Command Timeout=300;"
    )

    conn_str_encoded = urllib.parse.quote_plus(conn_str)

    return f"mssql+pyodbc:///?odbc_connect={conn_str_encoded}"


##### Nb lines----

def get_nb_rows(db_source_name: str) -> int:
    #logger = dlt.current.logger
    logger.info(f"🔍 Executing row count for {db_source_name}")

    engine = create_engine(get_mssql_engine())

    query = text(f"SELECT COUNT(*) AS nb_rows FROM {db_source_name} (nolock)")

    with engine.connect() as conn:
        total = conn.execute(query).scalar()

    logger.info(f"✅ Total rows in {db_source_name}: {total:,}")
    
    return total


### Extract from mssql
def extract_from_mssql(
    db_source_name: str,
    log_every: int = 200_000
):
    #logger = dlt.current.logger

    # Nb rows
    total_rows = get_nb_rows(db_source_name)

    logger.info(f"🚀 Starting multi-thread extraction for {db_source_name}")

    extracted_rows = 0

    # DLT source multi-threads
    source = (
        sql_database(
            get_mssql_engine(),
            backend="pyarrow",
            chunk_size=300_000,
            reflection_level="minimal",
            include_views=True,
        ).with_resources(db_source_name).parallelize()
    )

    # Data stream in chunks 
    for chunk in source:

        chunk_size = len(chunk)
        extracted_rows += chunk_size

        # Progress %
        if extracted_rows % log_every < chunk_size:
            pct = (extracted_rows / total_rows) * 100 if total_rows else 0

            logger.info(
                f"📊 {db_source_name} progress: "
                f"{extracted_rows:,}/{total_rows:,} rows "
                f"({pct:.1f}%)"
            )

        yield chunk

    logger.info(
        f"✅ Finished extraction for {db_source_name}: "
        f"{extracted_rows:,}/{total_rows:,} rows loaded (100%)"
    )


#### Dlt resources

@dlt.resource(
    name="V_Equipment",
    write_disposition="replace",
)
def get_equipment_data():
    yield from extract_from_mssql("V_Equipment")


@dlt.source
def equipment_source():
    return get_equipment_data()



@dlt.resource(
    name="V_devis_dashboard_am",
    write_disposition="replace",
)
def get_devis_data():
    yield from extract_from_mssql("V_devis_dashboard_am")


@dlt.source
def devis_source():
    return get_devis_data()

@dlt.resource(
    name="V_commande_dashboard_am",
    write_disposition="replace",
)
def get_commande_data():
    yield from extract_from_mssql("V_commande_dashboard_am")


@dlt.source
def commande_source():
    return get_commande_data()


@dlt.resource(
    name="V_facture_dashboard_am",
    write_disposition="replace",
)
def get_facture_data():
    yield from extract_from_mssql("V_facture_dashboard_am")


@dlt.source
def facture_source():
    return get_facture_data()


