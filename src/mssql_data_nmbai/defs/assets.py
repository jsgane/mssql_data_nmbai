import dagster as dg
from dagster import AssetExecutionContext, RetryPolicy
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
import logging
from mssql_data_nmbai.defs.dlt_mssql_source import make_inventory_parts_ops_source,equipment_source, facture_source, tiers_source, gcm_retour_donnees_olga_source##, inventory_parts_ops_source, devis_source, commande_source
#from mssql_data_nmbai.defs.load_bcp_copy_into import run_pipeline, Config
from mssql_data_nmbai.defs.load_bcp_copy_into import Config, extract_mssql_data
# Pipeline DLT
pipeline = dlt.pipeline(
    pipeline_name="mssql_to_snowflake_pipeline",
    destination="snowflake",
    dataset_name="equipement",
    progress="log",
)

# Retry policy global
retry_policy = RetryPolicy(
    max_retries=3,
    delay=10,  # 10 secondes entre chaque retry
)


##### ASSETS USING BCP + COPY INTO
@dg.asset(
    name="v_Inventory_Parts_Ops",
    group_name="data_for_nmbai",
    description="Inventory Parts Ops from MSSQL → Snowflake via BCP + COPY INTO",
)
def inventory_parts_ops_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Inventory Parts Ops from MSSQL"""
    
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        mssql_table_name = "V_Inventory_Parts_Ops",
        snowflake_table_name = "AI_V_Inventory_Parts_Ops",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )


@dg.asset(
    name="V_Equipment",
    group_name="data_for_nmbai",
    description="Equipment from MSSQL → Snowflake via BCP + COPY INTO",
)
def equipment_dashboard_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Equipment from MSSQL"""
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        mssql_table_name = "V_Equipment",
        snowflake_table_name = "AI_V_Equipment",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  


@dg.asset(
    name="V_facture_dashboard_am",
    group_name="data_for_nmbai",
    description="Facture_dashboard_am from MSSQL → Snowflake via BCP + COPY INTO",
)
def facture_dashboard_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Facture_dashboard_am from MSSQL"""
    
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        mssql_table_name="V_facture_dashboard_am",
        snowflake_table_name="AI_V_facture_dashboard_am",
        logger=context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  

@dg.asset(
    name="V_tiers_dashboard_am",
    group_name="data_for_nmbai",
    description="Tiers_dashboard_am from MSSQL → Snowflake via BCP + COPY INTO",
)
def tiers_dashboard_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Tiers_dashboard_am from MSSQL"""
    
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        mssql_table_name="V_tiers_dashboard_am",
        snowflake_table_name="AI_V_tiers_dashboard_am",
        logger=context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  


@dg.asset(
    name="GCM_Retour_Donnees_OLGA",
    group_name="data_for_nmbai",
    description="GCM_Retour_Donnees_OLGA from MSSQL → Snowflake via BCP + COPY INTO",
)
def gcm_retour_donnees_olga_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """GCM_Retour_Donnees_OLGA from MSSQL"""
    
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        mssql_table_name="GCM_Retour_Donnees_OLGA",
        snowflake_table_name="AI_GCM_Retour_Donnees_OLGA",
        logger=context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  


###### ASSET USING DLT
##@dlt_assets(
##    dlt_source=inventory_parts_ops_source(),
##    dlt_pipeline=pipeline,
##    name="v_Inventory_Parts_Ops",
##    group_name="data_for_nmbai",
##    #retry_policy=retry_policy,
##)
##def inventory_parts_ops_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
##    """Inventory Parts Ops from MSSQL"""
##    try:
##        yield from dlt.run(context=context)
##    except Exception as e:
##        context.log.error(f"❌ Inventory asset failed: {e}")
##        raise

###@dlt_assets(
###    dlt_source=equipment_source(),
###    dlt_pipeline=pipeline,
###    name="V_Equipment",
###    group_name="data_for_nmbai",
###    op_tags={"priority": "high"},
###    #retry_policy=retry_policy,
###)
###def equipment_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """Equipment data from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ Equipment asset failed: {e}")
###        raise
###
###
###@dlt_assets(
###    dlt_source=facture_source(),
###    dlt_pipeline=pipeline,
###    name="V_facture_dashboard_am",
###    group_name="data_for_nmbai",
###    #retry_policy=retry_policy,
###)
###def facture_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """Facture data from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ Facture asset failed: {e}")
###        raise
###
###
###@dlt_assets(
###    dlt_source=tiers_source(),
###    dlt_pipeline=pipeline,
###    name="V_tiers_dashboard_am",
###    group_name="data_for_nmbai",
###    #retry_policy=retry_policy,
###)
###def tiers_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """Tiers data from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ Tiers asset failed: {e}")
###        raise
###
###
###@dlt_assets(
###    dlt_source=gcm_retour_donnees_olga_source(),
###    dlt_pipeline=pipeline,
###    name="GCM_Retour_Donnees_OLGA",
###    group_name="data_for_nmbai",
###    #retry_policy=retry_policy,
###)
###def gcm_retour_donnees_olga_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """GCM Retour Données OLGA from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ GCM asset failed: {e}")
###        raise


##@dlt_assets(
##    dlt_source=make_inventory_parts_ops_source(logging.getLogger(__name__)),
##    dlt_pipeline=pipeline,
##    name="v_Inventory_Parts_Ops",
##    group_name="data_for_nmbai",
##    #retry_policy=retry_policy,
##)
##def inventory_parts_ops_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
##    """Inventory Parts Ops from MSSQL"""
##    try:
##        dlt_source = make_inventory_parts_ops_source(context.log)
##        yield from dlt.run(context=context, dlt_source=dlt_source)
##    except Exception as e:
##        context.log.error(f"❌ Inventory asset failed: {e}")
##        raise



###  Cet asset utilise bcp + copy into
#@asset(
#    name="vlinklocalisation_bcp_fast",
#    group_name="DATA_FOR_NMB_AI",
#    description=f"BCP direct: {Config.MAX_ROWS:,} rows via BCP + COPY INTO",
#)
#def vlinklocalisation_bcp_asset(context: AssetExecutionContext) -> Output:
#    """
#    Asset Dagster inpired by the PowerShell script
#    """
#    
#    context.log.info("🚀 Démarrage pipeline BCP")
#    
#    result = run_pipeline()
#    
#    context.log.info(f"✅ Pipeline terminé: {result['rows_loaded']:,} lignes")
#    
#    return Output(
#        value=result,
#        metadata={
#            "rows_loaded": MetadataValue.int(result['rows_loaded']),
#            "errors": MetadataValue.int(result['errors']),
#            "duration_seconds": MetadataValue.float(result['duration']),
#            "method": MetadataValue.text("BCP + COPY INTO"),
#            "speed_rows_per_sec": MetadataValue.float(
#                result['rows_loaded'] / result['duration']
#            ),
#        }
#    )
