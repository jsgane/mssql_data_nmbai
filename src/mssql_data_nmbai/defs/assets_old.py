import dagster as dg
from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
from mssql_data_nmbai.defs.equ_pipeline import equipment_source, facture_source, tiers_source, gcm_retour_donnees_olga_source, inventory_parts_ops_source ##, devis_source, commande_source
#from mssql_data_nmbai.defs.mssql_sources import equipment_source, devis_source, commande_source, facture_source



pipeline = dlt.pipeline(
    pipeline_name = "facture_dashboard_am_pipeline",
    destination = "snowflake",
    dataset_name = "equipement",
    progress="log",
)

## Equipment
@dlt_assets(
    dlt_source = equipment_source(),
    dlt_pipeline = pipeline,
    name="V_Equipment",
    group_name="data_for_nmbai",
    #kinds={"mssql", "snowflake", "dlthub"},
)
def equipment_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


## Facture
@dlt_assets(
    dlt_source = facture_source(),
    dlt_pipeline = pipeline,
    name="V_facture_dashboard_am",
    group_name="data_for_nmbai",
    #kinds={"mssql", "snowflake", "dlthub"},
)
def facture_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


## tiers_source
@dlt_assets(
    dlt_source = tiers_source(),
    dlt_pipeline = pipeline,
    name="V_tiers_dashboard_am",
    group_name="data_for_nmbai",
    #kinds={"mssql", "snowflake", "dlthub"},
)
def tiers_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

## gcm_retour_donnees_olga_source
@dlt_assets(
    dlt_source = gcm_retour_donnees_olga_source(),
    dlt_pipeline = pipeline,
    name="GCM_Retour_Données_OLGA",
    group_name="data_for_nmbai",
    #kinds={"mssql", "snowflake", "dlthub"},
)
def gcm_retour_donnees_olga_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


## inventory_parts_ops_source
@dlt_assets(
    dlt_source = inventory_parts_ops_source(),
    dlt_pipeline = pipeline,
    name="v_Inventory_Parts_Ops",
    group_name="data_for_nmbai",
    #kinds={"mssql", "snowflake", "dlthub"},
)
def inventory_parts_ops_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)