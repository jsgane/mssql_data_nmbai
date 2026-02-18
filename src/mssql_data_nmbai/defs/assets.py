import dagster as dg
from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
from mssql_data_nmbai.defs.equ_pipeline import equipment_source, devis_source, commande_source, facture_source
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

## Devis
@dlt_assets(
    dlt_source = devis_source(),
    dlt_pipeline = pipeline,
    name="V_devis_dashboard_am",
    group_name="data_for_nmbai",
    #kinds={"mssql", "snowflake", "dlthub"},
)
def devis_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

## Commande
@dlt_assets(
    dlt_source = commande_source(),
    dlt_pipeline = pipeline,
    name="V_commande_dashboard_am",
    group_name="data_for_nmbai",
    #kinds={"mssql", "snowflake", "dlthub"},
)
def commande_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
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
