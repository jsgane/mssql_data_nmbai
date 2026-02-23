from pathlib import Path

from dagster import Definitions, load_from_defs_folder, ScheduleDefinition, define_asset_job
from dagster_embedded_elt.dlt import DagsterDltResource
from mssql_data_nmbai.defs.assets import(
    equipment_dashboard_assets, 
    facture_dashboard_assets,
    tiers_dashboard_assets,
    #gcm_retour_donnees_olga_assets,
    inventory_parts_ops_assets,
)


#jobs
equipment_dashboard_job = define_asset_job(
    name="equipment_dashboard_job",
    selection=[equipment_dashboard_assets],
)

facture_dashboard_job = define_asset_job(
    name="facture_dashboard_job",
    selection=[facture_dashboard_assets],
)

tiers_dashboard_job = define_asset_job(
    name="tiers_dashboard_job",
    selection=[tiers_dashboard_assets],
)
'''
gcm_retour_donnees_olga_job = define_asset_job(
    name="gcm_retour_donnees_olga_job",
    selection=[gcm_retour_donnees_olga_assets],
)
'''
inventory_parts_ops_job = define_asset_job(
    name="inventory_parts_ops_job",
    selection=[inventory_parts_ops_assets],
)



#schedule : every day
equipment_dashboard_schedule = ScheduleDefinition(
    job=equipment_dashboard_job,
    cron_schedule="0 0 * * *", ## every day
)

facture_dashboard_schedule = ScheduleDefinition(
    job= facture_dashboard_job,
    cron_schedule="0 0 * * *", ## every day
)

tiers_dashboard_schedule = ScheduleDefinition(
    job= tiers_dashboard_job,
    cron_schedule="0 0 * * *", ## every day
)
'''
gcm_retour_donnees_olga_schedule = ScheduleDefinition(
    job= gcm_retour_donnees_olga_job,
    cron_schedule="0 0 * * *", ## every day
)
'''
inventory_parts_ops_schedule = ScheduleDefinition(
    job= inventory_parts_ops_job,
    cron_schedule="0 0 * * *", ## every day
)


defs = Definitions(
    jobs= [equipment_dashboard_job,facture_dashboard_job,tiers_dashboard_job,inventory_parts_ops_job],
    assets=[equipment_dashboard_assets,facture_dashboard_assets,tiers_dashboard_assets,inventory_parts_ops_assets],
    resources={
        "dlt":DagsterDltResource(),
    },
    schedules = [equipment_dashboard_schedule,facture_dashboard_schedule,tiers_dashboard_schedule,inventory_parts_ops_schedule]
)


