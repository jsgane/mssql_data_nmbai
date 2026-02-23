"""
Dagster Asset pour pipeline ultra-rapide
"""

from dagster import (
    asset, 
    AssetExecutionContext, 
    Output, 
    MetadataValue,
    Definitions,
    ScheduleDefinition,
)
from load_bcp_copy_into import run_pipeline, Config


@asset(
    name="mssql_bcp",
    group_name="DATA_FOR_NMB_AI",
    description=f"BCP direct: {Config.MAX_ROWS:,} rows via BCP + COPY INTO",
)
def mssql_bcp_asset(context: AssetExecutionContext) -> Output:
    """
    Asset Dagster inpired by the PowerShell script
    """
    
    context.log.info("🚀 Démarrage pipeline BCP")
    
    result = run_pipeline()
    
    context.log.info(f"✅ Pipeline terminé: {result['rows_loaded']:,} lignes")
    
    return Output(
        value=result,
        metadata={
            "rows_loaded": MetadataValue.int(result['rows_loaded']),
            "errors": MetadataValue.int(result['errors']),
            "duration_seconds": MetadataValue.float(result['duration']),
            "method": MetadataValue.text("BCP + COPY INTO"),
            "speed_rows_per_sec": MetadataValue.float(
                result['rows_loaded'] / result['duration']
            ),
        }
    )


# Schedule quotidien
daily_schedule = ScheduleDefinition(
    name="mssql_daily_bcp",
    target=mssql_bcp_asset,
    cron_schedule="0 0 * * *",  # 2h du matin
)


defs = Definitions(
    assets=[mssql_bcp_asset],
    schedules=[daily_schedule],
)