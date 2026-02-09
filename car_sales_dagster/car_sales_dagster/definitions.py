from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource, dbt_assets

from .assets import postgres_to_s3, s3_to_rds
from .project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def car_sales_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


car_sales_job = define_asset_job(
    name="car_sales_full_pipeline",
    selection=[postgres_to_s3, s3_to_rds, car_sales_dbt_assets],
)

car_sales_schedule = ScheduleDefinition(
    name="car_sales_schedule",
    job=car_sales_job,
    cron_schedule="0 6,18 * * *",  # 6:00 AM and 6:00 PM UTC
)

defs = Definitions(
    assets=[postgres_to_s3, s3_to_rds, car_sales_dbt_assets],
    jobs=[car_sales_job],
    schedules=[car_sales_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=dbt_project.profiles_dir,
        ),
    },
)
