from pathlib import Path

from dotenv import load_dotenv
from dagster_dbt import DbtProject

# Load .env from the project root so dbt subprocess gets PG_* vars
load_dotenv(Path(__file__).joinpath("..", "..", "..", ".env").resolve())

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "car_sales_dbt").resolve(),
    profiles_dir=Path(__file__).joinpath("..", "..", "..", "car_sales_dbt").resolve(),
)
dbt_project.prepare_if_dev()
