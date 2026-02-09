from setuptools import find_packages, setup

setup(
    name="car_sales_dagster",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "dbt-postgres",
        "boto3",
        "psycopg2-binary",
    ],
)
