from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import carbon_intensity

defs = Definitions(
    assets=load_assets_from_modules([carbon_intensity]),
)