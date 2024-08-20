from dagster import define_asset_job, AssetSelection


pipeline = define_asset_job(
    name="pipeline",
    selection=AssetSelection.all()
)