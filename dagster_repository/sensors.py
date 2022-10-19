from dagster import AssetKey, asset_sensor

from dagster_repository.jobs import materialize_ticker_meta_job


@asset_sensor(
    name=f"on_ticker_update_sensor",
    asset_key=AssetKey("ticker"),
    job=materialize_ticker_meta_job,
    minimum_interval_seconds=10,
)
def on_ticker_update_sensor(context, asset_event):
    yield materialize_ticker_meta_job.run_request_for_partition(
        partition_key=asset_event.dagster_event.partition,
        run_key=asset_event.dagster_event.partition,
    )
