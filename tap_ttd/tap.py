"""ttd tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_ttd import streams


class Tapttd(Tap):
    """ttd tap class."""

    name = "tap-ttd"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate ttd API service",
        ),
        th.Property(
            "start_date",
            th.StringType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.StringType,
            description="The last record date to sync",
        ),
        th.Property(
            "manual",
            th.BooleanType,
            required=True,
            description="Indicate if this is a manual run",
        ),
        th.Property(
            "advertiser_id",
            th.StringType,
            required=True,
            description="The id of the advertiser to sync",
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.ttdStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.StandardStream(self),
        ]


if __name__ == "__main__":
    Tapttd.cli()
