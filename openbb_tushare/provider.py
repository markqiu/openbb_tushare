"""openbb_tushare OpenBB Platform Provider."""

from openbb_core.provider.abstract.provider import Provider
from openbb_tushare.models.example import ExampleFetcher

# mypy: disable-error-code="list-item"

provider = Provider(
    name="openbb_tushare",
    description="Data provider for openbb_tushare.",
    # Only add 'credentials' if they are needed.
    # For multiple login details, list them all here.
    # credentials=["api_key"],
    website="https://openbb_tushare.com",
    # Here, we list out the fetchers showing what our provider can get.
    # The dictionary key is the fetcher's name, used in the `router.py`.
    fetcher_dict={
        "Example": ExampleFetcher,
    }
)
