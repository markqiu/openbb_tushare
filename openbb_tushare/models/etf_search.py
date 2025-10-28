"""Tushare ETF Search Model."""

from typing import Any, Dict, List, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_search import (
    EquitySearchData,
    EquitySearchQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)
from pydantic import Field


class TushareETFSearchQueryParams(EquitySearchQueryParams):
    """Tushare ETF Search Query.

    Source: https://tushare.pro/document/2?doc_id=19
    """

    use_cache: bool = Field(
        default=True,
        description="Whether to use a cached request. The ETF data is cached.",
    )
    limit: Optional[int] = Field(
        default=10000,
        description=QUERY_DESCRIPTIONS.get("limit", ""),
    )


class TushareETFSearchData(EquitySearchData):
    """Tushare ETF Search Data."""


class TushareETFSearchFetcher(
    Fetcher[
        TushareETFSearchQueryParams,
        List[TushareETFSearchData],
    ]
):
    """Transform the query, extract and transform the data from the Tushare endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> TushareETFSearchQueryParams:
        """Transform the query."""
        return TushareETFSearchQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: TushareETFSearchQueryParams,  # pylint: disable=unused-argument
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the Tushare endpoint."""

        from openbb_tushare.utils.ts_etf_search import get_etf_symbols
        api_key = credentials.get("tushare_api_key") if credentials else ""

        return get_etf_symbols(query.use_cache, api_key=api_key).to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: TushareETFSearchQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[TushareETFSearchData]:
        """Transform the data to the standard format."""

        return [TushareETFSearchData.model_validate(d) for d in data]

