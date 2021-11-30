from abc import ABC
from typing import List, Union

import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
from ntiles.portals.base_portal import BaseDeltaPortal

from toolbox.db.read.reader import read_db_timeseries

from toolbox.db.read.query_constructor import QueryConstructor


class PricingPortal(BaseDeltaPortal, ABC):
    """
    pulls pricing from database
    """

    def __init__(self, assets: Union[pd.Series, List[int], str], search_by: str, start: str, end: str,
                 fields: List[str] = ['prccd'], schema: str = 'CRSP'):
        super().__init__(assets, pd.Period(start), pd.Period(end))
        self._search_by = search_by
        self._fields = fields
        self._schema = schema

        self._pricing = None
        self._get_pricing()

    @property
    def assets(self):
        return self._pricing.columns.tolist()

    @property
    def delta_data(self):
        """
        returns the delta of the data held by the portal
        :return: Index: Id, pd.Period; Columns: 'delta'; Values: data
        """
        return self._pricing.sort_index().pct_change(1).iloc[1:].fillna(0).replace([np.inf, -np.inf],
                                                                                   0)  # .clip(-.75, .75)

    @property
    def periods(self):
        return self._pricing.index.drop_duplicates().to_list()

    def _get_pricing(self):
        df = QueryConstructor().query_timeseries_table(self._schema + '.security_daily', assets=self._assets,
                                                       start_date=str(self._start), end_date=str(self._end),
                                                       search_by=self._search_by,
                                                       fields=self._fields).distinct().df.abs()

        cal = mcal.get_calendar('NYSE'
                                ).valid_days(start_date=self._start.to_timestamp(),
                                             end_date=self._end.to_timestamp()).to_period('D').to_series().tolist()

        self._pricing = df.unstack().reindex(cal)
        self._pricing.columns = self._pricing.columns.get_level_values(1)
