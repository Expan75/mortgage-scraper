import itertools
from typing import List, Optional
from dataclasses import dataclass
import numpy as np


@dataclass
class MortgageMarketSegment:
    asset_value: float
    loan_amount: float

    # not always sue#
    period: Optional[str] = None

    @property
    def ltv(self) -> float:
        return self.loan_amount / self.asset_value


def generate_segments(period: Optional[str] = None) -> List[MortgageMarketSegment]:
    """
    Determines the bins to be used for query formatting

    Note that while we're always interested in loan amount and ltv,
    API:s only accept actual loan amount and asset value (i.e. implicitely ltv)

    ltv = loan / asset <=> asset = loan/ltv

    This corresponds to a 2-dimensional market segment. Cardinality of cartesian
    ltv_bins x loan_amount_bins x (unique maturity periods) correponds to the number 
    of urls to be sent, forcing us to select bins modestlybins
    
    Bins below are selected to keep the number of segments and urls below 1 million.
    """

    loan_amount_bins = [
        *np.arange(50_000, 2_000_000, 50_000).tolist(),
        *np.arange(2_000_000, 5_000_000, 100_000).tolist(),
        *np.arange(5_000_000, 10_000_000, 250_000).tolist(),
    ]
    ltv_bins = np.arange(0.5, 1.0, 0.01).tolist()

    # infer asset values based off of this
    asset_value_bins = [
        vol / ltv for (ltv, vol) in itertools.product(ltv_bins, loan_amount_bins)
    ]

    segments = []
    for loan_amount in loan_amount_bins:
        for asset_value in asset_value_bins:
            segment = MortgageMarketSegment(asset_value, loan_amount, period)
            segments.append(segment)

    return segments


if __name__ == "__main__":
    pass
