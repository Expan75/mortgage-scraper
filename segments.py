import itertools
from typing import List, Tuple
from dataclasses import dataclass
import numpy as np



@dataclass
class MortgageMarketSegment:
    asset_value: float
    loan_amount: float

    @property
    def ltv(self) -> float:
        return self.loan_amount / self.asset_value


def generate_loan_ltv_bins() -> List[MortgageMarketSegment]:
    """
    Determines the bins to be used for query formatting

    Note that while we're always interested in loan amount and ltv,
    API:s only accept actual loan amount and asset value (i.e. implicitely ltv)
    
    ltv = loan_amount / asset_value <=> (1 / (ltv / loan_amount)) = asset_value

    """
    loan_amount_bins = np.arange(50_000, 10_000_000, 50_000)
    ltv_bins = np.arange(0.005, 1, 0.005)
    
    # infer asset values based off of this
    asset_value_bins = 1 / (ltv_bins / loan_amount_bins)
    segments = [
        MortgageMarketSegment(asset_value, loan_amount)
        for asset_value, loan_amount 
        in itertools.product(asset_value_bins, loan_amount_bins) 
    ]

    return segments


if __name__ == "__main__":
    generate_loan_ltv_bins()
