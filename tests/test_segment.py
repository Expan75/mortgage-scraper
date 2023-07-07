from mortgage_scraper.segment import generate_segments


def test_should_generate_segments():
    unique_ltvs = set()
    unique_vols = set()
    unique_asset_values = set()

    segments = generate_segments()

    assert len(segments) != 0
    for segment in segments:
        unique_ltvs.add(segment.ltv)
        unique_vols.add(segment.loan_amount)
        unique_asset_values.add(segment.asset_value)

        correct_ltv = segment.ltv == segment.loan_amount / segment.asset_value
        assert correct_ltv, "ltv ratio not sane"
        assert segment.loan_amount >= 50_000
        assert segment.asset_value >= 50_000

    assert len(unique_ltvs) > 1, "should have plenty of segments"
    assert len(unique_vols) > 1, "should have plenty of segments"
    assert len(unique_asset_values) > 1, "should have plenty of segments"
    assert len(segments) < 500_000, "should not have an insane amount of segments"
