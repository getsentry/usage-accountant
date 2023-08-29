from usageaccountant import UsageAccumulator, UsageUnit


def test_accountant() -> None:
    accumulator = UsageAccumulator(1000)
    accumulator.record("ASd", "ASd", 10.0, UsageUnit.BYTES)
