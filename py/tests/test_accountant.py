from usageaccountant import UsageAccumulator, UsageType


def test_accountant() -> None:
    accumulator = UsageAccumulator(1000)
    accumulator.record("ASd", "ASd", 10.0, UsageType.BYTES)
