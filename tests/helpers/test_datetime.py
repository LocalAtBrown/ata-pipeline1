from datetime import datetime, timedelta

import pytest

from ata_pipeline1.helpers.datetime import (
    TIMESTAMP_POSIX,
    AppliesDuringTimePeriod,
    ChangesBetweenTimePeriods,
)


class Component(AppliesDuringTimePeriod):
    """
    Dummy component that inherits from `AppliesDuringTimePeriod`.
    """

    def __init__(self, effective_starting: datetime) -> None:
        self.set_effective_starting(effective_starting)


@pytest.mark.unit
class TestAppliesDuringTimePeriod:
    def test_set_effective_starting(self) -> None:
        component = AppliesDuringTimePeriod()
        component.set_effective_starting(effective_starting=TIMESTAMP_POSIX)
        assert component.effective_starting == TIMESTAMP_POSIX


@pytest.mark.unit
class TestChangesBetweenTimePeriods:
    @pytest.fixture(scope="class")
    def timestamp_230101(self) -> datetime:
        return datetime(2023, 1, 1, 0, 0, 0)

    @pytest.fixture(scope="class")
    def component_zero(self) -> Component:
        return Component(effective_starting=TIMESTAMP_POSIX)

    @pytest.fixture(scope="class")
    def component_230101(self, timestamp_230101) -> Component:
        return Component(effective_starting=timestamp_230101)

    def test_set_components(self, component_zero, component_230101) -> None:
        thing = ChangesBetweenTimePeriods()
        thing.set_components(components=[component_zero, component_230101])

        assert thing.components[0] is component_zero
        assert thing.component_timestamps_float[0] == component_zero.effective_starting.timestamp()

        assert thing.components[1] is component_230101
        assert thing.component_timestamps_float[1] == component_230101.effective_starting.timestamp()

    def test_assign_component(self, timestamp_230101, component_zero, component_230101) -> None:
        thing = ChangesBetweenTimePeriods()
        thing.set_components(components=[component_zero, component_230101])

        assert thing.assign_component(TIMESTAMP_POSIX) is component_zero
        assert thing.assign_component(TIMESTAMP_POSIX + timedelta(microseconds=1)) is component_zero

        assert thing.assign_component(timestamp_230101 - timedelta(microseconds=1)) is component_zero
        assert thing.assign_component(timestamp_230101) is component_230101
        assert thing.assign_component(timestamp_230101 + timedelta(microseconds=1)) is component_230101
