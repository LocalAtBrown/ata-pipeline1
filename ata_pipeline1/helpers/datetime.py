from abc import ABC
from datetime import datetime
from typing import Sequence

import numpy as np

TIMESTAMP_POSIX = datetime(1970, 1, 1)


# ---------- MIXINS ----------
class AppliesDuringTimePeriod(ABC):
    """
    Mixin to be added to a class (e.g., newsletter-submission validator by time period)
    whose logic only applies from a particular timestamp moving forward (e.g.,
    after a UI update from a partner's end).

    The class that uses this mixin should be in a list of components used by whichever
    class that uses the `ChangesBetweenTimePeriods` mixin.
    """

    def set_effective_starting(self, effective_starting: datetime) -> None:
        """
        Sets the timestamp.
        """
        self.effective_starting = effective_starting


class ChangesBetweenTimePeriods(ABC):
    """
    Mixin to be added to a class (e.g., site newsletter-submission validator) whose
    logic changes from time period to time period (e.g., across different
    UI updates).
    """

    def set_components(self, components: Sequence[AppliesDuringTimePeriod]) -> None:
        """
        Sorts the input component list, then sets it as a class attribute.
        """
        self.components = sorted(components, key=lambda c: c.effective_starting)
        # Store POSIX floats of components' effective_starting timestamps
        self.component_timestamps_float = [c.effective_starting.timestamp() for c in self.components]

    def assign_component(self, timestamp: datetime) -> AppliesDuringTimePeriod:
        """
        Given a timestamp of class `datetime` (or any class that subclasses it, such
        as `pd.Timestamp`), returns the component whose `effective_starting` timestamp
        is right before it.

        This is essentially assigning said timestamp into an appropriate bin.
        """
        # Get component index (= bin index - 1)
        index = np.searchsorted(self.component_timestamps_float, timestamp.timestamp(), side="right") - 1

        return self.components[index]
