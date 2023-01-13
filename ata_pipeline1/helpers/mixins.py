from abc import ABC
from collections.abc import Iterable
from datetime import datetime
from typing import List

import numpy as np


class AppliesFromTimestamp(ABC):
    """
    To be added to a class (e.g., newsletter-submission validator by time period)
    whose logic only applies from a particular timestamp moving forward (e.g.,
    after a UI update from a partner's end).

    The class that uses this mixin should be in a list of components used by whichever
    class that uses the `ChangesBetweenTimestamps` mixin.
    """

    def set_effective_starting(self, effective_starting: datetime = datetime(1970, 1, 1, 0, 0, 0)) -> None:
        """
        Sets the timestamp with Unix epoch as default.
        """
        self.effective_starting = effective_starting


class ChangesBetweenTimestamps(ABC):
    """
    To be added to a class (e.g., site newsletter-submission validator) whose
    logic changes from time period to time period (e.g., across different
    UI updates).
    """

    def set_components(self, components: List[AppliesFromTimestamp]) -> None:
        """
        Sorts the input component list, then sets it as a class attribute.
        """
        self.components = sorted(components, key=lambda c: c.effective_starting)

    def assign_components(self, timestamps: Iterable[datetime]) -> List[AppliesFromTimestamp]:
        """
        Given a list of datetime timestamps, returns a corresponding list of components
        where each component's `effective_starting` timestamp is right before the
        timestamp of the same index in the original list.

        This is essentially assigning timestamps into bins.
        """
        # Convert datetimes into POSIX floats to take advantage of NumPy
        values = [t.timestamp() for t in timestamps]
        bins = [c.effective_starting.timestamp() for c in self.components]

        # Get component indices (= bin indices - 1)
        indices = np.digitize(values, bins)

        return [self.components[i] for i in indices]
