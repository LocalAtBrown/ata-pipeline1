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
    """

    def __init__(self, effective_starting: datetime = datetime(1970, 1, 1, 0, 0, 0)) -> None:
        self.effective_starting = effective_starting


class ChangesBetweenTimestamps(ABC):
    """
    To be added to a class (e.g., site newsletter-submission validator) whose
    logic changes from time period to time period (e.g., across different
    UI updates).
    """

    def __init__(self, components: List[AppliesFromTimestamp]) -> None:
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
