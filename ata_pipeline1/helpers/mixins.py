from abc import ABC
from datetime import datetime
from typing import List

import numpy as np


class AppliesFromTimestamp(ABC):
    """
    Mixin to be added to a class (e.g., newsletter-submission validator by time period)
    whose logic only applies from a particular timestamp moving forward (e.g.,
    after a UI update from a partner's end).

    The class that uses this mixin should be in a list of components used by whichever
    class that uses the `ChangesBetweenTimestamps` mixin.
    """

    def __init__(self, *args, effective_starting: datetime, **kwargs) -> None:
        # Formality to make sure the next class after this one in the MRO of
        # whichever class that subclasses this one has its __init__ method called
        # (see: https://www.youtube.com/watch?v=X1PQ7zzltz4, 10:00 mark)
        super().__init__(*args, **kwargs)

        self._set_effective_starting(effective_starting)

    def _set_effective_starting(self, effective_starting: datetime) -> None:
        """
        Sets the timestamp.
        """
        self.effective_starting = effective_starting


class ChangesBetweenTimestamps(ABC):
    """
    Mixin to be added to a class (e.g., site newsletter-submission validator) whose
    logic changes from time period to time period (e.g., across different
    UI updates).
    """

    def __init__(self, *args, components: List[AppliesFromTimestamp], **kwargs) -> None:
        # Formality to make sure the next class after this one in the MRO of
        # whichever class that subclasses this one has its __init__ method called
        # (see: https://www.youtube.com/watch?v=X1PQ7zzltz4, 10:00 mark)
        super().__init__(*args, **kwargs)

        self._set_components(components)

    def _set_components(self, components: List[AppliesFromTimestamp]) -> None:
        """
        Sorts the input component list, then sets it as a class attribute.
        """
        self.components = sorted(components, key=lambda c: c.effective_starting)
        # Store POSIX floats of components' effective_starting timestamps
        self.component_timestamps_float = [c.effective_starting.timestamp() for c in self.components]

    def assign_component(self, timestamp: datetime) -> AppliesFromTimestamp:
        """
        Given a timestamp of class `datetime` (or any class that subclasses it, such
        as `pd.Timestamp`), returns the component whose `effective_starting` timestamp
        is right before it.

        This is essentially assigning said timestamp into an appropriate bin.
        """
        # Get component index (= bin index - 1)
        index = np.searchsorted(self.component_timestamps_float, timestamp.timestamp()) - 1

        return self.components[index]
