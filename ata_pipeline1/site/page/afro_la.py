from datetime import datetime

from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent

# Original AfroLA component
# TODO: Rules
COMPONENT_ZERO = SitePageClassifierComponent(effective_starting=TIMESTAMP_POSIX)

# Pre-launch period
# TODO: Hour, minutes, seconds & rules
COMPONENT_221215 = SitePageClassifierComponent(effective_starting=datetime(2022, 12, 15))

# Post-launch period
# TODO: Rules
COMPONENT_230201 = SitePageClassifierComponent(effective_starting=datetime(2023, 2, 1))

# When AfroLA rules change, add a new component here (create a new child class of SitePageClassifierComponent
# as necessary) and add it to the list of components in CLASSIFIER

CLASSIFIER = SitePageClassifier([COMPONENT_ZERO, COMPONENT_221215])
