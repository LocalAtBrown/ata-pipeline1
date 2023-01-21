from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent

# Original The 19th* component
# TODO: Rules
COMPONENT_ZERO = SitePageClassifierComponent(effective_starting=TIMESTAMP_POSIX)

# When The 19th* rules change, add a new component here (create a new child class of SitePageClassifierComponent
# as necessary) and add it to the list of components in CLASSIFIER

CLASSIFIER = SitePageClassifier([COMPONENT_ZERO])
