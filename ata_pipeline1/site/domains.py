import re
from dataclasses import dataclass, field


@dataclass
class SiteDomain:
    name: str  # Could be stricter than str if needed
    pattern: re.Pattern[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # re.escape escapes regex meta-characters like the period (".") in the URL
        self.pattern = re.compile(re.escape(self.name))


DOMAINS_AFRO_LA = [SiteDomain("afrolanews.org"), SiteDomain("afrolanews.beehiiv.com")]
DOMAINS_DALLAS_FREE_PRESS = [SiteDomain("dallasfreepress.com")]
DOMAINS_OPEN_VALLJO = [SiteDomain("openvallejo.org")]
DOMAINS_THE_19TH = [SiteDomain("19thnews.org")]
