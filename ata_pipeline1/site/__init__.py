from .names import SiteName
from .sites import AfroLa, DallasFreePress, OpenVallejo, The19th

# Site objects
AFRO_LA = AfroLa()
DALLAS_FREE_PRESS = DallasFreePress()
OPEN_VALLEJO = OpenVallejo()
THE_19TH = The19th()

SITES = {
    SiteName.AFRO_LA: AFRO_LA,
    SiteName.DALLAS_FREE_PRESS: DALLAS_FREE_PRESS,
    SiteName.OPEN_VALLEJO: OPEN_VALLEJO,
    SiteName.THE_19TH: THE_19TH,
}
