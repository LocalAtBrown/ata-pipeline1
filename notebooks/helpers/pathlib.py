# These helpers should only be used by notebooks to ensure created/returned paths are correct

import os
from pathlib import Path

from ata_pipeline1.site.names import SiteName

PATH_DATA = Path(os.getcwd()) / "data"


def get_path_site_checkpoint(checkpoint: int, site: SiteName) -> Path:
    """
    Grabs a site's checkpoint pickled DataFrame.
    """
    path_parent = PATH_DATA / "checkpoints" / str(checkpoint)
    # Create parent if it doesn't exist, ignore otherwise
    path_parent.mkdir(parents=True, exist_ok=True)

    return path_parent / f"{site}.pkl"
