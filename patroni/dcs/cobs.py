import logging
import cobs_client
from typing import Dict, Any, Union

# from patroni.dcs import AbstractDCS
from patroni.config import Config

logger = logging.getLogger(__name__)

class CobsDCS():
    """
    A class to manage distributed coordination using the Cobs service.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the CobsDCS client with the given configuration.

        :param config: A dictionary containing configuration parameters.
        """
        self._ks = cobs_client.sync.open("cobs://host.docker.internal:8080/patroni_config")

    def initialize(self, create_new: bool = True, sysid: str = ""):
        return self.retry(self._client.put, self.initialize_path, sysid, create_revision='0' if create_new else None)
