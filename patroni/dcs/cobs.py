import logging
from cobs_client import CobsClient
from typing import Dict, Any, Union

from patroni.dcs import AbstractDCS

logger = logging.getLogger(__name__)

class CobsDCS(AbstractDCS):
    """
    A class to manage distributed coordination using the Cobs service.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the CobsDCS client with the given configuration.

        :param config: A dictionary containing configuration parameters.
        """
        super().__init__(config)
        self.client = CobsClient(config['host'], config['port'])
        self._base_path = config.get('base_path', '/')
        logger.info(f"Connected to Cobs at {config['host']}:{config['port']}")

    def client_path(self, path: str) -> str:
        """
        Construct the full client path.

        :param path: The path to append to the base path.
        :return: The full client path.
        """
        return f"{self._base_path}/{path}"

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        """
        Reload the configuration.

        :param config: The new configuration to apply.
        """
        self.client.reload_config(config)
        logger.info("Configuration reloaded.")
