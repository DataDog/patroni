import logging
import cobs_client
from typing import Dict, Any, Union

# from patroni.dcs import AbstractDCS
from patroni.config import Config

logger = logging.getLogger(__name__)

class Cobs():
    """
    A class to manage distributed coordination using the Cobs service.
    """

    def __init__(self) -> None:
        """
        Initialize the CobsDCS client with the given configuration.

        :param config: A dictionary containing configuration parameters.
        """
        logger.info(f"Connecting to Cobs at host.docker.internal:8080")
        self._ks = cobs_client.sync.open("cobs://host.docker.internal:8080/patroni_config?auth=off")

    def initialize(self, init_path: str):
        logger.info(f"Initializing Cobs with path {init_path}")
        return self._ks.transact(lambda tx: tx.set(init_path, bytes("true", 'utf-8')))

    def cancel_initialization(self, init_path: str):
        logger.info(f"Canceling initialization of Cobs with path {init_path}")
        return self._ks.transact(lambda tx: tx.delete(init_path))

    def set(self, key: str, value: Union[str, bytes]):
        logger.info(f"Setting key {key} with value {value}")
        return self._ks.transact(lambda tx: tx.set(key, value))

    def get(self, key: str):
        logger.info(f"Getting value for key {key}")
        with self._ks.read() as snapshot:
            return snapshot.get(key)
