import json
import logging
import cobs_client
import os
from typing import Dict, Any, Union, Optional
from patroni.dcs import Cluster, ClusterConfig, TimelineHistory, Status, Member, Leader, Failover, SyncState

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
        try:
            self._ks.transact(lambda tx: tx.set('ttl', '30'))
        except Exception as e:
            logger.error(f"Failed to set TTL: {e}")

        # Set default paths
        self.initialize_path = '/service/initialize'
        self.config_path = '/service/config'
        self.members_path = '/service/members/'
        self.leader_path = '/service/leader'
        self.failover_path = '/service/failover'
        self.history_path = '/service/history'
        self.status_path = '/service/status'
        self.leader_optime_path = '/service/optime/leader'
        self.sync_path = '/service/sync'
        self.failsafe_path = '/service/failsafe'

    def set(self, key: str, value: Union[str, bytes]) -> bool:
        """Set a value for a given key in Cobs.

        :param key: The key to set the value for.
        :param value: The value to set, either as a string or bytes.

        :returns: ``True`` if the operation was successful.
        """
        logger.info(f"Setting key {key} with value {value}")
        try:
            return self._ks.transact(lambda tx: tx.set(key, value.encode('utf-8') if isinstance(value, str) else value))
        except Exception as e:
            logger.error(f"Failed to set key {key} with value {value}: {e}")
            return False

    def get(self, key: str) -> Optional[bytes]:
        """Get the value for a given key from Cobs.

        :param key: The key to retrieve the value for.

        :returns: The value as bytes if the key exists, otherwise ``None``.
        """
        logger.info(f"Getting value for key {key}")
        try:
            with self._ks.read() as snapshot:
                return snapshot.get(key).decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to get value for key {key}: {e}")
            return None

    def set_ttl(self, ttl: int) -> None:
        logger.info(f"Setting TTL to {ttl}")
        # Implement TTL setting logic here
        self.set('ttl', str(ttl))

    @property
    def ttl(self) -> int:
        # Implement logic to get current TTL
        return 30

    def set_retry_timeout(self, retry_timeout: int) -> None:
        logger.info(f"Setting retry timeout to {retry_timeout}")
        # Implement retry timeout setting logic here

    def touch_member(self, member_path: str, value: str) -> bool:
        logger.info(f"Touching member with data {value}")
        try:
            return self._ks.transact(lambda tx: tx.set(member_path, value.encode('utf-8')))
        except Exception as e:
            logger.error(f"Failed to touch member with data {value}: {e}")
            return False

    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        logger.info(f"Initializing with sysid {sysid}")
        try:
            self._ks.transact(lambda tx: tx.set(self.initialize_path, "true".encode('utf-8')))
            return True
        except Exception as e:
            logger.error(f"Failed to initialize with sysid {sysid}: {e}")
            return False

    def cancel_initialization(self) -> bool:
        logger.info("Canceling initialization")
        try:
            self._ks.transact(lambda tx: tx.delete(self.initialize_path))
            return True
        except Exception as e:
            logger.error(f"Failed to cancel initialization: {e}")
            return False

    def set_failover_value(self, value: str, version: Optional[Any] = None) -> bool:
        logger.info(f"Setting failover value {value}")
        try:
            self._ks.transact(lambda tx: tx.set(self.failover_path, value.encode('utf-8')))
            return True
        except Exception as e:
            logger.error(f"Failed to set failover value {value}: {e}")
            return False

    def set_config_value(self, value: str, version: Optional[Any] = None) -> bool:
        logger.info(f"Setting config value {value}")
        try:
            self._ks.transact(lambda tx: tx.set(self.config_path, value.encode('utf-8')))
            return True
        except Exception as e:
            logger.error(f"Failed to set config value {value}: {e}")
            return False

    def set_sync_state_value(self, value: str, version: Optional[Any] = None) -> Union[Any, bool]:
        logger.info(f"Setting sync state value {value}")
        try:
            self._ks.transact(lambda tx: tx.set(self.sync_path, value.encode('utf-8')))
            return value
        except Exception as e:
            logger.error(f"Failed to set sync state value {value}: {e}")
            return False

    def delete_sync_state(self, version: Optional[Any] = None) -> bool:
        logger.info("Deleting sync state")
        try:
            self._ks.transact(lambda tx: tx.delete(self.sync_path))
            return True
        except Exception as e:
            logger.error(f"Failed to delete sync state: {e}")
            return False

    def set_history_value(self, value: str) -> bool:
        logger.info(f"Setting history value {value}")
        try:
            self._ks.transact(lambda tx: tx.set(self.history_path, value.encode('utf-8')))
            return True
        except Exception as e:
            logger.error(f"Failed to set history value {value}: {e}")
            return False

    def delete_cluster(self) -> bool:
        logger.info("Deleting cluster")
        try:
            self._ks.transact(lambda tx: tx.delete(self.client_path('')))
            return True
        except Exception as e:
            logger.error(f"Failed to delete cluster: {e}")
            return False

    def write_failsafe(self, value: str) -> bool:
        """Write current cluster topology to DCS that will be used by failsafe mechanism (if enabled).

         :param value: failsafe topology serialized in JSON format.

         :returns: ``True`` if successfully committed to DCS.
         """
        logger.info(f"Writing failsafe topology {value}")
        try:
            self._ks.transact(lambda tx: tx.set(self.failsafe_path, value.encode('utf-8')))
            return True
        except Exception as e:
            logger.error(f"Failed to write failsafe topology {value}: {e}")
            return False


    def get_members(self) -> Dict[str, str]:
        logger.info("Getting members")

        # TODO: cobs_client does not support range reads right now so we have to
        # hardcode the possible member names here
        member_names = ["patroni1", "patroni2", "patroni3"]
        node_data = {}
        try:
            for member in member_names:
                node_data[member] = self.get(self.members_path + member)

            return node_data
        except Exception as e:
            logger.error(f"Failed to get members: {e}")
            return {}


    def cluster_loader(self, path: str) -> Cluster:
        """Load and build the :class:`Cluster` object from Cobs, which represents a single Patroni or Citus cluster.

        :param path: the path in Cobs where to load Cluster(s) from.

        :returns: :class:`Cluster` instance.
        """
        try:
            nodes = self.get_members()

            logger.info(f"NODES: {nodes}")

            # get initialize flag
            initialize = self.get(self.initialize_path)
            initialize = initialize and initialize.decode('utf-8')

            # get global dynamic configuration
            config = self.get(self.config_path)
            config = config and ClusterConfig.from_node(0, config.decode('utf-8'))

            # get timeline history
            history = self.get(self.history_path)
            history = history and TimelineHistory.from_node(0, history.decode('utf-8'))

            # get last known leader lsn and slots
            status = self.get(self.status_path) or self.get(self.leader_optime_path)
            status = Status.from_node(status and status.decode('utf-8'))

            # get list of members
            if nodes and nodes != {}:
                members = [Member.from_node(0, name, data.decode('utf-8')) for name, data in nodes.items()]

                # get leader
                leader = self.get(self.leader_path)
                if leader:
                    member = Member(-1, leader.decode('utf-8'), None, {})
                    member = ([m for m in members if m.name == leader.decode('utf-8')] or [member])[0]
                    leader = Leader(0, None, member)

                # failover key
                failover = self.get(self.failover_path)
                if failover:
                    failover = Failover.from_node(0, failover.decode('utf-8'))

                # get synchronization state
                sync = self.get(self.sync_path)
                sync = SyncState.from_node(0, sync and sync.decode('utf-8'))

                # get failsafe topology
                failsafe = self.get(self.failsafe_path)
                try:
                    failsafe = json.loads(failsafe.decode('utf-8')) if failsafe else None
                except Exception:
                    failsafe = None

                return Cluster(initialize, config, leader, status, members, failover, sync, history, failsafe)
            else:
                return Cluster.empty()
        except Exception as e:
            logger.error(f"Failed to load cluster from path {path}: {e}")
            return Cluster.empty()
