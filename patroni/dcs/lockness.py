from lockness import SyncClient, Heartbeats, LockStatus, LockNessTimeoutError
from typing import Dict, Any

class LockNess:
    """
    A class to manage distributed locks using the LockNess service.

    This class provides methods to acquire locks and send heartbeats to maintain them.
    It uses a SyncClient to connect to the LockNess service.
    """
    def __init__(self, config: Dict[str, Any], ttl: float) -> None:
        """
        Initialize the LockNess client with the given configuration.

        :param config: A dictionary containing configuration parameters.
        """
        logger.info("TTL: ", ttl)
        connect_string = config.get('connect_string', 'host.docker.internal')
        logger.info(f"Connecting to LockNess at {connect_string}")
        self.client = SyncClient.connect(connect_string, port=9111,
                                         heartbeats=Heartbeats.auto(int(ttl / 3)),
                                         auth_n=False)
        self._lock_id = None

    def acquire_lock(self, target: str, owner: str, ttl: int) -> bool:
        """
        Attempt to acquire a lock for a specified target.

        :param target: The target resource to lock.
        :param owner: The owner of the lock.
        :param ttl: Time-to-live for the lock in seconds.
        :return: True if the lock is acquired, False otherwise.
        """
        logger.info(f"Acquiring lock for {target} with owner {owner} and ttl {ttl}")
        lock_id = self.client.request_lock(targets=[target], owner=owner,
                                           domain="postgres", ttl=ttl)
        logger.info(f"LOCK ID: {lock_id}")
        states = self.client.stream_lock_states(lock_id)

        self._lock_id = lock_id

        lock_state = None
        while lock_state is None or lock_state.status != LockStatus.Acquired:
            try:
                lock_state = states.recv(timeout_ms=1000)
                if lock_state is None:
                    continue
                if lock_state is not None and lock_state.status == LockStatus.Acquired:
                    return True

                return False
            except LockNessTimeoutError:
                logger.info("Waiting for lock...")

            logger.info(f"Lock state: {lock_state=}")

        return False

    def heartbeat(self) -> str | None:
        """
        Send a heartbeat to maintain the lock.

        :return: The lock ID if the heartbeat is successful, None otherwise.
        """
        if self._lock_id is None:
            return None

        try:
            self.client.send_heartbeat(self._lock_id)
        except Exception as e:
            logger.exception(f"Error sending heartbeat: {e}")

        return self._lock_id

    def release_lock(self) -> None:
        """
        Release the lock.
        """
        if self._lock_id is not None:
            self.client.release_lock(self._lock_id)
            self._lock_id = None
