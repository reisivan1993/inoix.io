import random
import time
import json
import logging
from abc import ABC, abstractmethod

# -------------------------------
# Queue and SimulatedQueue Setup
# -------------------------------

class PollingException(Exception):
    pass

class Queue(ABC):
    @abstractmethod
    def poll(self, timeout: int):
        raise NotImplementedError


class SimulatedQueue(Queue):

    def poll(self, timeout: int):
        try:
            # Simulate waiting time (in seconds)
            time.sleep(random.randint(0, 2000) / 1000)
        except Exception:
            pass

        random_number = random.randint(0, 5)
        if random_number == 0:
            raise PollingException("Polling error. At lunch, please try again later")
        elif random_number == 1:
            # Simulate a valid ACCOUNT_CREATED message.
            message = {
                "version": "1.0",
                "event_type": "ACCOUNT_CREATED",
                "timestamp": "2025-02-25T12:34:56Z",
                "payload": {"id": random.randint(1000, 9999)}
            }
            return json.dumps(message)
        elif random_number == 2:
            raise RuntimeError("Encountered an unexpected error. Probably Mas Hachnasa got me this time.")
        elif random_number == 3:
            # Simulate a valid USER_ADDED message.
            message = {
                "version": "1.0",
                "event_type": "USER_ADDED",
                "timestamp": "2025-02-25T12:35:56Z",
                "payload": {"username": f"user_{random.randint(1, 100)}"}
            }
            return json.dumps(message)
        elif random_number == 4:
            # Simulate a valid USER_DELETED message.
            message = {
                "version": "1.0",
                "event_type": "USER_DELETED",
                "timestamp": "2025-02-27T12:36:56Z",
                "payload": {"username": f"user_{random.randint(1, 100)}"}
            }
            return json.dumps(message)
        # When random_number == 5, return None to indicate no message available.
        return None
