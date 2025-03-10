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
    # Abstract poll method; subclasses must implement this.
    @abstractmethod
    def poll(self, timeout: int):
        raise NotImplementedError


class SimulatedQueue(Queue):
    def poll(self, timeout: int):
        """
        Simulates waiting for a message from the queue with a timeout.
        Depending on a random number, this method may:
          - Raise a PollingException,
          - Return a valid JSON string representing an ACCOUNT_CREATED or USER_ADDED event,
          - Raise a RuntimeError,
          - Or return None to indicate no message is available.
        """
        try:
            # Simulate waiting time (in seconds)
            time.sleep(random.randint(0, 2000) / 1000)
        except Exception:
            pass  # Ignore any sleep errors

        random_number = random.randint(0, 99) % 5
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
        # When random_number == 4, return None to indicate no message available.
        return None


# -------------------------------
# Consumer with Chainable API Using Generators
# -------------------------------

class IonixConsumer:
    def __init__(self, event_stream=None):
        """
        :param event_stream: A generator yielding raw JSON messages.
                             (Optional; if not provided, the consumer will poll the queue directly.)
        """
        self.handlers = {}  # Maps event types to their handler functions.
        self.event_stream = event_stream

    def register_event(self, event_handlers: dict):
        """
        Registers event handlers via a dictionary where keys are event types.
        Each handler should accept two parameters: the event payload and the full event.
        Returns self to allow chainable calls.
        """
        self.handlers.update(event_handlers)
        return self

    def set_event_stream(self, event_stream):
        """
        Sets or updates the event stream (a generator yielding events).
        Returns self to allow chaining.
        """
        self.event_stream = event_stream
        return self

    def _handle_event(self, raw_event: str):
        """
        Internal helper method.
        Decodes the JSON event and dispatches it to the registered handler based on event_type.
        """
        try:
            event = json.loads(raw_event)
        except json.JSONDecodeError as e:
            print(f"Error decoding event: {e}")
            return

        event_type = event.get("event_type")
        handler = self.handlers.get(event_type)
        if handler:
            try:
                handler(event.get("payload"), event)
            except Exception as e:
                print(f"Error in handler for {event_type}: {e}")
        else:
            print(f"No handler registered for event type: {event_type}")

    def consume_event(self, raw_event: str = None, poll_times: int = 10, timeout: int = 1000):
        """
        Consumes events either from a provided raw_event, from an event stream generator, or by polling the queue.

        Modes of Operation:
          1. If a raw_event (JSON string) is provided, it is processed immediately.
          2. Else, if an event_stream generator has been set via set_event_stream(), it iterates over the generator,
             handling each event as it is yielded.
          3. If neither a raw_event nor an event_stream is provided, the method creates a new instance of
             SimulatedQueue and polls it poll_times times (with the given timeout). Each valid event is handled.

        This method returns self to support chainable calls. For example:
            consumer.register_event({...}).consume_event()
        """
        if raw_event:
            # Mode 1: Process the provided raw event.
            self._handle_event(raw_event)
        elif self.event_stream:
            # Mode 2: Process events yielded by the event stream generator.
            for event in self.event_stream:
                self._handle_event(event)
        else:
            # Mode 3: Poll the SimulatedQueue for events.
            queue_instance = SimulatedQueue()
            for _ in range(poll_times):
                try:
                    result = queue_instance.poll(timeout)
                    if result:
                        self._handle_event(result)
                    else:
                        print("No message available at this poll.")
                except PollingException as pe:
                    print(f"PollingException occurred: {pe}")
                except RuntimeError as re:
                    print(f"RuntimeError occurred: {re}")
        return self  # Enable chainable API calls.


# -------------------------------
# Demonstration of Usage with the SimulatedQueue
# -------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Option 1: Directly poll the SimulatedQueue within consume_event()
    print("=== Direct Polling Mode ===")
    consumer_direct = IonixConsumer()
    consumer_direct.register_event({
        "ACCOUNT_CREATED": lambda payload, event: print(f"Account created with id: {payload.get('id')}"),
        "USER_ADDED": lambda payload, event: print(f"User added with username: {payload.get('username')}")
    }).consume_event(poll_times=10, timeout=1000)


    # Option 2: Use a generator that polls the SimulatedQueue and yields events.
    def event_generator(poll_times=10, timeout=1000):
        """
        Generator function that polls the SimulatedQueue poll_times times and yields valid events.
        Exceptions are caught and logged.
        """
        queue_instance = SimulatedQueue()
        for _ in range(poll_times):
            try:
                result = queue_instance.poll(timeout)
                if result:
                    yield result
                else:
                    print("No message available at this poll (generator).")
            except PollingException as pe:
                print(f"PollingException (generator) occurred: {pe}")
            except RuntimeError as re:
                print(f"RuntimeError (generator) occurred: {re}")


    print("\n=== Generator-Based Mode ===")
    # Create a consumer that uses the event stream from the generator.
    consumer_gen = Consumer(event_generator(poll_times=10, timeout=1000))
    consumer_gen.register_event({
        "ACCOUNT_CREATED": lambda payload, event: print(f"Account created with id: {payload.get('id')}"),
        "USER_ADDED": lambda payload, event: print(f"User added with username: {payload.get('username')}")
    }).consume_event()
