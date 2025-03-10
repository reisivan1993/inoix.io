
import json
import logging


class IonixConsumer:
    def __init__(self, name: str):
        self.handlers = {}  # Dictionary to map event types to handler functions.
        self.logger = logging.getLogger(name)
        self.name = name

    def register_handler(self, event_type: str, handler_func):
        """Register a handler for a specific event type."""
        self.handlers[event_type] = handler_func
        return self

    def consume_message(self, raw_message: str):
        """
        Consume and process a raw JSON event message.
        The method decodes the message and dispatches it to the registered handler.
        If the JSON cannot be decoded or if the event_type is missing or Service not registered to the handler, an error is logged.
        """
        try:
            event = json.loads(raw_message)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode message: {e}")
            return

        event_type = event.get("event_type")
        if not event_type:
            self.logger.error("Event missing 'event_type'. Message ignored.")
            return

        handler = self.handlers.get(event_type)
        if handler:
            try:
                handler(event.get("payload"), event)
            except Exception as e:
                self.logger.error(f"Error in handler for {event_type}: {e}")
        else:
            self.logger.info(f"No handler registered for event type '{event_type}'. Message ignored.")