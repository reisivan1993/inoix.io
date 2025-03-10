import logging

from ionix_consumer.IonixConsumer import IonixConsumer
from ionix_consumer.SimulatedQueue import SimulatedQueue, PollingException


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Client (Service) Handlers ---

    # ServiceA handlers
    def service_a_account_created_handler(payload, event):
        print(f"Service A handling ACCOUNT_CREATED: Account id {payload.get('id')} registered.")

    def service_a_user_deleted_handler(payload, event):
        print(f"Service A handling USER_DELETED: user id {payload.get('username')} deleted.")

    def service_a_user_added_handler(payload, event):
        print(f"Service A handling USER_ADDED: Username {payload.get('username')} added.event:{event}")

    # ServiceB handlers
    def service_b_account_created_handler(payload, event):
        print(f"Service B processing ACCOUNT_CREATED: Creating welcome package for account id {payload.get('id')}.")

    def service_b_user_added_handler(payload, event):
        print(f"Service B processing USER_ADDED: Notifying team of new user {payload.get('username')}.")

    # --- Create Consumers ---
    consumer1 = IonixConsumer("ServiceA")
    consumer2 = IonixConsumer("ServiceB")

    # Register handlers for each consumer.
    consumer1.register_handler("ACCOUNT_CREATED", service_a_account_created_handler)
    consumer1.register_handler("USER_ADDED", service_a_user_added_handler)
    consumer1.register_handler("USER_DELETED", service_a_user_deleted_handler)

    consumer2.register_handler("ACCOUNT_CREATED", service_b_account_created_handler)
    consumer2.register_handler("USER_ADDED", service_b_user_added_handler)

    # Create an instance of the simulated abstract queue.
    queue = SimulatedQueue()

    # Poll the queue repeatedly (simulate a continuous process)
    for _ in range(10):
        try:
            result = queue.poll(timeout=1000)
            if result:
                print(f"\nNew message received: {result}")
                # Both consumers process the same message.
                consumer1.consume_message(result)
                consumer2.consume_message(result)
            else:
                print("\nNo message available at this poll.")
        except PollingException as pe:
            print(f"\nPollingException occurred: {pe}")
        except RuntimeError as re:
            print(f"\nRuntimeError occurred: {re}")