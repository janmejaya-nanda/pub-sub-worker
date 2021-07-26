import json
import logging

from google.cloud import pubsub_v1


# Initialize a Publisher client.
PROJECT_ID, TOPIC_ID = 'to-be-added', 'to-be-added'
publisher_client = pubsub_v1.PublisherClient()
topic_path = publisher_client.topic_path(PROJECT_ID, TOPIC_ID)
logger = logging.getLogger()


def publish_callback(api_future, data):
    """Show published message in the context of a callback function."""
    def log_published_message(api_future):
        try:
            logger.info("Published message:\n{} with message ID {}".format(data, api_future.result()))
        except Exception:
            logger.exception(
                "A problem occurred when publishing:\n{}\nException:{}".format(data, api_future.exception())
            )
            raise

    return log_published_message


def publish_message(message: dict):
    """
    Publish message to a Topic
    :param message: message dict, which needs to be published
    :return: None
    """
    api_future = publisher_client.publish(
        topic_path, data=json.dumps(message).encode('utf-8'))
    api_future.add_done_callback(publish_callback(api_future, message))
    logger.info("Published messages successfully!")
