import logging
import multiprocessing
import time
import json
from abc import ABC, abstractmethod

from google.cloud import pubsub_v1
from google.api_core.exceptions import DeadlineExceeded


class PubSubWorker(ABC):
    def __init__(self, project_id: str, topic_id: str, subscriptions_with_priority: dict, service_json_path=None,
                 logger=None, **kwargs):
        """
        Abstract base class for all pub-sub worker. every subscriber should inherit this base class and implement
        their own subscriber method.
        :param project_id: str: Id of the project
        :param topic_id: str: unique id that represents topic
        :param subscriptions_with_priority: dict: A dictionary containing subscription id as key and corresponding
        priority in values. Note: priority should start with zero(which is considered as highest priority).
        e.g. {"subscription_id1": 0, "subscription_id1": 1}
        :param service_json_path:(optional) str: path to the service account json file, which should have access to
        the pub-sub
        :param logger:(optional) root logger instance
        :param kwargs: any other key words to overwrite default params
        """
        assert type(subscriptions_with_priority.values()[0]) == int, "Priority should be an integer"

        self.project_id = project_id
        self.topic_id = topic_id
        self.subscriptions_with_priority = sorted(subscriptions_with_priority.items(), key=lambda item: item[1])

        self.subscriber_client = pubsub_v1.SubscriberClient.from_service_account_json(
            filename=service_json_path
        ) if service_json_path else pubsub_v1.SubscriberClient()
        self.logger = logger if logger else logging.getLogger()

        self.max_messages_to_pull = 1
        self.worker_sleep_time = 10
        self.ack_deadline_buffer = 150
        self.ack_deadline = 600
        self.ack_sleep_interval = 10
        self.number_of_times_to_extend = 2
        self.worker_success_statuses = ['success']
        self.worker_fail_statuses = ['fail']

        self.__update_default_params(**kwargs)

    def __update_default_params(self, **kwargs):
        allowed_kwargs = self.__dict__
        for key, value in kwargs.items():
            if key in allowed_kwargs and isinstance(value, type(allowed_kwargs[key])):
                self.__dict__.update([(key, value)])

    def __get_message_based_on_priority(self):
        response, subscription_path = None, None
        for subscription, priority in self.subscriptions_with_priority:
            self.logger.info("Reading message with priority {}".format(priority))
            subscription_path = self.subscriber_client.subscription_path(
                project=self.project_id,
                subscription=subscription,
            )
            self.logger.info("Connected to Subscription: {}".format(subscription))
            try:
                response = self.subscriber_client.pull(subscription=subscription_path,
                                                       max_messages=self.max_messages_to_pull,
                                                       return_immediately=False)
            except DeadlineExceeded:
                pass

            if response and response.received_messages:
                # got a message process it
                self.logger.info("Processing {} messages..".format(len(response.received_messages)))
                break
            else:
                self.logger.info("'{}'-> Subscription is empty, pulling message from new subscription".format(
                    subscription))
                continue
        else:
            self.logger.info("All Subscriptions are empty.... Retrying after {} sec".format(self.worker_sleep_time))
            time.sleep(self.worker_sleep_time)

        return response, subscription_path

    def __start_workers(self, pull_response):
        workers, worker_num, return_values = {}, 0, []
        manager = multiprocessing.Manager()

        for message in pull_response.received_messages:
            return_value = manager.dict()
            return_values.append(return_value)

            worker = multiprocessing.Process(target=self.subscriber,
                                             args=(json.loads(message.message.data), return_value))

            message_data, acknowledge_id = message.message.data, message.ack_id
            # (ack id, message, renew count, worker number)
            workers[worker] = (acknowledge_id, message_data, 1, worker_num)

            self.logger.info("starting worker with message:\n{}\nand ackID:\n{}".format(message_data, acknowledge_id))
            worker.start()
            worker_num += 1

        return workers, return_values

    def __check_to_renew_ack_window(self, workers, worker, subscription_path, time_consumed):
        ack_id, message_data, renew_count, worker_num = workers[worker]

        if time_consumed + self.ack_deadline_buffer > renew_count * self.ack_deadline:
            # total ack time window would be roughly ACK_DEADLINE_SECONDS * (ACK_DEADLINE_TIMES + 1)
            if renew_count < self.number_of_times_to_extend:
                self.logger.info("renewing the ack window for ackID\n{}\n {} time with worker {}".format(
                    ack_id, renew_count, worker_num))
                self.subscriber_client.modify_ack_deadline(
                    subscription=subscription_path,
                    ack_ids=[ack_id],
                    ack_deadline_seconds=self.ack_deadline
                )
                renew_count += 1
                workers[worker] = (ack_id, message_data, renew_count, worker_num)
            else:
                # TODO: deal with exit code better or as per requirement
                # This message took longer than expected, so terminate the worker and try again.
                self.logger.error(
                    "trying to terminate worker thread {}.\n message:\n{}\nackID:\n{}\n since it took longer than "
                    "expected".format(worker_num, message_data, ack_id)
                )
                worker.terminate()
                # pop the worker out to avoid duplicate processing
                workers.pop(worker)

    @abstractmethod
    def subscriber(self, message, return_value):
        pass

    def run(self):
        while True:
            try:
                pull_response, subscription_path = self.__get_message_based_on_priority()
                if not (pull_response and subscription_path):
                    self.logger.info("Finished processing Message. Waiting for New one!")
                    continue

                workers, return_values = self.__start_workers(pull_response)

                total_time_consumed = 0
                while workers:
                    for worker in list(workers):
                        ack_id, message_data, _, worker_num = workers[worker]
                        self.logger.info("running worker message having ackid: {}".format(ack_id))

                        if worker.is_alive():
                            self.logger.info("{} Worker/Workers is/are alive".format(len(workers)))
                            # if message taking longer time extend ack window
                            self.__check_to_renew_ack_window(workers, worker, subscription_path, total_time_consumed)
                        else:
                            self.logger.info("Acknowledging the message as its finished")
                            # ack the message since it is already finished
                            return_info = return_values[worker_num]
                            # Ideally subscriber should return appropriate fail status and message
                            status = return_info.get("status", self.worker_fail_statuses)
                            err_msg = return_info.get("message", "unknown")
                            if status in self.worker_success_statuses:
                                # task completed successfully, acknowledge the message
                                self.subscriber_client.acknowledge(subscription=subscription_path, ack_ids=[ack_id])
                            else:
                                # TODO: deal with failure case appropriately
                                # message failed, retry again
                                self.logger.error(
                                    "Message failed with STATUS: {}, Error:{} for message:\n{}\nackID:\n{}".format(
                                        status, err_msg, message_data, ack_id
                                    )
                                )
                                self.logger.error(
                                    "Retrying message after specified time interval.\nMessage: {}".format(message_data)
                                )

                            workers.pop(worker)
                            self.logger.info("acked  ack_id {} return info \n{}".format(ack_id, return_info))

                    if workers:
                        self.logger.info(
                            "{} worker still processing the message. So going to Sleep for {} seconds".format(
                                len(workers), self.ack_sleep_interval
                            )
                        )
                        time.sleep(self.ack_sleep_interval)
                        total_time_consumed += self.ack_sleep_interval
            except Exception as exc:
                self.logger.exception("Worker Failed due to: {}".format(exc))
