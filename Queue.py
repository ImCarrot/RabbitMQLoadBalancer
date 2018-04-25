import pika
import pika.exceptions

import ErrorConstants as ErCon
from Configuration.ConfigurationManager import ConfigurationManager


class Operator(object):

    def __init__(self, stop_event, delegate: callable, identifier):
        """
        Create a new instance of the Operator and initialize the connections
        """
        self._stop_event = stop_event
        self._queue_details = self._get_queue_details()
        self._host_ip = self._queue_details['IP']
        self._port = self._queue_details['Port']
        self._username = self._queue_details['Username']
        self._password = self._queue_details['Password']
        self._input_queue_name = self._queue_details['ReadQueueName']
        self._output_queue_name = self._queue_details['WriteQueueName']
        self._error_queue_name = self._queue_details['ErrorQueueName']
        self._delegate = delegate
        self._identifier = identifier
        self._queue_connection = None
        self._input_channel = None
        self._output_channel = None
        self._error_channel = None
        self.is_busy = False
        self.mark_to_terminate = False

    def __del__(self):
        # close connections
        self._queue_connection.close()

    @staticmethod
    def _initialize_channel(connection, queue_name, durable):
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=durable)
        return channel

    @staticmethod
    def _get_queue_details() -> dict:
        """
        Fetches the details from the config file for the Queue
        :return: The dict with all connection details
        """
        return ConfigurationManager().get_value('queueDetails')

    @staticmethod
    def _get_connection(username, password, host_ip, port):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            credentials=pika.PlainCredentials(username, password), host=host_ip, port=port))
        return connection

    def initialize_operator(self):
        connection = self._get_connection(self._username, self._password, self._host_ip, self._port)
        self._queue_connection = connection
        self._input_channel = self._initialize_channel(connection, self._input_queue_name, durable=True)
        self._output_channel = self._initialize_channel(connection, self._output_queue_name, durable= True)
        self._error_channel = self._initialize_channel(connection, self._error_queue_name, durable=True)
        print(f'{self._identifier} is online.')

    def consume(self):
        self._input_channel.basic_qos(prefetch_count=1)
        self._input_channel.basic_consume(self._process_incoming_message, queue=self._input_queue_name)
        self._input_channel.start_consuming()

    def _push_to_queue(self, channel, response):
        channel.basic_publish(exchange='', routing_key=self._output_queue_name, body=response,
                              properties=pika.BasicProperties(delivery_mode=2))  # make message persistent

    def _process_incoming_message(self, channel, method, properties, message):
        if self._stop_event.is_set()
            # TODO you may want to cancel consuming messages and
            # exit this process at this point
            pass

        self.is_busy = True
        processed_result, error_status, error_package = self._delegate(message)

        # handle errors in case it's a Low, Medium, High let the message through but push it to error channel too!
        if error_status is ErCon.NO_ERROR:
            self._output_channel.basic_publish(exchange='', routing_key=self._output_queue_name, body=processed_result,
                                               properties=pika.BasicProperties(delivery_mode=2))
        elif error_status is ErCon.CRITICAL_ERROR:
            self._error_channel.basic_publish(exchange='', routing_key=self._error_queue_name, body=error_package,
                                              properties=pika.BasicProperties(delivery_mode=2))
        else:
            self._output_channel.basic_publish(exchange='', routing_key=self._output_queue_name, body=processed_result,
                                               properties=pika.BasicProperties(delivery_mode=2))

            self._error_channel.basic_publish(exchange='', routing_key=self._error_queue_name, body=error_package,
                                              properties=pika.BasicProperties(delivery_mode=2))

        # send in the final ack of the process.
        channel.basic_ack(delivery_tag=method.delivery_tag)

        # close connection if to avoid receiving messages
        if self.mark_to_terminate:
            self._queue_connection.close()

        self.is_busy = False
