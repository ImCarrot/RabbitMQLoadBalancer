import datetime
import json
import sys
import time
from collections import deque

import pika

import ErrorConstants as ErCon
from .Queue import Operator
from .Configuration.ConfigurationManager import ConfigurationManager
from multiprocessing import Process


class LoadBalancer(object):
    _instance = None
    _max_agent_count = None
    _blocking_limit = None
    _byte_value = {'C': 0.1, 'H': 0.01, 'M': 0.001, 'L': 0.0001}

    def __new__(cls, *args, **kwargs):
        if LoadBalancer._instance is None:
            LoadBalancer._instance = object.__new__(cls)

        # set the maximum agents allowed from the config
        config = ConfigurationManager().get_value('queueDetails')
        LoadBalancer._max_agent_count = config['MaxQueueClientCount']
        LoadBalancer._blocking_limit = config['BlockingLimit']
        return LoadBalancer._instance

    def __init__(self):
        self._speed_queue = deque()
        self._online_agents = []
        self._rps = None
        self._road = None
        self._incoming_lane = None
        self._outgoing_lane = None
        # establish all connections
        self._start_engine()

    def _start_engine(self):
        config = ConfigurationManager().get_value('queueDetails')

        # create the communication channel
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            credentials=pika.PlainCredentials(config['Username'], config['Password']),
            host=config['IP'], port=config['Port']))

        self._road = connection.channel()
        self._incoming_lane = self._road.queue_declare(queue=config['ReadQueueName'], durable=True)
        self._outgoing_lane = self._road.queue_declare(queue=config['WriteQueueName'], durable=True)

    def _clean_up_agents(self):
        """
        removes the agents that are marked for termination from active state and sets them to None for GC
        Returns
        -------
        None
        """
        fresh_agents = []
        for agent, process in self._online_agents:
            if agent.mark_to_terminate and not agent.is_busy:
                # noinspection PyUnusedLocal
                agent = None
                process.terminate()
                continue
            fresh_agents.append((agent, process))

        self._online_agents = fresh_agents

    def _translate_and_parse(self, message) -> tuple:
        """

        Parameters
        ----------
        message

        Returns
        -------
        (tuple) 3 values,
        processed_result (dict):
        error_status (ErrorConstant):
        error_package (dict):
        """
        try:
            # decodes the transferred byte[] to json string and then converts to a dict
            transferred_message = json.loads(message.decode('UTF-8'))

            # to avoid warning of too broad exception handling:
            # noinspection PyBroadException
            try:

                # process start checkpoint
                start = time.time()

                # process the results
                processed_result = f'I processed {transferred_message}'

                # let's assume it takes 1 second to process.
                time.sleep(1)

                # converts the processed output the best match of the lot as json
                to_transfer = json.dumps(processed_result[0])

                # process end checkpoint
                processing_time = round(((time.time() - start) * 1000), 2)

                # update processing stats
                self._calculate_velocity(processing_time)

                # write to log file
                print(f'In: {processing_time} ms -> Processed: {transferred_message}')

                return to_transfer, ErCon.NO_ERROR, None

            except Exception as error:
                print(f'{transferred_message} -> {error}')

                error_package = dict(SourceProcess='loadBalancer', Blame='loadBalancer',
                                     Timestamp=str(datetime.datetime.now()),
                                     Payload=dict(ErrorMessage=str(error), Input=message.decode('UTF-8')),
                                     Severity=ErCon.HIGH_ERROR)

                # return the input as it is in case of any error.
                return json.dumps(transferred_message['result'][0]), ErCon.HIGH_ERROR, json.dumps(error_package)

        except json.JSONDecodeError:
            print('error while converting from json')
            error_package = dict(SourceProcess='loadBalancer', Blame='InputQ', Timestamp=str(datetime.datetime.now()),
                                 Payload=dict(ErrorMessage='JSON Decode Error.', Input=message.decode('UTF-8')),
                                 Severity=ErCon.CRITICAL_ERROR)
            return None, ErCon.CRITICAL_ERROR, json.dumps(error_package)

    def _calculate_velocity(self, time_taken: float):
        """
        records time taken for last 10 transactions and calculates speed in record/second
        Parameters
        ----------
        time_taken
        The time taken to process the message
        Returns
        -------
        None
        """
        # maintains the collection for last 10 transactions
        if len(self._speed_queue) is 10:
            self._speed_queue.popleft()

        self._speed_queue.append(time_taken)
        self._rps = round(((1000 * len(self._online_agents)) *
                           (round(sum(self._speed_queue) / len(self._speed_queue), 2))), 2)

    def _fetch_speed_bucket(self, load_value):
        """
        fetches the bucket in which the load value exists.
        C => meaning critical, load has crossed threshold
        H => meaning High, load has crossed 75% of the threshold
        M => Medium, load has crossed 40% of the threshold
        L => Low, , load is below 40% of the threshold
        Parameters
        ----------
        load_value
        (float) => the value of the load, load / threshold
        Returns
        -------
        (str) => C or H or M or L based on the bucket
        """

        if load_value >= 1:
            return self._byte_value['C']
        elif 1 < load_value >= 0.75:
            return self._byte_value['H']
        elif 0.75 < load_value >= 0.40:
            return self._byte_value['M']
        else:
            return self._byte_value['L']

    def _calculate_scaling_ratio(self, incoming_load: float, outgoing_load: float):
        x = self._fetch_speed_bucket(incoming_load)
        y = self._fetch_speed_bucket(outgoing_load)

        # difference between loads (Z)
        z = x - y

        if z == 0:
            return None, 0

        # number of times digit 9 occurs in the decimal (N)
        n = str(z).count('9')

        # calculate the scale ratio (Q)
        q = (2 * n) - 1

        # absolute co-efficient (P)
        p = 1 if z > 0 else -1

        # co-efficient * scale ratio
        result = q * p

        # True if scale up False if scale down and None if remain same
        scale_up = True if result > 0 else False

        return scale_up, abs(result)

    def _scale_up(self, affected_count: int):
        """
        scales up the number of agents
        :param affected_count: the number of agents to bring online.
        :return: None
        """

        active_agents = [agent for agent, _ in self._online_agents if not agent.mark_to_terminate or not agent.is_busy]
        spin_up_count = affected_count - len(active_agents)

        if spin_up_count is 0 or spin_up_count < 0:
            return
        elif spin_up_count > 5:
            spin_up_count = 5

        # spins up the agent
        for count in range(spin_up_count):
            instance = Operator(self._translate_and_parse, f'Operator: {time.time()}')
            instance.initialize_operator()
            process = Process(target=instance.consume)
            process.start()
            self._online_agents.append((instance, process))

    def _scale_down(self, affected_count: int):
        """
        Scales down the agents active.
        :param affected_count: The number of agents to shut down.
        :return: None
        """
        active_agents = [agent for agent, _ in self._online_agents if not agent.mark_to_terminate or not agent.is_busy]
        shut_count = len(active_agents) - affected_count

        if shut_count is 0:
            return

        # mark the old agents for termination.
        for count in range(shut_count):
            agent, _ = self._online_agents[count]
            agent.mark_to_terminate = True

    def balance_load(self):
        # remove idle agents
        self._clean_up_agents()
        status = 'Consistent'
        incoming_load_value = self._incoming_lane.method.message_count / self._blocking_limit
        outgoing_load_value = self._outgoing_lane.method.message_count / self._blocking_limit

        scale, affected_count = self._calculate_scaling_ratio(incoming_load_value, outgoing_load_value)

        if affected_count is 0 and len(self._online_agents) != 0:
            self.update_console_status(status)
            return

        # in-case there are no online agents.
        affected_count = 1 if affected_count is 0 else affected_count

        if scale:
            status = 'Scaled Up'
            self._scale_up(affected_count)
        else:
            status = 'Scaled Down'
            self._scale_down(affected_count)

        self.update_console_status(status)

    def update_console_status(self, current_status: str):
        sys.stdout.write(f"\r Agents Online: {len(self._online_agents)} || Average Speed: {self._rps} record/sec"
                         f" || Current Status: {current_status}")
        sys.stdout.flush()


def main():
    balancer = LoadBalancer()
    # to create a timer to keep it running.
    while True:
         balancer.balance_load()
         time.sleep(1)


if __name__ == '__main__':
    main()
