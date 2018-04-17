import datetime
import json
import time
import ErrorConstants as ErCon
from .Queue import Operator
from multiprocessing import Process


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
            to_transfer = json.dumps(processed_result)
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
            return json.dumps(transferred_message), ErCon.HIGH_ERROR, json.dumps(error_package)
    except json.JSONDecodeError:
        print('error while converting from json')
        error_package = dict(SourceProcess='loadBalancer', Blame='InputQ', Timestamp=str(datetime.datetime.now()),
                             Payload=dict(ErrorMessage='JSON Decode Error.', Input=message.decode('UTF-8')),
                             Severity=ErCon.CRITICAL_ERROR)
        return None, ErCon.CRITICAL_ERROR, json.dumps(error_package)


consumer_count = 5


def main():
    for count in range(consumer_count):
        instance = Operator(_translate_and_parse, f'Operator: {time.time()}')
        instance.initialize_operator()
        process = Process(target=instance.consume)
        process.start()


if __name__ == '__main__':
    main()
