import json
import sys
import logging, logging.handlers


def init_logger(logfile_path, max_bytes=5000000, backup_count=2):
    logger = logging.getLogger('agent')

    # +-> Prevent adding handlers more than once
    if len(logger.handlers) > 0:
        return

    # +-> Logging options / add handlers
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('{"time": "%(asctime)s", "level": "%(levelname)s", "msg": %(message)s}',
                                  '%Y-%m-%dT%H:%M:%SZ')
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)
    fileHandler = logging.handlers.RotatingFileHandler(logfile_path, maxBytes=max_bytes, backupCount=backup_count)
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)


def create_logger(iaas_client):
    """Loads the logger.
    For a given iaas_client, the logger updates the last_operation file for each log message X with level INFO
    by 'state=processing' and 'step=X'.

    :param iaas_client: the related iaas_client

    :returns: a ``Logger`` object

    :Example:
        ::

            iaas_client = create_iaas_client(...)
            logger = create_logger(iaas_client)
    """
    return Logger(iaas_client)


class Logger:
    def __init__(self, iaas_client):
        self.logger = logging.getLogger('agent')
        self.iaas_client = iaas_client

    def escape_message(self, message):
        try:
            return json.dumps(message)
        except:
            return str(message)

    def debug(self, message):
        """Logs a message with level 'debug'.

        :param message: the message to be logged

        :Example:
            ::

                iaas_client = create_iaas_client(...)
                iaas_client.logger.debug('A log message.')
        """
        self.logger.debug(self.escape_message(message))

    def info(self, message, last_operation_state=None):
        """Logs a message with level 'info' and updates the last_operation file.

        :param message: the message to be logged

        :Example:
            ::

                iaas_client = create_iaas_client(...)
                iaas_client.logger.info('A log message.')
        """
        self.iaas_client.last_operation(message, last_operation_state)
        self.logger.info(self.escape_message(message))

    def warning(self, message):
        """Logs a message with level 'warning'.

        :param message: the message to be logged

        :Example:
            ::

                iaas_client = create_iaas_client(...)
                iaas_client.logger.warning('A log message.')
        """
        self.logger.warning(self.escape_message(message))


    def error(self, message):
        """Logs a message with level 'error'.

        :param message: the message to be logged

        :Example:
            ::

                iaas_client = create_iaas_client(...)
                iaas_client.logger.error('A log message.')
        """
        self.logger.error(self.escape_message(message))

    def critical(self, message):
        """Logs a message with level 'critical'.

        :param message: the message to be logged

        :Example:
            ::

                iaas_client = create_iaas_client(...)
                iaas_client.logger.critical('A log message.')
        """
        self.logger.critical(self.escape_message(message))
