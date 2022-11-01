from typing import Union
import logging
from logging import config

'''

- notes
    - want an easy way to create a logger that can send messages to multiple outputs, change levels, change format/colors, 
    - may want some predefined logger classes that are commonly used
    - use name in file (i.data. __name__)
    - investigate the classes they already have and maybe make some wrappers
    - for a custom logging class may want the following:
        - location for where logs will be sent
    
    
    - for guis and other instances where a user will be using the program, want to find a way to pass logs up smoothly
    back up and offer a chance to send a log report given an error that was unforseen. 

'''


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'

    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def format_color(item):
    print(item)
    return item


class Logger():
    # todo may want to add color changes to different message levels; not super important

    def __init__(self, settings: Union[None, dict] = None, logger: str = __name__) -> None:
        self._default_log_settings = {
            'version': 1,
            'disable_existing_loggers': True,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s [%(levelname)s] %(name)s: \033[92m\t%(message)s\033[0m'
                },
            },
            'handlers': {
                'console': {
                    'level': 'DEBUG',
                    'formatter': 'standard',
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stdout'
                },
                # 'file': {
                #     'level': 'DEBUG',
                #     'formatter': 'standard',
                #     'class': 'logging.StreamHandler',
                #     'filename':'log.log',
                #     'mode':'w'
                # },

            },
            'loggers': {
                logger: {
                    'handlers': ['console'],
                    'level': 'DEBUG',
                    'propagate': False
                }
            }
        } if not settings else settings

        config.dictConfig(self._default_log_settings)
        self.logger = logging.getLogger(logger)

    def critical(self, message: str) -> None:
        self.logger.handlers[0].formatter._fmt = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        self.logger.critical(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def notset(self, message: str) -> None:
        self.logger.notset(message)


if __name__ == '__main__':
    l = Logger(logger=__name__)
    l.debug('a message!')
    l.info(' an info')
    l.critical('a bad message')
    l.info('blah')

    quit()
