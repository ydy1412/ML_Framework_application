import logging
import time
import json
from datetime import datetime
import os
import sys

import pytz
from dateutil.tz import tzlocal

class Logger :
    def __init__(self, name):
        self.name = name
        self.base_dir = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
        self.Log_base_dir = self.base_dir +'/Static_files/Log_folder'
        self.local_tz = tzlocal()
        self.date_str = datetime.now().strftime('%Y%m%d')
        self.Log_stats_dir =self.Log_base_dir + '/'  + self.date_str
        try :
            os.mkdir(self.Log_stats_dir)
        except Exception as e:
            print(e)

        logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.logger = logging.getLogger(self.name)
        self.log_handler = logging.FileHandler(self.Log_stats_dir+'/'+self.name+'.json')
        self.logger.addHandler(self.log_handler)

    def __timestamp(self) :
        return str(datetime.now(tz=self.local_tz).isoformat())

    def log(self, event, event_value) :
        log = {
            'timestamp' : self.__timestamp(),
            'component' : self.name,
            'log' : {
                'event' : event,
                'event_value' : event_value
            }
        }
        self.logger.info(json.dumps(log))