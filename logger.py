import logging

class Logger():
    def __init__(self, log_file='earthquake_prediction'):
        self.log_file = log_file

    def get_logger(self):
        logging.basicConfig(
            level = logging.INFO,
            format = "%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
            handlers = [
                logging.FileHandler(f"{self.log_file}.log"),
                logging.StreamHandler()
            ]
        )

        logger = logging.getLogger()
        return logger
