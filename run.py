"""
Created on Sat Apr 18 16:00:00 2020
@author: krishnknit
"""
import config
import pandas as pd
from logger import Logger
import threading
from eq_prediction import EQPrediction
from concurrent.futures import ThreadPoolExecutor

log = Logger().get_logger()

def prediction_process(eq_prediction):
    """
    To initialize the earthquake prediction process
    :param eq_prediction: earthquake prediction object
    """
    dates = pd.date_range(*(pd.to_datetime([eq_prediction.start_date, eq_prediction.end_date])), freq='10D')
    log.info(f"Creating ThreadPoolExecuter ...")
    total = len(dates)
    with ThreadPoolExecutor(max_workers=config.THREADPOOL_SIZE) as executor:
        futures = {}
        st      = eq_prediction.start_date
        for i, dt in enumerate(dates):
            dt = dt.strftime("%Y-%m-%d")
            log.info(f"processing for date {dt}")
            future = executor.submit(eq_prediction.load_data, st, dt)
            futures[future] = (i, st, dt)
            st = dt

        for future in futures.keys():
            i, st, dt = futures[future]
            try:
                result = future.result(config.TIMEOUT)
            except TimeoutError:
                log.error(f"Timeout Error({config.TIMEOUT})")
            except Exception as ex:
                log.error(f"Failed to process {ex}")
            finally:
                threading.current_thread().setName(f"Thread[{i}/{total}][{__name__}]")

    ## create stats
    eq_prediction.create_stats()


if __name__ == "__main__":
    log.info("Starting data retrieval...")
    eq_prediction = EQPrediction(config)
    prediction_process(eq_prediction)
log.info("Completed data retrieval...")