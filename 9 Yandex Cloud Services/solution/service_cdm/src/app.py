import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository.cdm_repository import CdmRepository


app = Flask(__name__)


# Make endpoint to check if service is up
@app.get('/')
def index():
    return 'service is working'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    # Init config. For convinience getting envs is placed in a separate class
    config = AppConfig()

    cdmRepository = CdmRepository(config.pg_warehouse_db())

    # Init messages processor, pass objects to constructor
    proc = CdmMessageProcessor(config.kafka_consumer(), 
                                cdmRepository, app.logger) 

    # Run processor in background
    # BackgroundScheduler will run upon schedule function "run" of StgMessageProcessor.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=AppConfig.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # start Flask app to keep service running
    app.run(debug=False, host='0.0.0.0', use_reloader=False)
