import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor
from stg_loader.repository.stg_repository import StgRepository

app = Flask(__name__)

# Make endpoint to check if service is up
@app.get('/')
def index():
    return 'service is up'

if __name__ == '__main__':
    app.logger.setLevel(logging.INFO)

    # Init config. For convinience getting envs is placed in a separate class
    config = AppConfig()
    
    stgRepository = StgRepository(config.pg_warehouse_db())
    
    # Init messages processor, pass objects to constructor
    proc = StgMessageProcessor(config.kafka_consumer(), config.kafka_producer(), 
                                config.redis_client(), stgRepository, app.logger) 

    # Run processor in background
    # BackgroundScheduler will run upon schedule function "run" of StgMessageProcessor.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()
    
    # start Flask app to keep service running
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
