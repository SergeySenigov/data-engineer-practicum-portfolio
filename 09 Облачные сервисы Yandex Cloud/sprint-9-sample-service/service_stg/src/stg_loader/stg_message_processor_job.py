import time
from datetime import datetime
from logging import Logger
from app_config import AppConfig
from stg_loader.repository.stg_repository import StgRepository
from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from lib.redis import RedisClient
import json

class StgMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_Repository: StgRepository,
                 logger: Logger) -> None:
        self._logger = logger
        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._redis = redis_client
        self._stg_repository = stg_Repository
        self._batch_size = 100

    # function called by scheduler
    def run(self) -> None:

        for i in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                self._logger.info('msg is not received')
                break

            if "object_type" in msg:
                object_type = msg["object_type"]
            else:
                self._logger.info('msg has no "object_type", skip to next msg')
                continue 

            if msg["object_type"] != 'order':
                self._logger.info('msg has object_type not "order" but: ' + msg["object_type"] + ', skip to next msg')
                continue  

            if "object_id" in msg:
                object_id = msg["object_id"]
                self._logger.info(self._consumer.topic + ' ---> ' + str(object_id))
                pass
            else:
                self._logger.info('Msg has no "object_id", skip to next msg')
                continue 

            if "payload" in msg:    
                payload = msg["payload"]
            else:
                self._logger.info('Msg has no "payload", skip to next msg')
                self._logger.info(msg)
                continue

            try:
                self._stg_repository.order_events_insert(object_id, object_type, datetime.now(), json.dumps(payload))
            except Exception as E:
                self._logger.info('Error inserting into pg: ' + str(E))

            user_id = payload["user"]["id"]
            restaurant_id = payload["restaurant"]["id"]
            
            # get enrichment data on users and restaurant from redis
            try:
                redis_user = self._redis.get(user_id)
                redis_restaurant = self._redis.get(restaurant_id)

                products_menu = redis_restaurant["menu"]
            except Exception as E:   
                self._logger.info('Error calling redis: ' + str(E)) 
                continue

            order_products = payload["order_items"]

            # forming outgoing msg enriching incoming msg
            msg_out = {}
            msg_out["object_id"] = object_id
            msg_out["object_type"] = object_type

            payload_out = {}
            payload_out["id"] = object_id
            payload_out["date"] = payload["date"]
            payload_out["cost"] = payload["cost"]
            payload_out["payment"] = payload["payment"]
            payload_out["status"] = payload["final_status"]

            user = {}
            user["id"] = user_id
            user["name"] = redis_user["name"]
            user["login"] = redis_user["login"]
            payload_out["user"] = user

            restaurant={}
            restaurant["id"] = restaurant_id
            restaurant["name"] = redis_restaurant["name"]
            payload_out["restaurant"] = restaurant

            for product in order_products:
                product_id = product["id"]
                product["category"] = "not found"

                for menu_item in products_menu:
                    if menu_item["_id"] == product_id:
                        product["category"] = menu_item["category"]
                        break 

                if product["category"] == "not found":
                    raise Exception("category for product_id " + product_id + " not found in restaurant " + restaurant_id)

            payload_out["products"] = order_products

            msg_out["payload"] = payload_out

            try:
                self._producer.produce(msg_out)
                self._logger.info(str(msg_out["object_id"]) + ' ---> ' + self._producer.topic)
            except Exception as E:   
                self._logger.info('ERROR producing to topic "' + self._producer.topic + '": ' + str(E))
                self._logger.info('msg_out = ' + json.dumps(msg_out))

