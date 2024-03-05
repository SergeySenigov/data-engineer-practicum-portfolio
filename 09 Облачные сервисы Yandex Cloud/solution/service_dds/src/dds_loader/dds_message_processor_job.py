from datetime import datetime
from logging import Logger
import json
from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer 
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_Repository: DdsRepository,
                 logger: Logger) -> None:

        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._dds_repository = dds_Repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:

        for i in range(self._batch_size):
            
            msg = self._consumer.consume()

            if msg is None:
                self._logger.info('msg is not received ')
                break

            if "object_id" in msg:
                object_id = msg["object_id"]
                self._logger.info(self._consumer.topic + ' ---> ' + str(object_id))
                pass
            else:
                self._logger.info('Msg has no "object_id", skip to next msg')
                continue 
            
            if "object_type" not in msg:
                self._logger.info('Msg has no "object_type", skip to next msg')
                continue 

            if msg["object_type"] != 'order':
                self._logger.info('Msg has object_type not "order" but: ' + msg["object_type"] + ', skip to next msg')
                continue  
              
            if "payload" in msg:
                payload = msg["payload"]
            else:
                self._logger.info('MSG has no "payload", skip to next msg')
                continue

            user_id = payload["user"]["id"]
            user_name = payload["user"]["name"]
            user_login = payload["user"]["login"]
  
            restaurant_id = payload["restaurant"]["id"]
            restaurant_name = payload["restaurant"]["name"]

            order_date = payload["date"]
            cost = payload["cost"]
            payment = payload["payment"]
            order_status = payload["status"]

            if "products" in payload:
                products = payload["products"]
            else:
                self._logger.info('MSG payload has no "products", skip to next msg')    
                continue

            src = {}
            src["service"] = "stg"
            src["object_id"] = object_id

            # add info to hub 'h_user' and satellite 's_user_names' in DDS 
            try:
                self._dds_repository.add_user(user_id, user_name, user_login, json.dumps(src))
            except Exception as E:
                self._logger.info('------------------')
                self._logger.info('Error adding user to dds: ' + str(E))

            # add info to hub 'h_restaurant' and satellite 's_restaurant_names' in DDS
            try:
                self._dds_repository.add_restaurant(restaurant_id, restaurant_name, json.dumps(src))
            except Exception as E:
                self._logger.info('------------------')
                self._logger.info('Error adding restaurant to dds: ' + str(E))

            # if order with the same object_id was loaded earlier (only if order status is CLOSED)
            # we want to get products list passed earlier to CDM in order to update summary info in CDM 
            loaded_earlier_products = self._dds_repository.get_order_products(object_id, self._logger)

            # add info to hub 'h_order', satellites 's_order_cost', 's_order_status', link 'l_order_user' in DDS
            try:
                self._dds_repository.add_order(object_id, order_date, order_status, cost, payment, user_id, json.dumps(src))
            except Exception as E:
                self._logger.info('------------------')
                self._logger.info('Error adding order to dds: ' + str(E))

            # add products info to DDS (h_category, h_product, l_product_category, s_product_names, l_product_restaurant, l_order_product)
            for product in products:

                product['category_id'] = 'not initialized'
            
                # add info to hub 'h_category' in DDS 
                # and get generated category_id
                try:
                    product['category_id'] = str(self._dds_repository.add_category(product["category"], json.dumps(src)))
                except Exception as E:
                    self._logger.info('------------------')
                    self._logger.info('Error adding category to dds: ' + str(E))

                # Add info to hub 'h_product', satellite 's_product_names', links 'l_product_category', 'l_product_restaurant', 'l_order_product' in DDS
                try:
                    self._dds_repository.add_product(object_id, product["id"], product["category"], product["name"], restaurant_id, json.dumps(src))
                except Exception as E:
                    self._logger.info('------------------')
                    self._logger.info('Error adding product to dds: ' + str(E))
          
                if product['category_id'] == "not initialized":
                    raise Exception("category_id for category " + product["category"] + " not initialized")

            # create payload for outgoing message 
            payload_out = {}
            payload_out["id"] = object_id

            # pass to CDM earlier loaded products list to decrease counters
            payload_out["products_remove_cdm"] = loaded_earlier_products

            # add products to CDM only if order status is CLOSED else pass empty list
            payload_out["products_add_cdm"] = []
            if order_status == 'CLOSED': 
                
                for product in products:
                    product_out = dict()
                    product_out['product_id'] = product["id"]
                    product_out['product_name'] = product["name"]
                    product_out['category_id'] = product['category_id']
                    product_out['category_name'] = product["category"]
                    product_out['user_id'] = user_id

                    payload_out["products_add_cdm"].append(product_out)
                    # self._logger.info(product_out['category_id'] + ' ---> ' + product_out["category_name"])
            else:
                pass

            #  compose outgoing message
            msg_out = {}
            msg_out["object_id"] = object_id
            msg_out["object_type"] = 'order'
            msg_out["payload"] = payload_out

            try:
                self._producer.produce(msg_out)
            except Exception as E:   
                self._logger.info('ERROR producing to topic "' + self._producer.topic + '": ' + str(E))
