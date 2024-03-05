from datetime import datetime
from logging import Logger
from uuid import UUID
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cmdRepository: CdmRepository,
                 logger: Logger) -> None:

        self._consumer = kafka_consumer
        self._cdm_repository = cmdRepository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:

        for i in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                self._logger.info('msg is not received')
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

            payload = msg["payload"]

            # we want to add category info to CDM only _once_ for each caterogy in order
            added_categories = set()

            for product in payload["products_add_cdm"]:

                # add user product info to CDM 
                try:
                    self._cdm_repository.add_user_product_info(
                                product["user_id"], 
                                product["product_id"],
                                product["product_name"])
                    
                    # self._logger.info('added for user ' + product['user_id'] + ' product id ' +  product['product_id'] + ', ' + product["product_name"])
                except Exception as E:
                    self._logger.info('Error adding user product ' + str(product["product_id"]) + ": " + str(E))   
                    
                # add user category info to CDM if not added already
                if product["category_name"] not in added_categories:
                    try:
                        self._cdm_repository.add_user_category_info(
                            product["user_id"],
                            product["category_id"], 
                            product["category_name"])    
                    
                        added_categories.add(product["category_name"])
                        # self._logger.info('added for user ' + product['user_id'] + ' category id ' +  product['category_id'] + ', ' + product["category_name"])
                    except Exception as E:
                        self._logger.info('Error adding user category ' + str(product["category_id"]) + ": " + str(E))

            # we want to remove category info from CDM only _once_ for each caterogy in order
            removed_categories = set()

            for product in payload["products_remove_cdm"] :

                # remove user product from CDM 
                try:
                    self._cdm_repository.remove_user_product_info(
                            product["user_id"], 
                            product["product_id"])
                    
                    # self._logger.info('removed for user ' + product['user_id'] + ' product id ' +  product['product_id'])
                except Exception as E:
                    self._logger.info('Error removing user ' + product["user_id"] + ' product ' + str(product["product_id"]) + ": " + str(E))

                # remove user category info from CDM if not removed already 
                if product["category_name"] not in removed_categories:
                    try:
                        self._cdm_repository.remove_user_category_info(
                                product["user_id"],
                                product["category_id"])    
                        
                        removed_categories.add(product["category_name"])
                     # self._logger.info('removed for user ' + product['user_id'] + ' category id ' +  product['category_id'] + ', ' + product["category_name"])
                    except Exception as E:
                        self._logger.info('Error removing user ' + product["user_id"] + ' category ' + str(product["category_id"]) + ": " + str(E))
            
            # for debug
            # self._logger.info('added ' + str(len(payload["products_add_cdm"])) + ', removed ' + str(len(payload["products_remove_cdm"])) + ': ' + str(self._cdm_repository.get_total_counters()))
              


