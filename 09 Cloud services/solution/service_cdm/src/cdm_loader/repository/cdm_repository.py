import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def add_user_product_info(self,
                            user_id: str,
                            product_id: str,
                            product_name: str
                            ) -> None:
        """ add new record with counter value "1" or increase counter in existing record for product for user """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
                    values (%(user_id)s, %(product_id)s, %(product_name)s, 1)
                    ON CONFLICT (user_id, product_id) DO UPDATE
                        SET
                        order_cnt = cdm.user_product_counters.order_cnt + 1,
                        product_name = EXCLUDED.product_name
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name
                    }
                )

    def add_user_category_info(self,
                            user_id: str,
                            category_id: str,
                            category_name: str
                            ) -> None:
        """ add new record with counter value "1" or increase counter in existing record for category for user """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
                    values (%(user_id)s, %(category_id)s, %(category_name)s, 1)
                    ON CONFLICT (user_id, category_id) DO UPDATE
                        SET
                        order_cnt = cdm.user_category_counters.order_cnt + 1,
                        category_name = EXCLUDED.category_name
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name
                    }
                )

    def remove_user_product_info(self,
                            user_id: str,
                            product_id: str,
                            ) -> None:
        """ decrease counter in existing record for product for user and check if record exists"""

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cdm.user_product_counters
                        SET
                        order_cnt = order_cnt - 1
                        where user_id = %(user_id)s and product_id = %(product_id)s returning cdm.user_product_counters.*; 
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id
                    }
                )

                assert(len(cur.fetchall())>0)
                
    def remove_user_category_info(self,
                            user_id: str,
                            category_id: str,
                            ) -> None:   
        """ decrease counter in existing record for category for user and check if record exists"""
 
        with self._db.connection() as conn:               
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cdm.user_category_counters
                        SET
                        order_cnt = order_cnt - 1
                        where user_id = %(user_id)s and category_id = %(category_id)s returning cdm.user_category_counters.*; 
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id
                    }
                )

                assert(len(cur.fetchall())>0)


    def get_total_counters(self,
                            ) -> tuple:
        """ return count orders in tables for debugging """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select s1.product_order_cnt, s2.category_order_cnt from 
                    (select sum(order_cnt) product_order_cnt from cdm.user_product_counters) s1,
                    (select sum(order_cnt) category_order_cnt from cdm.user_category_counters) s2
                     ; 
                    """
                )
                
                rows = cur.fetchall()

        return (rows[0][0], rows[0][1])
