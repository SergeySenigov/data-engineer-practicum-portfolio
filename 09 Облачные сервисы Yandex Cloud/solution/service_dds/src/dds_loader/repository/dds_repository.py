import uuid
from datetime import datetime
from typing import Any, Dict, List
from logging import Logger

from lib.pg import PgConnect
from pydantic import BaseModel



class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def get_order_products(self,
                             order_id: str,
                             logger: Logger
                             ) -> list:
        """ Returns earlier passed to CDM products list """
        
        products = list()

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select hp.product_id, cat.h_category_pk, cat.category_name, usr.user_id, sos.status
                     from dds.h_order ho
                     join dds.l_order_product lop on lop.h_order_pk = ho.h_order_pk
                     join dds.h_product hp on lop.h_product_pk = hp.h_product_pk
                     join dds.l_product_category lpc on lpc.h_product_pk = hp.h_product_pk
                     join dds.h_category cat on cat.h_category_pk = lpc.h_category_pk
                     join dds.l_order_user lou on lou.h_order_pk = ho.h_order_pk
                     join dds.h_user usr on usr.h_user_pk = lou.h_user_pk
                     join dds.s_order_status sos on sos.h_order_pk = ho.h_order_pk
                     where ho.order_id = %(order_id)s 
                     and sos.status = 'CLOSED'; 
                    """,
                    {
                        'order_id': order_id
                    }
                )
                
                for row in cur.fetchall():
                    product = dict()
                    product["product_id"] = row[0]
                    product["category_id"] = str(row[1])
                    product["category_name"] = row[2]
                    product["user_id"] = row[3]
                    products.append(product)

        return products

    def add_user(self,
                            user_id: str,
                            user_name: str,
                            user_login: str,
                            src: str
                            ) -> None:
        """ Add info to hub 'h_user' and satellite 's_user_names' in DDS """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into dds.h_user(h_user_pk, user_id, load_dt, load_src )
                    select md5(%(user_id)s)::uuid, %(user_id)s::varchar, now(), %(load_src)s
                    where %(user_id)s not in (select user_id from dds.h_user) returning * ; 
                    """,
                    {
                        'user_id': user_id,
                        'load_src': src,
                    }
                )

                cur.execute(
                        """
                        insert into dds.s_user_names(h_user_pk, load_dt, username, userlogin, load_src, hk_user_names_hashdiff)
                        select (select h_user_pk from dds.h_user where user_id = %(user_id)s), 
                                now(), %(username)s::varchar, %(userlogin)s::varchar, %(load_src)s, 
                                md5(%(user_id)s || %(username)s || %(userlogin)s)::uuid
                        where md5(%(user_id)s || %(username)s || %(userlogin)s)::uuid not in 
                            (select hk_user_names_hashdiff from dds.s_user_names)
                        """,
                        {
                            'user_id': user_id,
                            'username': user_name,
                            'userlogin': user_login,
                            'load_src': src
                        }
                )


    def add_restaurant(self,
                            restaurant_id: str,
                            restaurant_name: str,
                            src: str
                            ) -> None:
        """ Add info to hub 'h_restaurant' and satellite 's_restaurant_names' in DDS """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into dds.h_restaurant(h_restaurant_pk, restaurant_id, load_dt, load_src )
                    select md5(%(restaurant_id)s)::uuid, %(restaurant_id)s::varchar, now(), %(load_src)s
                    where %(restaurant_id)s not in (select restaurant_id from dds.h_restaurant) returning * ; 
                    """,
                    {
                        'restaurant_id': restaurant_id,
                        'load_src': src,
                    }
                )

                cur.execute(
                        """
                        insert into dds.s_restaurant_names(h_restaurant_pk, load_dt, name, load_src, hk_restaurant_names_hashdiff)
                        select (select h_restaurant_pk from dds.h_restaurant where restaurant_id = %(restaurant_id)s),
                               now(), %(restaurantname)s::varchar, %(load_src)s, 
                               md5(%(restaurant_id)s || %(restaurantname)s)::uuid
                        where md5(%(restaurant_id)s || %(restaurantname)s)::uuid not in 
                            (select hk_restaurant_names_hashdiff from dds.s_restaurant_names)
                        """,
                        {
                            'restaurant_id': restaurant_id,
                            'restaurantname': restaurant_name,
                            'load_src': src
                        }
                )
                
    def add_category(self,
                            category_name: str,
                            src: str
                            ) -> uuid:
        """ Add info to hub 'h_category' in DDS """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into dds.h_category(h_category_pk, category_name, load_dt, load_src )
                    select md5(%(category_name)s)::uuid, 
                           %(category_name)s, 
                           now(), 
                           %(load_src)s
                        ON CONFLICT(h_category_pk) DO UPDATE 
                         SET
                         category_name = EXCLUDED.category_name returning *;
                    """,
                    {
                        'category_name': category_name,
                        'load_src': src,
                    }
                )

                res = cur.fetchall()

                if len(res) > 0:
                    category_id = res[0][0]
                else:
                    category_id = 'not initialized'
                
        return category_id
    
    def add_order(self,
                            order_id: str,
                            order_dt: str,
                            order_status: str,
                            cost: str,
                            payment: str,
                            user_id: str,
                            src: str
                            ) -> None:
        """ Add info to hub 'h_order',
            satellites 's_order_cost', 's_order_status', link 'l_order_user' in DDS """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into dds.h_order(h_order_pk, order_id, order_dt, load_dt, load_src )
                    select md5(%(order_id)s::varchar)::uuid, %(order_id)s, %(order_dt)s, now(), %(load_src)s
                    ON CONFLICT (h_order_pk) DO UPDATE
                        SET
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_src': src,
                    }
                )

                cur.execute(
                        """
                        insert into dds.s_order_cost(h_order_pk, load_dt, cost, payment, load_src, hk_order_cost_hashdiff)
                        select (select h_order_pk from dds.h_order where order_id = %(order_id)s), 
                                now(), %(cost)s, %(payment)s, %(load_src)s, 
                                md5(%(order_id)s::varchar || %(cost)s::varchar || %(payment)s::varchar)::uuid
                        where md5(%(order_id)s::varchar || %(cost)s::varchar || %(payment)s::varchar)::uuid not in
                            (select hk_order_cost_hashdiff from dds.s_order_cost)
                        """,
                        {
                            'order_id': order_id,
                            'cost': cost,
                            'payment': payment,
                            'load_src': src
                        }
                )

                cur.execute(
                        """
                        insert into dds.s_order_status(h_order_pk, load_dt, status, load_src, hk_order_status_hashdiff)
                        select (select h_order_pk from dds.h_order where order_id = %(order_id)s), 
                                now(), %(status)s::varchar, %(load_src)s, 
                                md5(%(order_id)s::varchar || %(status)s)::uuid
                        where md5(%(order_id)s::varchar || %(status)s)::uuid not in 
                            (select hk_order_status_hashdiff from dds.s_order_status)
                        """,
                        {
                            'order_id': order_id,
                            'status': order_status,
                            'load_src': src
                        }
                )

                cur.execute(
                        """
                        insert into dds.l_order_user(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                        select md5(%(order_id)s::varchar || %(user_id)s)::uuid, 
                               (select h_order_pk from dds.h_order where order_id = %(order_id)s), 
                               (select h_user_pk from dds.h_user where user_id = %(user_id)s), 
                               now(), %(load_src)s 
                        where (md5(%(order_id)s::varchar || %(user_id)s)::uuid not in (select hk_order_user_pk from dds.l_order_user))
                        """,
                        {
                            'order_id': order_id,
                            'user_id': user_id,
                            'load_src': src
                        }
                )
                
    def add_product(self,
                            order_id: str,
                            product_id: str,
                            category: str,
                            product_name: str,
                            restaurant_id: str,
                            src: str
                            ) -> int:
        """ Add info to hub 'h_product',
            satellite 's_product_names', 
            links 'l_product_category', 'l_product_restaurant', 'l_order_product' in DDS """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into dds.h_product(h_product_pk, product_id, load_dt, load_src)
                    select md5(%(product_id)s)::uuid, %(product_id)s, now(), %(load_src)s
                    where %(product_id)s not in (select product_id from dds.h_product) returning * ; 
                    """,
                    {
                        'product_id': product_id,
                        'load_src': src,
                    }
                )

                res = cur.fetchall()

                added_cnt = len(res)

                cur.execute(
                        """
                        insert into dds.s_product_names(h_product_pk, load_dt, name, load_src, hk_product_names_hashdiff)
                        select (select h_product_pk from dds.h_product where product_id = %(product_id)s), 
                                now(), %(product_name)s::varchar, %(load_src)s, 
                                md5(%(product_id)s || %(product_name)s)::uuid
                        where md5(%(product_id)s || %(product_name)s)::uuid not in 
                           (select hk_product_names_hashdiff from dds.s_product_names)
                        """,
                        {
                            'product_id': product_id,
                            'product_name': product_name,
                            'load_src': src
                        }
                )

                cur.execute(
                        """
                        insert into dds.l_product_category(hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                        select md5(%(product_id)s || %(category)s)::uuid, 
                              (select h_product_pk from dds.h_product where product_id = %(product_id)s),
                              (select h_category_pk from dds.h_category where category_name = %(category)s), 
                              now(), %(load_src)s 
                        where (md5(%(product_id)s || %(category)s)::uuid not in (select hk_product_category_pk from dds.l_product_category))
                        """,
                        {
                            'product_id': product_id,
                            'category': category,
                            'load_src': src
                        }
                )

                cur.execute(
                        """
                        insert into dds.l_product_restaurant(hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                        select md5(%(product_id)s || %(restaurant_id)s)::uuid, 
                               (select h_product_pk from dds.h_product where product_id = %(product_id)s), 
                               (select h_restaurant_pk from dds.h_restaurant where restaurant_id = %(restaurant_id)s), 
                               now(), %(load_src)s 
                        where (md5(%(product_id)s || %(restaurant_id)s)::uuid not in (select hk_product_restaurant_pk from dds.l_product_restaurant))
                        """,
                        {
                            'product_id': product_id,
                            'restaurant_id': restaurant_id,
                            'load_src': src
                        }
                )

                cur.execute(
                        """
                        insert into dds.l_order_product(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                        select md5(%(order_id)s::varchar || %(product_id)s)::uuid,
                               (select h_order_pk from dds.h_order where order_id = %(order_id)s), 
                               (select h_product_pk from dds.h_product where product_id = %(product_id)s), 
                               now(), %(load_src)s 
                        where (md5(%(order_id)s::varchar || %(product_id)s)::uuid not in (select hk_order_product_pk from dds.l_order_product))
                        """,
                        {
                            'product_id': product_id,
                            'order_id': order_id,
                            'load_src': src
                        }
                )
                
        return added_cnt
