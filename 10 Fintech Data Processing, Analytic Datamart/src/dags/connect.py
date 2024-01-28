from airflow.models.variable import Variable
    
VERTICA_CONFIG = {
    'host': Variable.get("VERTICA_HOST"),
    'port': '5433',
    'user': Variable.get("VERTICA_USER"),
    'password': Variable.get("VERTICA_PASSWORD"),
    'database': Variable.get("VERTICA_DB"),
    'autocommit': True,
    'use_prepared_statements': False
} 
