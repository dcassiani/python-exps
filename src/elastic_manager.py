from elasticsearch import Elasticsearch, helpers
import logging
import threading


class ElasticManager:
    def __init__(self, spark_ctx):
        self.configs = {
            'url': spark_ctx.environment['ELASTIC_URL'],
            'user': spark_ctx.environment['ELASTIC_USER'],
            'passwd': spark_ctx.environment['ELASTIC_PASS'],
            'use_ssl': self._spark_env_bool(spark_ctx.environment.get('ELASTIC_USE_SSL')),
            'verify_certs': self._spark_env_bool(spark_ctx.environment.get('ELASTIC_VERIFY_CERTS')),
            'timeout': 6000
        }
        self.index = spark_ctx.environment.get('ELASTIC_INDEX', 'invoices')

        self.es = Elasticsearch(
            [self.configs['url']],
            http_auth=(self.configs['user'], self.configs['passwd']),
            use_ssl=self.configs['use_ssl'],
            verify_certs=self.configs['verify_certs'],
            timeout=self.configs['timeout']
        )

    def bulk_insert_or_update(self, df, retry_as_update=False, log_level=logging.INFO):
        logging.basicConfig(level=log_level)
        failed_docs = []

        actions = [
            {
                "_op_type": "index",  # Mudar para "update" durante o retry
                "_index": self.index,
                "_id": row['id'],  # Supondo que a coluna 'id' seja única
                "_source": row.asDict()
            }
            for row in df.collect()
        ]

        logging.info(f"Attempting bulk insert into index: {self.index}")

        insert_thread = threading.Thread(target=self._execute_bulk, args=(actions,))
        insert_thread.start()
        insert_thread.join()

        if failed_docs and retry_as_update:
            logging.debug(f"Retrying {len(failed_docs)} failed inserts as updates.")

            update_actions = [
                {
                    "_op_type": "update",
                    "_index": self.index,
                    "_id": doc['_id'],
                    "doc": doc['_source']
                }
                for doc in failed_docs
            ]

            update_thread = threading.Thread(target=self._execute_bulk, args=(update_actions,))
            update_thread.start()
            update_thread.join()

            if failed_docs:
                logging.error(f"Update failed for {len(failed_docs)} documents.")
            else:
                logging.info("All failed inserts successfully updated.")
        else:
            logging.info("All documents inserted successfully.")

    def _execute_bulk(self, actions):
        success, failed = [], []
        try:
            for success_item, failed_item in helpers.parallel_bulk(self.es, actions):
                if not success_item:
                    failed.append(failed_item)
                else:
                    success.append(success_item)
        except Exception as e:
            logging.error(f"Bulk operation failed: {e}")

        return success, failed

    @staticmethod
    def _spark_env_bool(value):
        return value.lower() == 'true' if value else False
