# elastic/dao/elastic_dao.py
import json

from elasticsearch import helpers


class ElasticDAO:
    def __init__(self, es):
        self.es = es

    def insertme(self, index: str, documentType: str, dataList: list, error_occurred, _id_gen, isDebugEnabled: bool):

        if not self.es.indices.exists(index=index):
            error_occurred.append(True)
            raise Exception(f"Indice {index} nao encontrado. Abortando operacao.")

        actions_update = [
            {
                "_op_type": "update",
                "_index": index,
                "_type": documentType,
                "_id": _id_gen(row),  # usa o _id_gen passado
                "doc": row,
                "doc_as_upsert": False
            }
            for row in dataList
        ]

        actions_insert = []
        indexed = 0

        try:
            for success, result in helpers.streaming_bulk(self.es, actions_update,
                              raise_on_error=False, request_timeout=60, max_retries=3):
                if success:
                    indexed += 1
                else:
                    action, error = result.popitem()
                    if 'document_missing_exception' == error['error']['type']:
                        status = error['status']
                        if status == 404:
                            row = next((item for item in dataList if _id_gen(item) == error['_id']), None)
                            if row:
                                actions_insert.append({
                                    "_op_type": "index",
                                    "_index": index,
                                    "_type": documentType,
                                    "_id": error['_id'],
                                    "_source": row,
                                    "doc_as_upsert": False
                                })
                            else:
                                error_occurred.append(True)
                    else:
                        error_occurred.append(True)

            for success, result in helpers.streaming_bulk(self.es, actions_insert,
                      raise_on_error=True, raise_on_exception=True, request_timeout=60, max_retries=3):
                if success:
                    indexed += 1
                else:
                    action, error = result.popitem()
                    print(f"Erro ao inserir documento ID {error['index']['_id']}: {json.dumps(error, indent=2)}")
                    error_occurred.append(True)

        except Exception as err:
            print('Exception no insertme')
            error_message = str(err)
            print('{}'.format(error_message))
            error_occurred.append(True)
            raise

        return indexed

