




from datetime import datetime, timezone
import logging

from elasticsearch import Elasticsearch, TransportError

from DatabaseCreator.database import get_Elasticsearch

class ExcludeElasticsearchLogs(logging.Filter):
    def filter(self, record):
        return not record.name.startswith("elasticsearch")

class ElasticsearchHandler(logging.Handler):
    def __init__(self, es_client, index_name="extraction_logs"):
        super().__init__()
        self.es = es_client
        self.index_name = index_name

        

    def emit(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger_name": record.name,
            "message": record.getMessage(),
            
        }
        try:
            self.es.index(index=self.index_name, document=log_entry)
        except TransportError as e:
            print(f"[Logging Error] Could not send log to Elasticsearch: {e}")