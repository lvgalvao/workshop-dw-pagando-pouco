from datasource.api import APICollector
from datasource.postgre import PostgresCollector
from contracts.schema import CompraSchema
from tools.aws.client import S3Client

import time
import schedule

schema = CompraSchema
aws = S3Client()


def apiCollector(schema, aws, repeat):
    reponse = APICollector(schema, aws).start(repeat)
    print("Executei")
    return


def getPostgre(aws, dbId):
    postgres = PostgresCollector(aws, dbId).start()


# schedule.every(1).minutes.do(apiCollector,schema, aws, 50)


# while True:
#     schedule.run_pending()
#     time.sleep(1)

getPostgre(aws, dbId=1)
getPostgre(aws, dbId=2)
