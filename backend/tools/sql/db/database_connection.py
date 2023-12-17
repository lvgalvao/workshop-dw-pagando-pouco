import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def getDbConnectionById(id: int):
    configs = {
        "DB_USER": os.getenv(f"DB_USER{id}"),
        "DB_PASSWORD": os.getenv(f"DB_PASSWORD{id}"),
        "DB_HOST": os.getenv(f"DB_HOST{id}"),
        "DB_NAME": os.getenv(f"DB_NAME{id}"),
    }

    for var in configs:
        if configs[var] is None:
            print(f"A variável de ambiente {var} não está definida.")
            sys.exit(1)

    DATABASE_URI = f"postgresql+psycopg2://{configs['DB_USER']}:{configs['DB_PASSWORD']}@{configs['DB_HOST']}/{configs['DB_NAME']}"

    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)
    return Session()
