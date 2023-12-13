from database_setup import DATABASE_URI, Transaction
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

# Carrega as variáveis de ambiente
load_dotenv()


def get_engine():
    return create_engine(DATABASE_URI)


def count_transactions():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Contagem total de transações
        count = session.query(func.count("*")).select_from(Transaction).scalar()
        return count
    except Exception as e:
        print(f"Erro ao realizar a consulta: {e}")
        return None
    finally:
        session.close()


if __name__ == "__main__":
    total_transactions = count_transactions()
    print(f"Total de transações: {total_transactions}")
