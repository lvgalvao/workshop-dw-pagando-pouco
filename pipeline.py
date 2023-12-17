import time
from log import log


@log
def load_data():
    time.sleep(1)
    pass


@log
def clean_data():
    time.sleep(50)
    pass


@log
def transform_data():
    time.sleep(1)
    pass


@log
def load():
    time.sleep(1)
    pass


@log
def main():
    load_data()
    clean_data()
    transform_data()
    load()


if __name__ == "__main__":
    main()
