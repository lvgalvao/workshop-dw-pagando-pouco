import functools
import time
import sentry_sdk
from sentry_sdk import capture_message, capture_exception

# Sentry SDK initialization
sentry_sdk.init(
    dsn="https://d78de1e9d7a327c80fcb76c5fd3343a3@o4505699197452288.ingest.sentry.io/4506412273893376",
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)


def log(func):
    """
    Decorator to add automatic logging to a function, including the start time, end time, and duration.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start time
        start_msg = f"Starting function '{func.__name__}' at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
        capture_message(start_msg)

        try:
            result = func(*args, **kwargs)
            end_time = time.time()  # End time
            duration = end_time - start_time  # Duration of the process
            success_msg = f"The function '{func.__name__}' completed successfully in {duration:.2f} seconds"
            capture_message(success_msg)
            return result
        except Exception as e:
            error_msg = f"The function '{func.__name__}' failed with an exception: {e}"
            capture_exception(e)
            capture_message(error_msg)
            raise

    return wrapper
