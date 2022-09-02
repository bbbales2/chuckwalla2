import time


class Throttler:
    last_finished: float
    minimum_seconds_between_calls: float

    def __init__(self, minimum_seconds_between_calls: float = 0.25):
        if minimum_seconds_between_calls <= 0:
            raise ValueError("minimum_seconds_between_calls must be positive")

        self.last_finished = time.time() - minimum_seconds_between_calls
        self.minimum_seconds_between_calls = minimum_seconds_between_calls

    def sleep_if_necessary(self):
        elapsed_time = time.time() - self.last_finished
        if elapsed_time < self.minimum_seconds_between_calls:
            time.sleep(self.minimum_seconds_between_calls - elapsed_time)

        self.last_finished = time.time()
