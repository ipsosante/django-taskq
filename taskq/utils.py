import datetime
import traceback


def ordinal(n: int):
    """Output the ordinal representation ("1st", "2nd", "3rd", etc.) of any number."""
    # https://stackoverflow.com/a/20007730/404321
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    return str(n) + suffix


def parse_timedelta(delay, nullable=False):
    """A convenience function to create a timedelta from seconds.

    You can also pass a timedelta directly to this function and it will be
    returned unchanged."""
    if delay is None and nullable:
        return None

    if isinstance(delay, datetime.timedelta):
        return delay

    if isinstance(delay, int):
        return datetime.timedelta(seconds=delay)

    raise TypeError("Unexpected delay type")


def traceback_filter_taskq_frames(exception):
    """Will return the traceback of the passed exception without the taskq
    internal frames except the last one (which will be "_protected_call" in
    most cases).
    """
    exc_traceback = exception.__traceback__
    stack = traceback.extract_tb(exc_traceback)

    # Find the number of internal frame we need to skip using the StackSummary
    n_skiped_frames = 0
    found_protected_call = False
    for frame in stack:
        if frame.name == "_protected_call":
            found_protected_call = True
            break

        n_skiped_frames += 1

    if not found_protected_call:
        return exc_traceback

    # Unroll the traceback until the taskq frames are not included anymore
    for _ in range(n_skiped_frames):
        exc_traceback = exc_traceback.tb_next

    return exc_traceback
