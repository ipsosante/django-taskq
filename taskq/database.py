"""Define database-related utilities functions."""

from django.db.transaction import Atomic, get_connection


LOCK_MODES = (
    'ACCESS SHARE',
    'ROW SHARE',
    'ROW EXCLUSIVE',
    'SHARE UPDATE EXCLUSIVE',
    'SHARE',
    'SHARE ROW EXCLUSIVE',
    'EXCLUSIVE',
    'ACCESS EXCLUSIVE',
)


class LockedTransaction(Atomic):
    """Does lock table inside db transaction.

    Locks the entire table for any transactions, for the duration of this
    transaction. Although this is the only way to avoid concurrency
    issues in certain situations, it should be used with
    caution, since it has impacts on performance, for obvious reasons...

    Inheriting from Atomic ensures we're inside a transaction when calling
    LOCK TABLE.

    Usage:
       with LockedTransaction(Model, TypeLock):
          # DO UPDATE OPERATION in safe mode.
    """

    def __init__(self, model, lock_mode, using=None):
        assert lock_mode in LOCK_MODES
        self.model = model
        self.lock_mode = lock_mode

        # Using savepoint=True ensures we're inside a transaction (which is
        # required to use LOCK TABLE).
        # Also, creating a transaction as close as possible to the SQL query
        # ensures we won't break tests if the query triggers an exception, as
        # we'll break our inner-most transaction instead of the whole test's
        # transaction created by Django.
        super().__init__(using=using, savepoint=True)

    def __enter__(self):
        """Context manager for locking table."""
        super().__enter__()

        connection = get_connection(self.using)
        with connection.cursor() as cursor:
            cursor.execute(
                'LOCK TABLE {db_table_name} IN {lock_mode} MODE'.format(
                    db_table_name=self.model._meta.db_table,
                    lock_mode=self.lock_mode
                )
            )
