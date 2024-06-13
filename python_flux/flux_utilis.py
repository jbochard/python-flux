import traceback
import time
from jsonmerge import merge


class FluxValue:

    def __init__(self, value, empty, ex):
        self.val_value = value
        self.val_empty = empty
        self.val_error = ex

    def is_error(self, exceptions=None):
        return self.val_error is not None \
            and (exceptions is None
                 or len(exceptions) == 0
                 or any([isinstance(self.val_error, ex) for ex in exceptions]))

    def is_empty(self):
        return self.val_empty

    def is_value(self):
        return not self.is_error() and not self.is_empty()

    def is_stop(self):
        return self.is_error() and isinstance(self.val_error, StopIteration)

    def val(self):
        return self.val_value

    def err(self):
        return self.val_error

    @staticmethod
    def value(val):
        return FluxValue(val, False, None)

    @staticmethod
    def empty():
        return FluxValue(None, True, None)

    @staticmethod
    def error(ex):
        return FluxValue(None, False, ex)

    @staticmethod
    def stop():
        return FluxValue(None, False, StopIteration())


class Register:
    def __init__(self, sup):
        self.value = None
        self.ctx = None
        self.elapsed_time = 0
        self.sup = sup
        self.sup.prepare_next()

    def execute(self, context):
        t = time.monotonic()
        self.value, ctx = self.sup.next(context)
        self.ctx = merge(context, ctx)
        self.elapsed_time = (time.monotonic() - t)
        return self


class FluxUtils:

    @staticmethod
    def default_error_resume(value, ex, context):
        raise value

    @staticmethod
    def default_action(value, context):
        pass

    @staticmethod
    def default_predicate(value, context):
        return True

    @staticmethod
    def default_success(value, context):
        pass

    @staticmethod
    def default_error(e, context):
        traceback.print_exception(type(e), e, e.__traceback__)
        raise e

    @staticmethod
    def default_finish(context):
        pass

    @staticmethod
    def try_or(statement, *args) -> FluxValue:
        try:
            v = statement(*args)
            if v is None:
                return FluxValue.empty()
            return FluxValue.value(v)
        except Exception as ex:
            return FluxValue.error(ex)
