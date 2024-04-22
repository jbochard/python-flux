import traceback


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
    def try_or(statement, *args) -> (object, Exception):
        try:
            v = statement(*args)
            return v, None
        except Exception as ex:
            return None, ex
