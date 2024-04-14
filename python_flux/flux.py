import asyncio
import logging
import types
import time
from datetime import timedelta, datetime
from functools import partial
from jsonmerge import merge
from python_flux.flux_utilis import FluxUtils as fu
from python_flux.subscribers import SSubscribe, SForeach


class Flux(object):

    def filter(self, predicate, on_mismatch=fu.default_action):
        """
        Permite filtrar un flujo perimtiendo continuar a aquellos valores que cumplen con el
        predicado que se indica.
        Si no se cumple el predicado opcionalmente se puede indicar una función que dado el valor y el contexto acutal
        retorna el valor que se enviará al flujo.

        :param predicate: función(valor, contexto) que retorna un booleano
        :param on_mismatch: función(valor, contexto) que retorna un valor alternativo al testeado originalmente
        """
        return FFilter(predicate, on_mismatch, self)

    def map(self, map_function):
        """
        Permite modificar el valor de un flujo.
        Recibe una función(valor, contexto) que se evaluará para cada elemento del flujo y se substiuirá en el mismo
        por le obtenido de la función.

        :param map_function: función(valor, contexto) desde donde se obtendrá el valor a subsituir.
        """
        return FMap(map_function, self)

    def map_context(self, map_context_function):
        """
        Permite modificar el contexto de un flujo.
        Recibe una función(valor, contexto) que se evaluará para cada elemento del flujo y retornará
        un nuevo contexto que subsituirá al actual

        :param map_context_function: función(valor, contexto) desde donde se obtendrá el contexto a subsituir.
        """
        return FMapContext(map_context_function, self)

    def flat_map(self, flatmap_function):
        """
        Permite sustituir el valor de un flujo por elementos de otro flujo.
        Recibe una función(valor, contexto) que se evaluará para cada elemento del flujo original y retornará un
        flujo cuyos elementos se usarán como nuevo origen del flujo de datos.

        flatmap_function: función(valor, contexto) desde donde se obtendrá el valor a subsituir.
        """
        return FFlatMap(flatmap_function, self)

    def do_on_next(self, on_next):
        """
        Ejecuta par cada elemento del flujo una acción de forma asincrónica. No afecta a los valores
        del flujo.

        :param on_next: función(valor, contexto) que se ejecutará para cada valor del flujo
        """
        return FDoOnNext(on_next, self)

    def delay_ms(self, delay_ms, predicate=fu.default_predicate):
        """
        Retrasa en delay milisegundos la ejecución de paso del flujo. Esto marca el tiempo en que
        se procesarán sus elementos.

        :param delay_ms: Delay en milisegundos que se retrasará el procesamiento de los elementos del flujo
        :param predicate: Si es verdadero se aplica el delay establecido
        """
        return FDelayMs(delay_ms, predicate, self)

    def take(self, n):
        """
        Corta la ejecución del flujo luego de n elementos procesados.

        :param n: Cantidad de elementos procesados antes de cortar el flujo
        """
        return FTake(n, self)

    def take_until_seconds(self, n):
        """
        Corta la ejecución del flujo luego de n segundos de procesamiento.
        La ejecición se cortará si al momento de evaluar este paso el tiempo transcurrido fue superior
        al configurado.

        :param n: Cantidad de segundos que el stream procesará elementos.
        """
        return FTakeDuringTimeDelta(n, self)

    def log(self, message=lambda v, c: str(v), level=logging.INFO):
        """
        Recibe una función(valor, contexto) que retorna un mensaje de texto que será logueado con el
        nivel de log indicado en level.
        El logueo se hace de forma asincrónica y no afecta al flujo.

        :param message: Función que dado un valor y contexto retorna un mensaje a loguear.
        :param level: Nivel de logueo que se usará
        """
        def log_function(l, msg, v, c):
            if l is logging.ERROR:
                logging.error(msg(v, c))
            elif l is logging.WARNING or l is logging.WARN:
                logging.warning(msg(v, c))
            elif l is logging.INFO:
                logging.info(msg(v, c))
            elif l is logging.DEBUG:
                logging.debug(msg(v, c))

        return FDoOnNext(partial(log_function, level, message), self)

    def on_error_resume(self, resume=fu.default_error_resume, exceptions=[Exception]):
        """
        En caso de error, este paso ejecuta la función resume que debe retornar un valor
        a partir del error recibido por parámetro.

        :param resume: función(ex, contexto) Debe retornar el valor a substituir en el flujo.
        :param exceptions: Lista de excepciones para los que se aplica el resume.
        """
        return FOnErrorResume(resume, exceptions, self)

    def on_error_retry(self, retries=3, delay_ms=lambda i: 0, exceptions=[Exception]):
        """
        En caso de error este paso reintenta ejecutar el flujo retries veces más. Esto
        lo hace sólo para las excepciones indicadas por exceptions y los reintentos
        tienen un delay dado por la función delay_ms que recibe el número de reintento que
        es y espera obtener los ms de delay que se deben aplicar.

        :param retries: Cantidad de reintentos a aplicar
        :param delay_ms: función(int) que retorna dado el número de reintento en el que se está cuantos
                  ms de delay se deben aplicar.
        :param exceptions: Lista de excepciones para los que se aplica el resume.
        """
        return FOnErrorRetry(retries, exceptions, delay_ms, self)

    def subscribe(self, context={}):
        """
        Crea un objeto iterable a partir del flujo. Si se itera sobre este objeto se obtendrán
        los valores del flujo.
        :param context: Contexto inicial para el flujo
        :return: Objeto iterable
        """
        return SSubscribe(context, self)

    def foreach(self, on_success=fu.default_success, on_error=fu.default_error, context={}):
        """
        Itera sobre los elementos del flujo e invoca a funciones on_success y on_error dependiendo
        el estado del flujo.
        :param on_success: función(valor, contexto) se invoca si el flujo procesa correctamente un valor
        :param on_error:  función(ex, contexto) se invoca si hay un error en el flujo.
                          Esto no corta el procesamiento a menos que se lance una excepción en el método
        :param context: Contexto inicial para el flujo
        """
        flux = self.subscribe(context)
        while True:
            try:
                value = next(flux)
                fu.try_or(partial(on_success), value, context)
            except StopIteration:
                return
            except Exception as ex:
                _, e = fu.try_or(partial(on_error), ex, context)
                if e is not None:
                    raise e

    def to_list(self, context={}):
        """
        Itera sobre los elementos del flujo y los retorna todos dentro de una lista.
        :param context: contexto inicial para el flujo
        :return: Lista de elementos
        """
        return list(iter(SSubscribe(context, self)))

    def collect(self, init=lambda c: {}, reduce=lambda v, a: a, context={}):
        """
        Permite resumir en un sólo objeto el procesamiento de todos los elementos del flujo.
        Se inicializa un acumulador a través de la función init(contexto) y luego para cada elemento del flujo
        se invoca la función reduce(valor, acumulador) que procesa el valor y retorna un nuevo valor del acumulador.
        :param init: función(contexto) Retorna valor inicial del acumulador
        :param reduce: funcón(valor, acumulador) Dados el nuevo valor y el acumulador retorna un nuevo valor de acumulador.
        :param context: Contexto inicial para el flujo
        :return: Acumulador
        """
        acum = init(context)
        flux = self.subscribe(context)
        while True:
            try:
                value = next(flux)
                acum = reduce(value, acum)
            except StopIteration:
                return
            except Exception as ex:
                raise ex


class Stream(Flux):
    def __init__(self, up):
        super(Stream, self).__init__()
        self.upstream = up

    def prepare_next(self):
        self.upstream.prepare_next()

    def next(self, context):
        value, e, ctx = self.upstream.next(context)
        return value, e, ctx


class FFilter(Stream):
    def __init__(self, p, m, flux):
        super().__init__(flux)
        self.predicate = p
        self.on_mismatch = m

    def next(self, context):
        value, e, ctx = super(FFilter, self).next(context)
        if e is not None:
            return value, e, ctx
        valid, e = fu.try_or(partial(self.predicate), value, context)
        while e is None and not valid:
            _, e = fu.try_or(partial(self.on_mismatch), value, context)
            if e is not None:
                return value, e, ctx
            super(FFilter, self).prepare_next()
            value, e, ctx = super(FFilter, self).next(context)
            if e is not None:
                return value, e, ctx
            valid, e = fu.try_or(partial(self.predicate), value, context)
        return value, e, ctx


class FTake(Stream):
    def __init__(self, count, flux):
        super().__init__(flux)
        self.count = count
        self.idx = 0

    def next(self, context):
        value, e, ctx = super(FTake, self).next(context)
        self.idx = self.idx + 1
        if self.idx <= self.count:
            return value, e, ctx
        else:
            return value, StopIteration(), ctx


class FTakeDuringTimeDelta(Stream):
    def __init__(self, td, flux):
        super().__init__(flux)
        if type(td) == timedelta:
            self.timedelta = td
        if type(td) == int or type(td) == float:
            self.timedelta = datetime.timedelta(seconds=td)
        self.starttime = None

    def next(self, context):
        if self.starttime is None:
            self.starttime = datetime.utcnow()

        value, e, ctx = super(FTakeDuringTimeDelta, self).next(context)
        if self.starttime + self.timedelta >= datetime.utcnow():
            return value, e, ctx
        else:
            return value, StopIteration(), ctx


class FDelayMs(Stream):
    def __init__(self, delay, predicate, flux):
        super().__init__(flux)
        self.delay = delay
        self.predicate = predicate

    def next(self, context):
        value, e, ctx = super(FDelayMs, self).next(context)
        if e is not None:
            return value, e, ctx
        valid, e = fu.try_or(partial(self.predicate), value, context)
        if e is not None:
            return value, e, ctx
        if valid:
            time.sleep(self.delay / 1000)
        return value, e, ctx


class FMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, e, ctx = super(FMap, self).next(context)
        if e is not None:
            return value, e, ctx
        mapped_value, e = fu.try_or(partial(self.function), value, ctx.copy())
        if e is not None:
            return value, e, ctx
        return mapped_value, e, ctx


class FMapContext(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, e, ctx = super(FMapContext, self).next(context)
        if e is not None:
            return value, e, ctx
        mapped_context, e = fu.try_or(partial(self.function), value, ctx.copy())
        if e is not None:
            return value, e, ctx
        return value, e, merge(ctx, mapped_context)


class FFlatMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func
        self.current = None

    def prepare_next(self):
        if self.current is None:
            super(FFlatMap, self).prepare_next()
        else:
            self.current.prepare_next()

    def next(self, context):
        ctx = context.copy()
        while True:
            while self.current is None:
                value, e, ctx = super(FFlatMap, self).next(ctx)
                if e is not None:
                    return None, e, ctx
                func, e = fu.try_or(partial(self.function), value, ctx)
                if e is not None:
                    return None, e, ctx
                fgen = func
                if isinstance(func, types.GeneratorType):
                    from python_flux.producers import PFromGenerator
                    fgen = PFromGenerator(func)
                self.current = fgen
                self.current.prepare_next()

            cur_value, cur_e, cur_ctx = self.current.next(ctx)
            if cur_e is None:
                return cur_value, cur_e, cur_ctx

            if isinstance(cur_e, StopIteration):
                self.current = None
                super(FFlatMap, self).prepare_next()


class FOnErrorResume(Stream):
    def __init__(self, f, exceptions, flux):
        super().__init__(flux)
        self.on_error_resume = f
        self.exceptions = exceptions

    def next(self, context):
        value, e, ctx = super(FOnErrorResume, self).next(context)
        if e is None:
            return value, e, ctx
        if isinstance(e, StopIteration):
            return None, e, ctx
        if any([isinstance(e, ex) for ex in self.exceptions]):
            v, e = fu.try_or(partial(self.on_error_resume), e, ctx)
            if e is not None:
                return value, e, ctx
            value = v
        return value, None, ctx


class FOnErrorRetry(Stream):
    def __init__(self, retries, exceptions, delay_ms, flux):
        super().__init__(flux)
        self.retries = retries
        self.exceptions = exceptions
        self.delay_ms = delay_ms
        self.current_retries = retries

    def prepare_next(self):
        super(FOnErrorRetry, self).prepare_next()
        self.current_retries = self.retries

    def next(self, context):
        value, e, ctx = super(FOnErrorRetry, self).next(context)
        while e is not None and self.current_retries > 0:
            d = self.delay_ms(self.retries - self.current_retries) / 1000
            if d > 0:
                time.sleep(d)
            self.current_retries = self.current_retries - 1
            value, e, ctx = super(FOnErrorRetry, self).next(context)
        return value, e, ctx


class FDoOnNext(Stream):
    async def __async_coroutine(self, f, value, context):
        f(value, context)

    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, e, ctx = super(FDoOnNext, self).next(context)
        if e is not None:
            return value, e, ctx
        asyncio.run(self.__async_coroutine(self.function, value, ctx))
        return value, e, ctx
