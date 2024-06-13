import logging
import threading
import time
import concurrent.futures as cr
from datetime import timedelta, datetime
from functools import partial
from statistics import mean

from jsonmerge import merge
from python_flux.flux_utilis import FluxUtils as fu, FluxValue, Register
from python_flux.subscribers import SSubscribe


class Flux(object):

    def filter(self, predicate):
        """
        Permite filtrar un flujo perimtiendo continuar a aquellos valores que cumplen con el
        predicado que se indica.

        :param predicate: función(valor, contexto) que retorna un booleano
        """
        return FFilter(predicate, self)

    def map(self, map_function):
        """
        Permite modificar el valor de un flujo.
        Recibe una función(valor, contexto) que se evaluará para cada elemento del flujo y se substiuirá en el mismo
        por le obtenido de la función.

        :param map_function: función(valor, contexto) desde donde se obtendrá el valor a subsituir.
        """
        return FMap(map_function, self)

    def map_if(self, predicate, map_function):
        """
        Evalúa el predicado, si este es verdadero mapea el valor actual utilizando la función de mapeo.

        :param predicate: función(valor, contexto) predicado que se evalúa.
        :param map_function: función(valor, contexto) desde donde se obtendrá el valor a subsituir.
        """
        return FMapIf(predicate, map_function, self)

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

    def do_on_next(self, on_next, on_error=fu.default_action, exec_async=True):
        """
        Ejecuta par cada elemento del flujo una acción de forma asincrónica. No afecta a los valores
        del flujo.

        :param on_next: función(valor, contexto) se ejecuta para cada valor del flujo
        :param on_error: función(ex, contexto) se ejecuta en caso de error
        :param exec_async: Booleano que indica si las funciones se ejecutarán de forma asíncrona
        """
        return FDoOnNext(on_next, on_error, exec_async, self)

    def delay_ms(self, delay_ms, predicate=fu.default_predicate):
        """
        Retrasa en delay milisegundos la ejecución de paso del flujo. Esto marca el tiempo en que
        se procesarán sus elementos.

        :param delay_ms: Delay en milisegundos que se retrasará el procesamiento de los elementos del flujo
        :param predicate: Si es verdadero se aplica el delay establecido
        """
        return FDelayMs(delay_ms, predicate, self)

    def rate(self, rate, must_apply=fu.default_predicate):
        """
        Fuerza un flujo constante de procesamiento de elementos en el stream.
        Para esto una vez fijado el rate de procesamiento en RPS, se mide el tiempo de producción
        de cada elemento y si este es inerior al indicado por el rate se espera el tiempo suficiente para asegurar
        el rate configurado.
        Si el tiempo de procesamiento es superior a lo indicado por el rate se procesa si espera el mensaje.

        :param rate: Delay en milisegundos que se retrasará el procesamiento de los elementos del flujo
        :param must_apply: Si es verdadero se aplica el delay establecido
        """
        return FRateRPS(self, rate=rate, must_apply=must_apply)

    def chunks(self, n):
        """
        Emite un valor construido como una lista de como máximo n valores obtenidos del flujo padre.
        Si algún valor mientras se construye el chunk da error se emiten los elementos recolectados
        y luego se propaga el error.

        :param n: Cantidad de elementos recolectados antes de emitir el evento de lista.
        """
        return FChunks(n, self)

    def take(self, n, predicate=None):
        """
        Corta la ejecución del flujo luego de n elementos que cumplan la condición dada por predicate.
        Si predicate es None todo los valores son considerados.

        :param n: Cantidad de elementos procesados antes de cortar el flujo
        :param predicate: función(value, context) Si esta función retorna verdadero ese elemento es tomado en cuenta
        """
        return FTake(n, predicate, self)

    def take_until_seconds(self, n):
        """
        Corta la ejecución del flujo luego de n segundos de procesamiento.
        La ejecición se cortará si al momento de evaluar este paso el tiempo transcurrido fue superior
        al configurado.

        :param n: Cantidad de segundos que el stream procesará elementos.
        """
        return FTakeDuringTimeDelta(n, self)

    def log(self, build_log_message=lambda v, c: str(v), build_error_message=lambda e, c: str(e), level=logging.INFO):
        """
        Recibe una función(valor, contexto) que retorna un mensaje de texto que será logueado con el
        nivel de log indicado en level.
        El logueo se hace de forma asincrónica y no afecta al flujo.

        :param build_log_message: Función que dado un valor y contexto retorna un mensaje a loguear
        :param build_error_message: Función que dada una excepción y el contexto retorna el mensaje a loguear como ERROR
        :param level: Nivel de logueo que se usará
        """
        def log_function(l, msg, v, c):
            message = msg(v, c)
            if l is logging.WARNING or l is logging.WARN:
                logging.warning(message)
            elif l is logging.INFO:
                logging.info(message)
            elif l is logging.DEBUG:
                logging.debug(message)

        def log_error(msg, e, c):
            message = msg(e, c)
            logging.error(message)

        return FDoOnNext(partial(log_function, level, build_log_message), partial(log_error, build_error_message), True, self)

    def parallel(self, pool_size=5, join_timeout=0.001, metric_function=None, metric_rate=10, metric_buffer_size=10):
        """
        Ejecuta la obtención de elementos del stream en paralelo

        :param pool_size: Pool de hilos a utilizar
        :param join_timeout: Tiempo de espera de join en segundos por cada hilo (decimal)
        :param metric_function: función(metrics, contexto) Esta es una función que si se setea recibe con cada elemento
        procesado un bloque de métricas sobre la ejecución paralela y el contexto del último elemento recibido
        :param metric_rate: Cantidad de evaluaciones por segundo que se ejecuta la función metric_function.
        Si es None se ejecuta siempre
        :param metric_buffer_size: Tamaño del buffer utilizado para generar las métricas recibidas por metric_function
        """
        return FParallel(pool_size, join_timeout, metric_function, metric_rate, metric_buffer_size, self)

    def on_empty_resume(self, resume=fu.default_finish):
        """
        En caso de empty, este paso ejecuta la función resume que debe retornar el valor a retornar.

        :param resume: función(contexto) Debe retornar el valor a substituir en el flujo.
        """
        return FOnEmptyResume(resume, self)

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

    def subscribe(self, context={}, on_error='BREAK', on_empty='SKIP'):
        """
        Crea un objeto iterable a partir del flujo. Si se itera sobre este objeto se obtendrán
        los valores del flujo.
        :param on_error: Política al obtener un error.
                         THROW=Lanza el error
                         SKIP=Ignora el error y busca el siguiente valor
                         BREAK=Detiene la ejecución sin error
        :param on_empty: Política al obtener un empty.
                         NONE=Retorna un valor None
                         SKIP=Ignora el empty y busca el siguiente valor
        :param context: Contexto inicial para el flujo
        :return: Objeto iterable
        """
        return SSubscribe(on_error, on_empty, context, self)

    def foreach(self, on_success=fu.default_success, on_error=fu.default_error, on_finish=fu.default_finish, context={}):
        """
        Itera sobre los elementos del flujo e invoca a funciones on_success y on_error dependiendo
        el estado del flujo.
        :param on_success: función(valor, contexto) se invoca si el flujo procesa correctamente un valor
        :param on_error:  función(ex, valor, contexto) se invoca si hay un error en el flujo.
                          Esto no corta el procesamiento a menos que se lance una excepción en el método
        :param on_finish: función(contexto) se invoca al finalizar el flujo
        :param context: Contexto inicial para el flujo
        """
        flux = self.subscribe(context, 'THROW', 'SKIP')
        while True:
            try:
                value = next(flux)
                fu.try_or(partial(on_success), value, context)
            except StopIteration:
                fu.try_or(partial(on_finish), context)
                return
            except Exception as ex:
                _, e = fu.try_or(partial(on_error), ex, value, context)

    def to_list(self, context={}, on_error='BREAK', on_empty='SKIP'):
        """
        Itera sobre los elementos del flujo y los retorna todos dentro de una lista.
        :param context: contexto inicial para el flujo
        :param on_error: Política al obtener un error.
                         THROW=Lanza el error
                         SKIP=Ignora el error y busca el siguiente valor
                         BREAK=Detiene la ejecución sin error
        :param on_empty: Política al obtener un empty.
                         NONE=Retorna un valor None
                         SKIP=Ignora el empty y busca el siguiente valor
        :return: Lista de elementos
        """
        return list(iter(self.subscribe(context, on_error, on_empty)))

    def collect(self, init=lambda c: {}, reduce=lambda v, a: a, context={}):
        """
        Permite agrupar en un sólo objeto el resultado del procesamiento de todos los elementos del flujo.
        Se inicializa un acumulador a través de la función init(contexto) y luego para cada elemento del flujo
        se invoca la función reduce(valor, acumulador) que procesa el valor y retorna un nuevo valor del acumulador.
        :param init: función(contexto) Retorna valor inicial del acumulador
        :param reduce: funcón(valor, acumulador) Dados el nuevo valor y el acumulador retorna un nuevo valor de acumulador.
        :param context: Contexto inicial para el flujo
        :return: Acumulador
        """
        acum = init(context)
        flux = self.subscribe(context, 'THROW', 'SKIP')
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
        value, ctx = self.upstream.next(context)
        return value, ctx


class FFilter(Stream):
    def __init__(self, p, flux):
        super().__init__(flux)
        self.predicate = p

    def next(self, context):
        value, ctx = super(FFilter, self).next(context)
        if value.is_error():
            return value, ctx

        v = fu.try_or(partial(self.predicate), value.val(), ctx)
        if v.is_error():
            return v, ctx

        if v.is_empty():
            return v, ctx

        if not v.val():
            return FluxValue.empty(), ctx
        return value, ctx


class FMapIf(Stream):
    def __init__(self, p, f, flux):
        super().__init__(flux)
        self.predicate = p
        self.function = f

    def next(self, context):
        value, ctx = super(FMapIf, self).next(context)
        if value.is_error():
            return value, ctx
        if value.is_empty():
            return value, ctx
        valid = fu.try_or(partial(self.predicate), value.val(), ctx)
        if valid.is_error():
            return valid, ctx
        if valid.val():
            v = fu.try_or(partial(self.function), value.val(), ctx)
            if v.is_error():
                return v, ctx
            return v, ctx
        return value, ctx


class FTake(Stream):
    def __init__(self, count, predicate, flux):
        super().__init__(flux)
        self.count = count
        self.predicate = predicate
        self.idx = 0

    def next(self, context):
        value, ctx = super(FTake, self).next(context)
        if value.is_error() or value.is_empty():
            return value, ctx
        if self.predicate is not None:
            valid = fu.try_or(partial(self.predicate), value.val(), ctx)
            if valid.is_error():
                return valid, ctx
            if valid.val():
                self.idx = self.idx + 1
        else:
            self.idx = self.idx + 1
        if self.idx <= self.count:
            return value, ctx
        else:
            return FluxValue.stop(), ctx


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

        value, ctx = super(FTakeDuringTimeDelta, self).next(context)
        if self.starttime + self.timedelta >= datetime.utcnow():
            return value, ctx
        else:
            return FluxValue.stop(), ctx


class FChunks(Stream):
    def __init__(self, count, flux):
        super().__init__(flux)
        self.count = count
        self.buffer = []
        self.stop_iteration = None
        self.last_context = None

    def prepare_next(self):
        if self.stop_iteration is None:
            super(FChunks, self).prepare_next()

    def next(self, context):
        if self.stop_iteration is not None:
            v = self.stop_iteration
            self.stop_iteration = None
            return v, context
        value, new_ctx = super(FChunks, self).next(context)
        if not value.is_value():
            self.stop_iteration = value
            if len(self.buffer) > 0:
                buf = self.buffer.copy()
                self.buffer = []
                return FluxValue.value(buf), self.last_context
            return value, context
        self.last_context = new_ctx
        self.buffer.append(value.val())
        if len(self.buffer) < self.count:
            return FluxValue.empty(), context
        else:
            buf = self.buffer.copy()
            self.buffer = []
            return FluxValue.value(buf), self.last_context


class FDelayMs(Stream):
    def __init__(self, delay, predicate, flux):
        super().__init__(flux)
        self.delay = delay
        self.predicate = predicate

    def next(self, context):
        value, ctx = super(FDelayMs, self).next(context)
        if value.is_error():
            return value, context
        valid = fu.try_or(partial(self.predicate), value, ctx)
        if valid.is_error():
            return valid, context
        if valid.val():
            time.sleep(self.delay / 1000)
        return value, ctx


class FRateRPS(Stream):
    def __init__(self, rate, must_apply, flux):
        super().__init__(flux)
        self.rate = rate
        self.predicate = must_apply

    def next(self, context):
        start_time = time.monotonic()
        value, ctx = super(FRateRPS, self).next(context)
        if value.is_error():
            return value, ctx
        end_time = time.monotonic()

        valid = fu.try_or(partial(self.predicate), value, ctx)
        if valid.is_error():
            return valid, ctx
        if valid.val():
            delay = (1 / self.rate) - (end_time - start_time)
            if delay > 0:
                time.sleep(delay)
        return value, ctx


class FParallel(Stream):
    def __init__(self, pool_size, join_timeout, metric_func, metric_rate, metric_buffer_size, flux):
        super().__init__(flux)
        self.pool_size = pool_size
        self.join_timeout = join_timeout
        self.metric_func = metric_func
        self.metric_rate = metric_rate
        self.metric_buffer_size = metric_buffer_size
        self.executor = cr.ThreadPoolExecutor(max_workers=pool_size)
        self.semaphore = threading.BoundedSemaphore(pool_size)
        self.futures = []
        self.buffer_metrics = []
        self.last_metric_execution = time.monotonic()
        self.waiting_to_stop = False

    def __show_metrics(self, elapsed_time, ctx):
        if self.metric_func is not None:
            now = time.monotonic()
            self.buffer_metrics.append((len(self.futures), elapsed_time, now))
            len_buffer = len(self.buffer_metrics)
            if len_buffer >= self.metric_buffer_size:
                self.buffer_metrics.pop(0)

            if len_buffer > 0:
                diff_time = max(map(lambda p: p[2], self.buffer_metrics)) - min(map(lambda p: p[2], self.buffer_metrics))
                metrics = {
                    'pool_size': self.pool_size,
                    'used_pool': len(self.futures),
                    'avg_used_pool': round(mean(map(lambda p: p[0], self.buffer_metrics)), 2),
                    'avg_task_time_ms': round(mean(map(lambda p: p[1], self.buffer_metrics)) * 1000, 2),
                    'rate_rps': round(len(self.buffer_metrics) / diff_time if diff_time > 0 else 0.0, 2)
                }
                if self.metric_rate is None or (now - self.last_metric_execution) > (1 / self.metric_rate):
                    self.metric_func(metrics, ctx)
                    self.last_metric_execution = now
        self.last_metric_execution = now

    def prepare_next(self):
        pass

    def next(self, context):
        if not self.waiting_to_stop:
            self.semaphore.acquire()
            try:
                future = self.executor.submit(Register(super(FParallel, self)).execute, context)
                self.futures.append(future)
            except Exception as e:
                self.semaphore.release()
                raise e
            else:
                future.add_done_callback(lambda x: self.semaphore.release())
        if self.waiting_to_stop and len(self.futures) == 0:
            return FluxValue.stop(), context
        futures = self.futures.copy()
        try:
            for future in cr.as_completed(futures, timeout=self.join_timeout):
                self.futures.remove(future)
                r = future.result()
                self.__show_metrics(r.elapsed_time, r.ctx)
                if not self.waiting_to_stop and r.value.is_stop():
                    self.waiting_to_stop = True
                    self.executor.shutdown(wait=True)
                    return FluxValue.empty(), context
                if not r.value.is_stop():
                    return r.value, r.ctx
        except TimeoutError:
            pass
        return FluxValue.empty(), context


class FMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = super(FMap, self).next(context)
        if not value.is_value():
            return value, context
        mapped_value = fu.try_or(partial(self.function), value.val(), ctx.copy())
        return mapped_value, ctx


class FMapContext(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = super(FMapContext, self).next(context)
        if value.is_value():
            mapped_context = fu.try_or(partial(self.function), value.val(), ctx.copy())
            if mapped_context.is_error():
                return mapped_context, ctx
            return value, merge(ctx, mapped_context.val())
        return value, ctx


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
        while True:
            if self.current is None:
                value, ctx = super(FFlatMap, self).next(context.copy())
                if not value.is_value():
                    return value, context
                flux_val = fu.try_or(partial(self.function), value.val(), context.copy())
                if flux_val.is_error():
                    return flux_val, context
                self.current = flux_val.val()
                self.current.prepare_next()

            cur_value, cur_ctx = self.current.next(context.copy())
            if cur_value.is_stop():
                self.current = None
                super(FFlatMap, self).prepare_next()
                continue
            if cur_value.is_error():
                return cur_value, context
            return cur_value, cur_ctx


class FOnEmptyResume(Stream):
    def __init__(self, f, flux):
        super().__init__(flux)
        self.on_empty_resume = f

    def next(self, context):
        value, ctx = super(FOnEmptyResume, self).next(context)
        if value.is_stop():
            return value, context
        if value.is_empty():
            v = fu.try_or(partial(self.on_empty_resume), context)
            return v, ctx
        return value, ctx


class FOnErrorResume(Stream):
    def __init__(self, f, exceptions, flux):
        super().__init__(flux)
        self.on_error_resume = f
        self.exceptions = exceptions

    def next(self, context):
        value, ctx = super(FOnErrorResume, self).next(context)
        if value.is_stop():
            return value, context
        if value.is_error(self.exceptions):
            v = fu.try_or(partial(self.on_error_resume), value.err(), value.val(), context)
            return v, ctx
        return value, ctx


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
        value, ctx = super(FOnErrorRetry, self).next(context)
        if value.is_stop():
            return value, context
        while value.is_error(self.exceptions) and self.current_retries > 0:
            d = self.delay_ms(self.retries - self.current_retries) / 1000
            if d > 0:
                time.sleep(d)
            self.current_retries = self.current_retries - 1
            value, ctx = super(FOnErrorRetry, self).next(context)
        return value, ctx


class FDoOnNext(Stream):

    def __init__(self, on_next, on_error, exec_async, flux):
        super().__init__(flux)
        self.on_next = on_next
        self.on_error = on_error
        self.execute_async = exec_async

    def next(self, context):
        value, ctx = super(FDoOnNext, self).next(context)
        if value.is_error():
            if self.execute_async:
                t = threading.Thread(target=self.on_error, args=(value.err(), context,))
                t.start()
            else:
                self.on_error(value.err(), context)
            return value, context
        if self.execute_async:
            t = threading.Thread(target=self.on_next, args=(value.val(), ctx,))
            t.start()
        else:
            self.on_next(value.val(), ctx)
        return value, ctx
