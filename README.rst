python-flux
===========

Esta librería implementa una interfaz de programación similar a la brindada por el proyecto **WebFlux** de spring.

La interfaz implementada es similar a la siguiente: `Flux <https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html>`_


filter(predicate, on_mismatch)
------------------------------

Permite filtrar un flujo permitiendo continuar a aquellos valores que cumplen con el predicado que se indica.

Si no se cumple el predicado opcionalmente se puede indicar una función que dado el valor y el contexto actual retorna el valor que se enviará al flujo.

-  **predicate**: función(valor, contexto) que retorna un booleano

-  **on_mismatch**: función(valor, contexto) que retorna un valor alternativo al testeado originalmente


map(map_function)
-----------------

Permite modificar el valor de un flujo.

Recibe una función(valor, contexto) que se evaluará para cada elemento del flujo y se substituirá en el mismo por lo obtenido de la función.

-  **map_function**: función(valor, contexto) desde donde se obtendrá el valor a sustituir.


map_context(map_context_function):
----------------------------------

Permite modificar el contexto de un flujo.

Recibe una función(valor, contexto) que se evaluará para cada elemento del flujo y retornará un nuevo contexto que sustituirá al actual

-  **map_context_function**: función(valor, contexto) desde donde se obtendrá el contexto a subsituir.


flat_map(flatmap_function)
--------------------------

Permite sustituir el valor de un flujo por elementos de otro flujo.

Recibe una función(valor, contexto) que se evaluará para cada elemento del flujo original y retornará un flujo cuyos elementos se usarán como nuevo origen del flujo de datos.

-  **flatmap_function**: función(valor, contexto) desde donde se obtendrá el valor a sustituir.


do_on_next(on_next)
-------------------

Ejecuta para cada elemento del flujo una acción de forma asincrónica.
No afecta a los valores del flujo.

-  **on_next**: función(valor, contexto) que se ejecutará para cada valor del flujo


delay_ms(delay_ms, predicate=fu.default_predicate)
--------------------------------------------------

Retrasa en delay milisegundos la ejecución de paso del flujo. Esto marca el tiempo en que se procesarán sus elementos.

-  **delay_ms**: Delay en milisegundos que se retrasará el procesamiento de los elementos del flujo

-  **predicate**: Si es verdadero se aplica el delay establecido


take(n)
-------

Corta la ejecución del flujo luego de procesar n elementos.

-  **n**: Cantidad de elementos procesados antes de cortar el flujo


take_until_seconds(n)
---------------------

Corta la ejecución del flujo luego de n segundos de procesamiento.

La ejecución se cortará si al momento de evaluar este paso el tiempo transcurrido fue superior al configurado.

-  **n**: Cantidad de segundos que el stream procesa elementos.


log(message=lambda v, c: str(v), level=logging.INFO)
----------------------------------------------------

Recibe una función(valor, contexto) que retorna un mensaje de texto que será logueado con el nivel de log indicado en level.

El logueo se hace de forma asincrónica y no afecta al flujo.

-  **message**: Función que dado un valor y contexto retorna un mensaje a loguear.

-  **level**: Nivel de logueo que se usará


on_error_resume(resume=fu.default_error_resume, exceptions=[Exception])
-----------------------------------------------------------------------
En caso de error, este paso ejecuta la función resume que debe retornar un valor a partir del error recibido por parámetro.

-  **resume**: función(ex, contexto) Debe retornar el valor a sustituir en el flujo.

-  **exceptions**: Lista de excepciones para los que se aplica la función resume.


on_error_retry(retries=3, delay_ms=lambda i: 0, exceptions=[Exception])
-----------------------------------------------------------------------

En caso de error, este paso intenta ejecutar nuevamente el flujo
**retries** cantidad de veces.

Esto lo hace sólo para las excepciones indicadas por **exceptions** y los reintentos tienen un delay dado por la función **delay_ms** que recibe el número de reintento que es y espera obtener los ms de delay que se deben aplicar.

-  **retries**: Cantidad de reintentos a aplicar

-  **delay_ms**: función(int) que retorna dado el número de reintento en el que se está cuantos: ms de delay se deben aplicar.

-  **exceptions**: Lista de excepciones para los que se aplica el método resume.


subscribe(context={})
---------------------

Crea un objeto iterable a partir del flujo. Si se itera sobre este objeto se obtendrán los valores del flujo.

-  **context**: Contexto inicial para el flujo

-  **return**: Objeto iterable


foreach(on_success=fu.default_success, on_error=fu.default_error, context={})
-----------------------------------------------------------------------------

Itera sobre los elementos del flujo e invoca a funciones **on_success** y on_error dependiendo el estado del flujo.

-  **on_success**: función(valor, contexto) se invoca si el flujo procesa correctamente un valor

-  **on_error**: función(ex, contexto) se invoca si hay un error en el flujo. Esto no corta el procesamiento a menos que se lance una excepción en el método

-  **context**: Contexto inicial para el flujo


to_list(context={})
-------------------

Itera sobre los elementos del flujo y los retorna todos dentro de una lista.

-  **context**: contexto inicial para el flujo

-  **return**: Lista de elementos


collect(init=lambda c: {}, reduce=lambda v, a: a, context={})
-------------------------------------------------------------

Permite resumir en un sólo objeto el procesamiento de todos los elementos del flujo.

Se inicializa un acumulador a través de la función init(contexto) y luego para cada elemento del flujo se invoca la función reduce(valor, acumulador) que procesa el valor y retorna un nuevo valor del acumulador.

-  **init**: función(contexto) Retorna valor inicial del acumulador

-  **reduce**: funcón(valor, acumulador) Dados el nuevo valor y el acumulador retorna un nuevo valor de acumulador.

-  **context**: Contexto inicial para el flujo

-  **return**: Acumulador
