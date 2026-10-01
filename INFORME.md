# Informe - TP Coordinación

## 1. Resumen del diseño

Para resolver el procesamiento distribuido, la logica se dividio en 3 etapas:

- **Sum**: toma los registro de una cola compartida y acumula subtotales por fruta por cada cliente.

- **Aggregation**: realiza la suma total de las suman parciales que le mandan los Sums. Cada replica de Aggregation se encarga de un subconjunto de frutas segun un hash y calcula un top parcial.

- **Join**: recibe los tops parciales de cada Aggregator, los junta y arma el top final. Finalmente, lo envia al Gateway.

Para que el sistema soporte multiples cleintes a la vez sin mezclar la informacion, los mensajes tieenen un client_id. De esta manera, cada nodo guarda en diccionarios el estado seprado por cleintes.

Además, use 2 tipos de mensajes:
- `DATA`: tiene el contenido a procesar(registro de frutas, subtotales, etc).
- `EOF`: indica el final de la transmision de un cliente y le avisa al nodo que ya  no va a recibir mas datos datos y puede hacer los calculos necesarios para pasarlo a la siguiente etapa.



## 2. Coordinación entre las réplicas de Sum

El problema con Sum era que todas las replicas leian de una misma cola. Cada fila de los archivos de input la tomaba un solo Sum, pero el EOF de fin de archivo tambien lo consume uno solo, lo que probocaba que las demas réplicas se quedaban con datos  acumulados que nunca se iban a mandar. Para coordinar eso, hice que el `MessageHandler` del Gateway cuente cuántos registros de datos manda el cliente y adjunta ese total en el mensaje del EOF.

Cuando a un sum le llega EOF, no lo procesa solo: lo reenvia a un exchange de control llamado (`SUM_CONTROL_EXCHANGE`) usando las routing keys de todas las replicas. Cada sum esta suscripto a su propia clave, por lo que todas las replicas se informan de que el cliente termino.

Al recibir esa señal de control, la replica registra el total esperado y hace un flush, le manda a los Aggregators lo que tiene acumulado hasta ese momento y la cantidad de mensajes que proceso. Si llega a procesar algun mensaje despues de ese aviso, lo acumula y manda otro flush con el nuevo conteo.  

A nivel implementación, cada réplica corre dos threads: uno escuchando la cola de datos y otro la cola del exchange de control. Para que no se pisen al tocar las sumas o los contadores por clientes, uso un  `Lock`. 

## 3. Flujo entre Sum, Aggregation y Join

- **Distribución por hash:** Para que una misma fruta no termine partida en distintos Aggregators, cada fruta se manda a `crc32(fruta) % AGGREGATION_AMOUNT`. Use `zlib.crc32` porque la función `hash()` de Python cambia la semilla entre procesos distintos, lo que hacía que dos Sum mandaran la misma fruta a diferentes Aggregators. 

- **Fin de Aggregation:** Cada vez que un Sum hace un flush, le manda un mensaje a todos los Aggregators con la cantidad de filas procesadas (aunque no tenga frutas para alguno en particular). Los Aggregators van sumando esas cantidades por cliente. Cuando la cuenta llega al `total` que venía en el aviso, el Aggregator sabe que ya no queda ningún dato en ningún Sum. Ahí arma su top parcial usando los operadores de `FruitItem`, se lo manda al Join y limpia la memoria de ese cliente.

- **Join:** Va contando los mensajes de fin que le llegan de los Aggregators para cada cliente. Cuando junta `AGGREGATION_AMOUNT` avisos, agarra todas las frutas recibidas, las ordena de mayor a menor, recorta según el `TOP_SIZE` configurado y le manda el top definitivo al Gateway con su `client_id`.

## 4. Modificaciones en el Middleware

Cambios realizados:

- Agregue **`prefetch_count = 1`** en ambos constructores. Sin esto, RabbitMQ puede entregar casi todos los mensajes al primer consumidor y las demás réplicas quedan ociosas.
- **Clase base común (`_RabbitMQMiddleware`):** Tanto `MessageMiddlewareQueueRabbitMQ` como `MessageMiddlewareExchangeRabbitMQ` compartían gran parte de su lógica: abrir la conexión con `BlockingConnection`, crear el canal, configurar el QoS, detener el consumo de forma segura con `add_callback_threadsafe` y despachar los acks. Extraje toda esa lógica repetidatal como me indicaron en las correcciones del tp1, a una clase base, dejando en las clases hijas únicamente la declaración puntual(colas directas o exchanges con sus bindings).
- **Traducción centralizada de errores (`_translate_pika_errors`):** Implementé un context manager con `@contextlib.contextmanager` para no duplicar bloques `try/except` idénticos en cada llamada a RabbitMQ. Al envolver las operaciones con este manejador, cualquier fallo se atrapa y se relanza como una excepción propia de la interfaz (`MessageMiddlewareDisconnectedError` o `MessageMiddlewareMessageError`).
- **Manejo de errores en confirmaciones (`ack` y `nack`):** En `MessageAcknowledger`, tanto `ack()` como `nack()` quedaron cubiertos por el context manager para capturar posibles caídas de conexión durante la confirmación de mensajes.
- **Colas del exchange declaradas en el constructor**: En el exchange direct, si  se publica a una routing key que todavía no tiene una cola atada, RabbitMQ descarta el mensaje silenciosamente. En el tp anterior, las colas se creaban recién en `start_consuming()`, así que si un Sum arrancaba a pasar datos antes de que Aggregation se conectara, se perdían mensajes. Lo modifiqué haciendo el `queue_declare` y el `queue_bind` de todas las claves en el `__init__`. 

- **Cierre limpio entre hilos:** Para poder frenar el consumo desde los handlers de `SIGTERM` o entre threads sin romper los sockets, implemente el `stop_consuming` usando `add_callback_threadsafe`. Además, cada hilo usa su propia instancia de middleware y el `close()` se llama una vez que el loop de consumo haya salido.

## 5. Análisis de escalabilidad

**Respecto a los clientes.** 
Todo el estado está indexado por `client_id`, así que los clientes concurrentes no interfieren entre sí y no hay ningún punto que serialice clientes. Varios clientes pueden mandar datos a la vez intercalándose en las colas sin bloquearse ni mezclarse. 

**Respecto a grandes volúmenes de datos.**

- Los datos se procesan de a un mensaje, sin cargar archivos completos en memoria.

- Los Sum reducen el volumen: por cliente guardan una entrada por fruta distinta, no una por registro. La memoria crece con la cantidad de frutas distintas, no con la cantidad de registros.

- Las réplicas de Sum se reparten los datos de la cola compartida, y `prefetch_count = 1` hace que todas reciban trabajo. Agregar réplicas de Sum aumenta el throughput de ingesta.

**Respecto a la cantidad de controles.**
Los mensajes de coordinación por cliente son:

- `SUM_AMOUNT` avisos por exchange para notificar los EOFs.

- Al menos `SUM_AMOUNT × AGGREGATION_AMOUNT` mensajes de sincronización entre Sum y Aggregation).

- `AGGREGATION_AMOUNT` mensajes al Join.


el costo de coordinación es independiente del volumen de datos y depende solo de la cantidad de réplicas. 

