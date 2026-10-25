# Cierre de la fase Ray

Resumen de lo que entrega el prototipo, las respuestas que pide la metodología antes de retirar Ray y las preguntas que quedan para la versión nativa.

## Entregables

| Entregable | Peso | Dónde |
|---|---|---|
| Prototipo funcional con Ray | 7 % | Código en [`Ray-Phase1/`](..); cómo ejecutarlo en el [README](../../README.md) |
| Diagrama de arquitectura | 5 % (con el mapa) | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Mapa de dependencias del middleware | | [DEPENDENCY_MAP.md](DEPENDENCY_MAP.md) |
| Contrato externo congelado | | [CONTRACT.md](CONTRACT.md) (v1.0) |
| Experimento inicial | 3 % | [F1](F1.md) (caída de un actor), [F3](F3.md) (proceso que deja de responder) y la referencia [F0](F0.md), con su [evidencia](evidence/) |

Estado verificado al cerrar: 55 pruebas unitarias en OK y, con Ray 2.58.0 real en WSL2, `two_instances` 8/8, `scale_sequence` 8/8, `placement_check` 6/6, `fault_recovery --fault crash` 10/10 y `fault_recovery --fault hang` 12/12.

## Preguntas de salida

**¿Qué problema resuelve la aplicación?**
Mantener un número deseado de instancias de una carga de larga duración, con recursos y ubicación declarados, a pesar de que los procesos fallen o se cuelguen: compara continuamente el objetivo con lo observado y crea o detiene intentos para acercarlos.

**¿Qué comportamiento debe sobrevivir al rediseño?**
Lo congelado en el [contrato v1.0](CONTRACT.md): las cuatro operaciones, la especificación, los seis estados y sus transiciones, los cuatro errores, las repeticiones seguras y las garantías mínimas (sección 5.1). Las limitaciones de la sección 5.2 no deben sobrevivir: la versión nativa tiene que superarlas.

**¿Qué viene de nuestro código?**
Validar, guardar el objetivo separado de lo observado, decidir cada transición, no duplicar intentos pendientes, abandonar solo tras K consultas, registrar por qué existe cada intento y medir los tiempos. Detalle en la sección "Lo que no viene de Ray" del [mapa](DEPENDENCY_MAP.md).

**¿Qué viene de Ray?**
Crear procesos y colocarlos según recursos y etiquetas, transportar llamadas y respuestas, confirmar la muerte de un proceso, poder matarlo aunque esté colgado y saber qué nodos existen. Detalle en el [mapa](DEPENDENCY_MAP.md).

**¿Qué semántica de fallos se observó, en lugar de suponerse?**

| Situación | Observado | Evidencia |
|---|---|---|
| Un proceso muere | Ray lo confirma en la siguiente consulta; `FAILED` sin pasar por `UNKNOWN`; un reemplazo en ~0,5 s | [F1](F1.md) |
| Un proceso deja de responder | `UNKNOWN` a los 2 s; abandono a los 7 s, tras exactamente 3 consultas perdidas; el proceso colgado se termina; un reemplazo | [F3](F3.md) |
| Se pide detener un intento | Queda en `STOPPING` hasta que Ray confirma la muerte; termina en `STOPPED` y no se reemplaza | [F0](F0.md), `scale_sequence` |
| No hay capacidad o etiqueta | El intento queda `PENDING` indefinidamente, sin duplicarse | `placement_check` |
| Estado local de una instancia | Se pierde siempre con su proceso | F1, F3 |

**Supuesto, no observado:** el manejo de `ActorUnavailableError` (Ray nunca lo emitió), la caída del controlador, la caída de un nodo entero y cualquier fallo de red.

## Lista de comprobación de la lección

**Ejecución.** Cada instancia es un **actor** de Ray (proceso propio, estado local); no usamos tareas sueltas salvo en `watch.py`. Son **concurrentes** las instancias entre sí y las consultas de una ronda (`probe_all`). Son **secuenciales** las operaciones del cliente y las decisiones del controlador, una ronda tras otra.

**Datos.** Solo viajan valores pequeños: argumentos del actor y respuestas de salud (PID, nodo, contador). El estado local es el contador de cada actor. **Nada es duradero:** objetivo, intentos e historial viven en la memoria del driver.

**Planificación.** Se declaran CPU lógica por instancia y, opcionalmente, una etiqueta de nodo. La ubicación importa para la **corrección** cuando hay etiqueta (una instancia nunca corre donde no debe) y solo para el rendimiento en el resto de casos. Sin capacidad, la instancia espera en `PENDING`.

**Fallos.** Ray no reintenta ni reinicia nada (`max_restarts=0`, `max_task_retries=0`): todo reemplazo lo decide el controlador. Repetir las operaciones del cliente es seguro: `set_replicas` es absoluta y `submit_job` con la misma especificación es idempotente. **No sobrevive ningún estado** de la aplicación a la caída del controlador.

**Observabilidad.** Cada consulta devuelve el PID y el nodo del proceso, confirmados por la API de estado de Ray. El historial relaciona cada intento con su trabajo, su proceso, su motivo de creación (`initial`, `scale_up`, `replacement`) y el intento al que reemplaza. Un reemplazo se ve como un nuevo intento con `replaces`.

**Rediseño nativo.**
- *Imprescindible:* arrancar y detener procesos en workers, comunicación controlador ↔ worker, detección de fallos con plazos y sospecha, registro de workers con su capacidad y etiquetas.
- *Se puede omitir:* el object store, la serialización general, las tareas sueltas, el autoescalado y la planificación genérica de Ray; nuestro problema solo necesita colocar procesos según CPU y etiquetas.
- *Debe ser más fuerte que en Ray* (sección 5.2 del [contrato](CONTRACT.md)): la **durabilidad** de todo cambio aceptado, que cambia el significado de "aceptar"; la **garantía de un proceso por réplica** cuando no se pueda matar un proceso inaccesible; y los **envíos repetibles tras un reinicio**.

## Preguntas abiertas para la versión nativa

Ordenadas por impacto en el diseño:

1. **¿Dónde vive el objetivo y cuándo se confirma una escritura?** Hoy se pierde al reiniciar el controlador. Es lo que la campaña de la fase 3 probará con F2 (caída y reinicio).
2. **¿Cómo se detecta que un worker murió sin la confirmación de Ray?** Solo habrá sospecha: plazos, K consultas o *heartbeats*. Hay que elegir valores y justificar el compromiso entre recuperar rápido y no reemplazar procesos lentos.
3. **¿Cómo se garantiza un proceso por réplica si un worker queda inaccesible?** No se le podrá matar. Opciones a estudiar: que el worker detenga sus instancias si pierde contacto con el controlador, o arrendamientos (*leases*) con caducidad.
4. **¿Cómo se registran los workers, anuncian su capacidad y sus etiquetas, y cómo se detecta que se fueron?**
5. **¿Qué protocolo une controlador y workers, y qué pasa con un mensaje perdido o duplicado?** Las órdenes de arrancar y detener deben poder repetirse sin efectos dobles, por ejemplo usando el ID del intento.
6. **¿Hace falta aislamiento real de recursos?** Los recursos de Ray eran lógicos; la versión nativa decidirá si basta con contar o si hay que limitar (cgroups, contenedores).

## Para comparar con la versión nativa

- Usar el **mismo ciclo de control** (0,5 s), el mismo plazo de consulta (2 s) y la misma K (3), o comparar en número de ciclos.
- Repetir F0, F1 y F3 con los mismos criterios y comparar contra la [evidencia](evidence/) de esta fase.
- Las 55 pruebas de `tests/` describen el comportamiento del dominio; la versión nativa debería poder demostrar el mismo comportamiento frente al [contrato](CONTRACT.md).
