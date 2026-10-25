# Mapa de dependencias del middleware

Qué responsabilidades distribuidas resuelve Ray por nosotros en este prototipo, bajo qué supuestos, con qué evidencia lo comprobamos y qué tendrá que decidir la versión sin Ray.

Cada fila parte de una **necesidad del orquestador**, no de una función de Ray. La última columna es la agenda de diseño de la versión nativa.

## Lo que hace Ray

| Necesidad del orquestador | Qué la resuelve en este prototipo | Supuesto o configuración | Evidencia | Pregunta para la versión nativa |
|---|---|---|---|---|
| **Ejecutar una instancia en otro proceso** (*remote execution*) | Ray crea un actor `Ticker` por instancia en un proceso worker propio | Un actor por instancia; la carga es código incluido en el proyecto, no un comando arbitrario | PID distintos del driver y entre sí, confirmados por la API de estado de Ray ([F0](F0.md)) | ¿Qué agente en cada worker arranca y detiene procesos, y con qué protocolo recibe las órdenes? |
| **Conservar estado entre llamadas** (*stateful worker*) | El actor guarda su contador en la memoria de su proceso | Ese estado puede perderse; nadie lo necesita sobrevivir | El reemplazo empieza de cero: 19 → 1 ([F1](F1.md)), 19 → 1 ([F3](F3.md)) | ¿Alguna carga necesitará conservar estado tras un reemplazo? Si sí, ¿dónde se guarda? |
| **Elegir dónde corre cada instancia** (*worker selection*) | El scheduler de Ray (raylet) coloca el actor en un nodo con recursos libres | La colocación puede variar; ninguna comprobación depende de un nodo concreto | Con 2 CPU, la 3.ª instancia espera en `PENDING` sin duplicarse (`placement_check`, `watch.py`) | ¿Quién decide la colocación y cómo conoce la capacidad libre de cada worker? |
| **Reservar recursos** (*resource scheduling*) | Recursos lógicos de Ray: `num_cpus` por actor | Lógicos: no limitan el consumo real ni aíslan procesos. `num_cpus` debe declararse explícitamente | Las CPU vuelven tras cada terminación en ≤0,001 s ([F0](F0.md)) | ¿Cómo se reserva y libera capacidad? ¿Se necesita aislamiento real (cgroups, contenedores)? |
| **Restringir la ubicación** (*placement constraint*) | Un recurso propio de Ray con el nombre de la etiqueta; los nodos lo declaran con capacidad 1000 | La etiqueta se declara al arrancar el nodo y no cambia | `zone-a` corre; `zone-b` queda `PENDING` 8 ciclos con un solo intento (`placement_check`) | ¿Cómo anuncia un worker sus etiquetas al registrarse? ¿Pueden cambiar en caliente? |
| **Llevar órdenes y respuestas entre procesos** (*transport / serialization*) | Ray serializa argumentos y resultados y los transporta | Argumentos pequeños y serializables; todo en una máquina | Las consultas de salud devuelven PID, nodo y progreso | ¿Qué mensajes existen, con qué formato y qué pasa si uno se pierde o llega duplicado? |
| **Representar un resultado que aún no existe** (*pending result*) | `ObjectRef`: el controlador lanza todas las consultas y espera una sola vez con `ray.wait` | Un único plazo por ronda de consultas | Con un intento colgado, cada ciclo tarda 2,00 s, no 2 s por instancia ([F3](F3.md)) | ¿Consultas en paralelo con un plazo global, o cada worker informa periódicamente (*heartbeats*)? |
| **Saber que un proceso murió** (*failure reporting*) | Ray (GCS) detecta la muerte y la siguiente consulta recibe `ActorDiedError` | Ray detecta la muerte de forma fiable en una sola máquina | Una caída se confirma en la primera consulta, sin pasar por `UNKNOWN` ([F1](F1.md)) | Sin Ray no habrá confirmación: ¿cómo se distingue "muerto" de "lento" o de "red caída"? ¿Basta con sospecha acumulada (K consultas)? |
| **Distinguir un fallo temporal** | `ActorUnavailableError` (subclase de `RayActorError`) se trata como `UNRESPONSIVE` | Nunca apareció en las pruebas | No ejercitado: Ray informó siempre `ActorDiedError` | ¿Qué errores de red se tratan como "quizá vuelva" y cuáles como "ya no existe"? |
| **Terminar un proceso** | `ray.kill` y confirmación posterior por consulta | Ray puede matar un proceso aunque esté colgado | El actor colgado se terminó y solo quedaron 2 procesos vivos ([F3](F3.md)) | Si el worker es inaccesible, no se le puede matar: ¿cómo se evita que su instancia siga ejecutándose junto a su reemplazo? |
| **Reintentar o reiniciar tras un fallo** (*retry*) | Desactivado a propósito: `max_restarts=0`, `max_task_retries=0` | El reemplazo lo decide nuestro controlador, no Ray | Exactamente un reemplazo, que nombra al intento fallido (F1, F3) | ¿Qué operaciones son seguras de repetir y cómo se detectan duplicados? |
| **Saber qué nodos existen** (*membership*) | Ray registra los nodos al arrancar el clúster | Un único nodo fijo; nadie entra ni sale | No probado: no hay cambios de nodos | ¿Cómo se registra un worker, cómo se detecta que se fue y qué pasa con sus instancias? |
| **Observar el sistema** (*observability*) | API de estado de Ray (`list_actors`) y dashboard | Requiere `ray[default]` y que el dashboard arranque | La API de estado confirma los PID de nuestros intentos en cada experimento | ¿Qué métricas y registros expone cada componente propio? |

## Lo que no viene de Ray

Estas partes son nuestro código y deben sobrevivir sin cambios de comportamiento a la versión nativa:

| Responsabilidad | Dónde | Evidencia |
|---|---|---|
| Validar la especificación y los argumentos | `orchestrator/model.py`, `api.py` | 15 casos inválidos y 4 argumentos inválidos (`tests/test_contract.py`) |
| Guardar el objetivo separado de lo observado | `orchestrator/api.py` | El objetivo no cambió durante ningún fallo (F1, F3) |
| Decidir cada transición de estado | `orchestrator/api.py` (`report_*`, `reconcile`) | Tabla de transiciones del [contrato](CONTRACT.md), probada en `tests/` |
| No duplicar intentos pendientes | `reconcile` | Un `PENDING` no se duplica en 8 ciclos (`placement_check`) |
| Abandonar un intento solo tras K consultas perdidas | `reconcile` | Abandono a los 7,0 s con 1, 2 y 3 consultas perdidas ([F3](F3.md)) |
| Registrar por qué existe cada intento | `orchestrator/api.py` | Motivo `replacement` y referencia al intento fallido (F1, F3) |
| Envío repetible con el mismo ID | `submit_job` | `tests/test_contract.py` |
| Medir tiempos sin depender del reloj de pared | Marca `m` de cada evento, `orchestrator/evidence.py` | Tiempos de detección y recuperación de F1 y F3 |

## Lo que aprendimos de Ray al ejecutarlo

- **`runtime_env` puede retener recursos.** Pasar `PYTHONPATH` por `runtime_env` dejó las CPU de los actores terminados reservadas más de 40 s. La versión final no lo usa.
- **Pedir la terminación no es confirmarla.** `ray.kill` solo la solicita; la confirmación llega con la siguiente consulta.
- **`ActorUnavailableError` es subclase de `RayActorError`.** Si se captura en el orden equivocado, un fallo temporal se confunde con una muerte.
- **El reloj de pared salta.** Con el reloj de Windows desfasado, el de WSL oscilaba ±86 s y los plazos internos de Ray abortaban el arranque. Toda duración propia se mide con un reloj monotónico.
- **La convergencia la marca el ciclo de control, no Ray.** Toda fase converge en dos ciclos ([F0](F0.md)); comparar con la versión nativa exige usar el mismo ciclo.
