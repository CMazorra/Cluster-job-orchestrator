# Contrato externo — v1.0 (congelado)

Comportamiento visible del orquestador, congelado al cerrar la fase Ray. Es la guía de la versión nativa, y distingue dos cosas:

- **Lo congelado** (secciones 2 a 4): operaciones, especificación, estados, transiciones, errores y repeticiones. La versión nativa debe comportarse igual; cualquier cambio se justifica por escrito al comparar versiones.
- **Las garantías** (sección 5): la versión Ray marca un **mínimo**, no un techo. Algunas de sus limitaciones **deben** superarse en la versión nativa; otras son decisiones de diseño que esta tendrá que justificar.

El contrato describe operaciones y datos del dominio. No depende de actores, `ObjectRef`, nombres de Ray ni de un transporte concreto.

## 1. Forma de uso

En el prototipo, las operaciones son funciones Python de `Orchestrator`. En la versión nativa, un cliente podrá traducirlas al protocolo que se elija (HTTP, gRPC…). Lo que se congela es su **significado**, no la forma de invocarlas.

## 2. Modelo y operaciones

Un **trabajo** mantiene un número deseado de instancias de una carga de larga duración. Una **instancia** es un intento concreto: si falla, su reemplazo es otro intento con otra identidad. El ID de un intento es `<job_id>/<n>`, con `n` creciente dentro del trabajo y nunca reutilizado.

| Operación | Entrada | Resultado o efecto |
|---|---|---|
| `submit_job(spec)` | Especificación con el ID que elige el cliente | Registra el objetivo y devuelve `job_id`. Repetir la misma especificación devuelve el mismo `job_id` |
| `get_job(job_id)` | ID de trabajo | Objetivo, recuento de intentos activos y todos los intentos del trabajo |
| `set_replicas(job_id, replicas)` | Cantidad absoluta, entero ≥ 0 | Cambia solo el objetivo y devuelve el valor aceptado |
| `stop_job(job_id)` | ID de trabajo | Equivale a `set_replicas(job_id, 0)`. El trabajo sigue registrado y puede reactivarse |

Las operaciones se procesan en secuencia. Cambiar el objetivo no arranca ni detiene nada por sí mismo: las instancias se ajustan en las siguientes rondas de reconciliación.

### Especificación

```json
{
  "job_id": "demo-001",
  "workload": {"entrypoint": "ticker", "args": {"interval_ms": 250}},
  "replicas": 2,
  "resources": {"cpu": 1},
  "placement": {"node_label": "zone-a"}
}
```

- **`job_id`:** de 1 a 64 caracteres entre `A-Z`, `a-z`, `0-9`, `.`, `_` y `-`.
- **`workload`:** solo se admiten cargas incluidas en el sistema; hoy, `ticker` (un contador que avanza cada `interval_ms` milisegundos, entero positivo). Nunca se ejecutan comandos arbitrarios.
- **`replicas`:** entero ≥ 0.
- **`resources.cpu`:** número positivo y finito, por réplica. Es una reserva lógica para decidir la colocación, no un límite físico de consumo.
- **`placement`** (opcional): con `node_label`, cada instancia solo puede ejecutarse en un nodo que declare esa etiqueta. Si ninguno con esa etiqueta tiene capacidad, la instancia queda `PENDING`; la restricción no se relaja sola.
- Cualquier campo no previsto (por ejemplo `memory_mb`) se rechaza.

**Aceptar** significa que el objetivo quedó registrado, nunca que las instancias ya estén funcionando. Qué significa "registrado" depende de la versión:

- **Versión Ray:** registrado en la memoria del controlador; un reinicio lo pierde.
- **Versión nativa:** debe significar **guardado de forma duradera**. `submit_job`, `set_replicas` y `stop_job` solo responden con éxito después de que el cambio sobreviva a un reinicio del controlador (ver sección 5).

### Consulta

```json
{
  "job_id": "demo-001",
  "desired_replicas": 2,
  "observed": {"running": 1, "pending": 1, "unknown": 0, "stopping": 0},
  "instances": [
    {"instance_id": "demo-001/1", "state": "RUNNING", "pid": 1234, "node_id": "node-A", "progress": 7},
    {"instance_id": "demo-001/2", "state": "PENDING", "pid": null, "node_id": null, "progress": null}
  ],
  "observed_at": "2026-10-18T12:00:00.000Z"
}
```

- `instances` incluye **todos** los intentos, también los `FAILED` y `STOPPED`, para poder seguir la historia de reemplazos.
- `observed` cuenta solo los intentos activos: `PENDING`, `RUNNING`, `UNKNOWN` y `STOPPING`.
- `observed_at` indica cuándo se tomó la vista; no es una instantánea atómica de todos los procesos, y `progress` es el último valor conocido, no el actual.

## 3. Estados y transiciones

| Estado | Significado |
|---|---|
| `PENDING` | Solicitada, sin respuesta de salud todavía: puede estar esperando recursos o arrancando |
| `RUNNING` | Ha respondido a la consulta de salud más reciente |
| `UNKNOWN` | Estaba en marcha y su última consulta no obtuvo respuesta dentro del plazo |
| `STOPPING` | Se pidió detenerla y aún no se ha confirmado su terminación |
| `STOPPED` | Terminación intencional confirmada (final) |
| `FAILED` | Muerte confirmada, o abandonada por el controlador tras K consultas sin respuesta (final) |

| Desde | Evento | Hacia |
|---|---|---|
| — | La reconciliación crea el intento | `PENDING` |
| `PENDING` | Primera respuesta de salud | `RUNNING` |
| `PENDING` | Consulta sin respuesta | `PENDING` (esperar recursos no es un fallo) |
| `RUNNING` | Consulta sin respuesta | `UNKNOWN` |
| `UNKNOWN` | Vuelve a responder | `RUNNING` |
| `UNKNOWN` | K consultas consecutivas sin respuesta | `FAILED`, y se pide terminar su proceso antes de reemplazarlo |
| `PENDING`, `RUNNING` o `UNKNOWN` | Se confirma la muerte del proceso | `FAILED` |
| `PENDING`, `RUNNING` o `UNKNOWN` | La reconciliación decide detenerlo | `STOPPING` |
| `STOPPING` | Se confirma la terminación o la muerte | `STOPPED`, nunca `FAILED` |

Reglas que se derivan de la tabla:

- `STOPPING` se registra **antes** de pedir la terminación, así que una parada intencional nunca dispara un reemplazo.
- Un silencio aislado no prueba una muerte. Los intentos `PENDING` y `UNKNOWN` cuentan para el objetivo, de modo que no se crean duplicados mientras hay dudas.
- Al reducir réplicas se detienen primero los `PENDING`, luego los `UNKNOWN` y al final los `RUNNING`; dentro de un mismo estado, los más nuevos.
- Una respuesta tardía de un intento ya `FAILED` o `STOPPED` no lo reactiva.
- K y el plazo de consulta son parámetros de configuración; en el prototipo, K = 3 y 2 s.

## 4. Errores y repeticiones

| Situación | Resultado |
|---|---|
| Especificación inválida, carga desconocida o campo no previsto | `INVALID_SPEC`; no se registra nada |
| Argumento inválido en una operación sin especificación (`set_replicas(id, -1)`) | `INVALID_ARGUMENT`; el objetivo no cambia |
| ID de trabajo inexistente | `JOB_NOT_FOUND` |
| `submit_job` con un ID existente y **la misma** especificación (comparada con la original, aunque después haya cambiado el objetivo) | Devuelve el mismo `job_id`, sin crear nada ni cambiar el objetivo actual |
| `submit_job` con un ID existente y una especificación **distinta** | `JOB_ALREADY_EXISTS` |
| Repetir `set_replicas(id, n)` o `stop_job(id)` | Sin efecto adicional: el objetivo queda en `n` (o 0) |
| Falta de capacidad | El objetivo se mantiene y los intentos quedan `PENDING`; nunca se informa que están funcionando |

Los errores se exponen con estos nombres, sin filtrar excepciones internas del middleware. Un cliente que reintenta con **otro** ID crea otro trabajo: no hay deduplicación por contenido.

Los casos de ejemplo ejecutables están en [`tests/test_contract.py`](../tests/test_contract.py).

## 5. Garantías

### 5.1 Garantías mínimas: las cumplen las dos versiones

- Con objetivo estable, procesos sanos y capacidad suficiente en nodos que cumplan la restricción de ubicación, la reconciliación acerca lo observado al objetivo.
- Un fallo nunca cambia el objetivo.
- Cada reemplazo es un intento nuevo que identifica al intento fallido al que sustituye.
- Mientras un intento esté pendiente o en duda, no se crea otro para cubrir la misma réplica.
- Una parada intencional nunca se trata como fallo ni provoca un reemplazo.

### 5.2 Límites de la versión Ray que la versión nativa debe superar

| Límite en la versión Ray | Qué debe garantizar la versión nativa |
|---|---|
| **El estado vive en memoria:** reiniciar el controlador pierde trabajos, objetivos e IDs | **Durabilidad:** todo cambio aceptado sobrevive a un reinicio del controlador, y tras reiniciar este reconcilia con las instancias que siguieron funcionando |
| **Un proceso por réplica solo porque Ray puede matar cualquier proceso**, incluso uno colgado | **Un proceso por réplica aunque un worker quede inaccesible:** una instancia abandonada no debe seguir ejecutándose junto a su reemplazo, aunque no se la pueda matar |
| **La repetición segura de `submit_job` solo dura una sesión** | **Envíos repetibles también tras un reinicio:** reenviar la misma especificación con el mismo ID devuelve el mismo trabajo |

### 5.3 Límites que la versión nativa puede mantener, justificándolo

- Conservar el estado interno de una instancia tras su reemplazo: el reemplazo empieza de cero.
- Un plazo universal de recuperación: depende del ciclo de control, del plazo de consulta y de K, que deben documentarse.
- Ejecución exactamente una vez durante fallos.
- Disponibilidad en ambos lados de una partición de red: la versión nativa debe decidir qué lado, si alguno, puede seguir tomando decisiones.
- Deduplicación por contenido: un cliente que reintenta con otro ID crea otro trabajo.

## 6. Qué no forma parte del contrato

Pueden cambiar en la versión nativa sin romperlo:

- El transporte (funciones Python, HTTP, gRPC) y el lenguaje de implementación.
- El formato del historial de eventos, que es evidencia interna para los experimentos.
- Cómo se ejecuta cada instancia (actor de Ray, proceso gestionado por un agente…), cómo se detectan los fallos y dónde se guarda el estado, siempre que se cumplan las garantías de la sección 5.

Cada garantía que la versión nativa añada o refuerce, empezando por las de la sección 5.2, se documenta al comparar versiones como un cambio respecto a esta, con la evidencia que la demuestra.
