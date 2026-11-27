# Experimento: caída de un nodo y entrada de un nodo nuevo

Caída de un nodo entero, el escenario "one service-node crash" de la campaña mínima del curso, seguida de la entrada de un nodo nuevo. Es la extensión de [F1](F1.md) del proceso al nodo: perder un nodo tira todos los procesos que aloja. Cubre dos frases del enunciado del P14: *"reschedules work when nodes fail"* y *"machines join and disappear"*.

La predicción se escribió **antes** de las ejecuciones registradas, pero **después** de un prototipo exploratorio, cuyas observaciones se indican.

> **Aviso:** las primeras tres ejecuciones (25 de noviembre) se hicieron, muy probablemente, con el reloj de WSL saltando ±35 s. Al repetirlas con el reloj estable (26 de noviembre), **desaparecieron las rondas de ~10 s y el retraso de ~85 s en la recuperación**. Las conclusiones vigentes están en la sección [Repetición con el reloj estable](#repetición-con-el-reloj-estable).

## Configuración

| Campo | Valor |
|---|---|
| Script | [`experiments/node_failure.py`](../experiments/node_failure.py) |
| Topología | Clúster simulado en una sola máquina (WSL2) con las utilidades de Ray: un nodo cabeza **sin CPU** y dos nodos worker de 2 CPU. Cada nodo tiene su propio raylet y sus propios procesos, pero no hay red real entre ellos |
| Trabajo | `node-demo`, carga `ticker` cada 100 ms, **4 réplicas** de 1 CPU: llenan los dos nodos |
| Controlador | ciclo cada 0,5 s, plazo de consulta 2 s, K = 3 |
| Disparador | `remove_node(..., allow_graceful=False)` del nodo que aloja instancias: se matan su raylet y sus procesos sin aviso. Después se añade un nodo nuevo de 2 CPU |
| Afirmación bajo prueba | Al perder un nodo, sus instancias se dan por perdidas y se reemplazan una a una sin tocar las del otro nodo; los reemplazos esperan sin multiplicarse mientras no hay capacidad, y se colocan en cuanto entra un nodo nuevo |

## Lo observado en el prototipo exploratorio

Antes de escribir el experimento se probó la caída de un nodo con un script mínimo. Mostró tres cosas que condicionan la predicción:

- **Un proceso de un nodo "muerto" siguió respondiendo ~50 s**, mientras otro del mismo nodo dejó de responder enseguida.
- **Algunas rondas de consulta duraron ~10 s** pese a un plazo de 2 s: mientras Ray procesa la caída, la espera se bloquea.
- **Apareció `ActorUnavailableError`**, y Ray tardó **~81 s** en confirmar las muertes.

## Predicción (antes de las ejecuciones registradas)

1. **Reparto inicial:** 2 instancias en cada nodo worker.
2. **Detección:** las instancias del nodo perdido pasan a `UNKNOWN` en pocos segundos. Acaban en **`FAILED`** de una de dos formas: porque Ray confirma la muerte, o por **decisión** del controlador tras K consultas perdidas, lo que ocurra antes. Por las rondas lentas, la detección tardará **decenas de segundos**, no los ~7 s de [F3](F3.md).
3. **Fallos temporales:** puede aparecer `ActorUnavailableError` y se tratará como incertidumbre, sin provocar un reemplazo inmediato.
4. **Un reemplazo por instancia perdida**, que la nombra (`replaces`).
5. **Sin capacidad, sin duplicados:** los 2 reemplazos quedan en `PENDING`, y el número total de intentos se mantiene en 6 ciclo tras ciclo.
6. **El otro nodo no se entera:** sus 2 instancias siguen `RUNNING`, con el mismo PID y el contador avanzando.
7. **Entrada de un nodo:** los 2 reemplazos pasan a `RUNNING` **en el nodo nuevo**, en uno o dos ciclos.
8. **El objetivo de 4 no cambia** en ningún momento, y la parada final es limpia.

**Incertidumbre:** si en alguna ejecución un proceso del nodo perdido sigue respondiendo durante mucho tiempo, como en el prototipo; cuánto tardan las rondas; y si el `FAILED` llega por observación o por decisión.

## Resultado observado

Tres ejecuciones el 25 de noviembre de 2026, Ray 2.58.0, Python 3.12.3, WSL2. **10 de 10 comprobaciones en las tres.** Evidencia: [`evidence/`](evidence/), `node_run1..3.json` y `.jsonl`.

| Medida | Ej. 1 | Ej. 2 | Ej. 3 |
|---|---|---|---|
| Sospecha de los intentos perdidos (`UNKNOWN`) | 9,7 s | 9,5 s | 2,1 s |
| `FAILED` | 14,8 s | 18,4 s | 14,9 s |
| Cómo se llegó a `FAILED` | Ray confirmó la muerte | Decisión del controlador (K consultas) | Decisión del controlador (K consultas) |
| Ronda de consulta más lenta (plazo: 2 s) | 10,6 s | 8,5 s | 9,8 s |
| Respuestas `ActorUnavailableError` | 0 | 0 | 2 |
| Reemplazos que fallaron y hubo que reemplazar | 0 | 2 | 2 |
| De la entrada del nodo nuevo a la recuperación | **1,4 s** | **84,3 s** | **88,7 s** |
| Procesos perdidos vivos cuando corrían sus reemplazos | ninguno | ninguno | ninguno |

Desarrollo de la ejecución 3, según el historial de eventos (`R` en marcha, `U` en duda, `P` pendiente, `F` fallido):

```
reparto inicial: nodo X: /1 /2   ·   nodo Y: /3 /4
  0,0 s   se elimina el nodo Y
  2,1 s   /3 y /4 → U  (sin respuesta; Ray llega a contestar ActorUnavailableError)
 14,9 s   /3 y /4 → F por decisión (K = 3) · se piden los reemplazos /5 y /6
          Ray aún cree vivo el nodo Y y les asigna su capacidad: nunca arrancan
          /5 y /6 quedan P: no responden, igual que si esperaran capacidad
 24,9 s   entra el nodo nuevo Z, pero /5 y /6 siguen asignados al nodo Y
113,1 s   Ray declara muerto el nodo Y: /5 y /6 → F · se piden /7 y /8
113,6 s   /7 y /8 → R en el nodo Z
```

En la ejecución 1, Ray declaró muerto el nodo antes de que el controlador abandonara los intentos: los reemplazos quedaron pendientes "de verdad" y arrancaron en el nodo nuevo 1,4 s después de su entrada.

## Comparación con la predicción

| # | Predicción | Observado | ¿Coincide? |
|---|---|---|---|
| 1 | 2 instancias en cada nodo | 2 y 2 en las tres ejecuciones | Sí |
| 2 | `FAILED` por observación o por decisión; decenas de segundos | 15–18 s, una vez por observación y dos por decisión | Sí |
| 3 | `ActorUnavailableError` tratado como incertidumbre | Apareció en la ejecución 3; el intento siguió en `UNKNOWN`, sin reemplazo inmediato | Sí |
| 4 | Un reemplazo por instancia perdida | Sí cuando Ray confirmó la muerte; **cuando decidió el controlador, el primer reemplazo quedó atascado en el nodo muerto, falló y hubo un segundo** | **En parte** |
| 5 | Pendientes sin multiplicarse | 2 en `PENDING` y el mismo número de intentos (6) en todos los ciclos de espera | Sí |
| 6 | El otro nodo no se entera | Sin cambios de estado, mismo PID, contador avanzando | Sí |
| 7 | Reemplazos en el nodo nuevo en uno o dos ciclos | En las tres ejecuciones acabaron en el nodo nuevo, pero solo en la 1 fue inmediato (1,4 s); en la 2 y la 3 tardaron **~85 s** | **En parte** |
| 8 | Objetivo constante y parada limpia | Objetivo 4 en todo momento; sin procesos vivos al final | Sí |

## Conclusión

- **El orquestador cumple sus garantías ante la pérdida de un nodo:** el objetivo no cambia, el otro nodo no se ve afectado, nunca hay dos procesos para una réplica (comprobado en el sistema operativo), los reemplazos esperan sin multiplicarse y se colocan en cuanto hay capacidad.
- **Hallazgo principal: información de membresía desactualizada que retrasa la recuperación.** Cuando nuestro controlador decide antes que Ray, el planificador de Ray todavía cree vivo el nodo perdido y **asigna allí los reemplazos**. Nunca arrancan y siguen asignados a ese nodo aunque entre otro con capacidad libre; solo cuando Ray declara muerto el nodo (~110 s) fallan y se reemplazan. Resultado: la recuperación tardó **~85 s** tras la entrada del nodo nuevo, frente a 1,4 s cuando Ray detectó primero la caída. Es el problema del escenario F10 (información de membresía obsoleta), aquí causado por el middleware. El orquestador no incumple ninguna garantía, porque el contrato no promete un plazo de recuperación, pero el resultado es claramente peor.
- **Un `PENDING` no distingue "esperando capacidad" de "asignado a un nodo muerto".** Desde el controlador, ambos son intentos que no responden. Es una consecuencia directa de haber descartado el estado `STARTING` en esta fase.
- **El plazo de consulta no se respeta cuando cae un nodo:** las rondas llegaron a ~10 s con un plazo de 2 s, porque Ray se bloquea mientras procesa la caída. La detección depende más de Ray que de nuestros parámetros.
- **Ray tarda ~15 s en declarar muerto un nodo** (en el prototipo, más de un minuto).

## Qué implica para la versión nativa

- La detección de fallos de nodo debe ser **propia** (latidos y arrendamientos), no depender del middleware.
- **No colocar nada en un nodo sospechoso:** el planificador debe usar la misma información de membresía que la detección de fallos. Así no puede repetirse el retraso de ~85 s observado.
- **Distinguir "asignado a un nodo" de "esperando capacidad"**, por ejemplo con un estado de asignación, para poder reasignar un intento si su nodo deja de ser válido.
- Las consultas deben respetar su plazo aunque un nodo no responda.
- Que los procesos de un nodo perdido estén muertos solo se comprobó aquí porque todo corre en una máquina. Con nodos reales no se podrá comprobar, y por eso hace falta la autodetención por arrendamiento de la sección 5.2 del [contrato](CONTRACT.md).

## Alcance de la evidencia e incertidumbre

- Los nodos son simulados en una sola máquina: cada uno tiene su raylet y sus procesos, pero no hay red real entre ellos. La caída es la terminación de sus procesos, no un corte de red.
- El tiempo de detección de Ray varió mucho entre el prototipo (~80 s) y las ejecuciones registradas (~15 s). No se investigó qué lo determina.
- En el prototipo, un proceso de un nodo perdido siguió respondiendo ~50 s. En las ejecuciones registradas no se repitió, así que no se pudo observar cómo lo habría tratado el controlador.

## Repetición con el reloj estable

El 26 de noviembre se comprobó que el reloj de WSL alternaba ±35 s cuando el de Windows estaba desincronizado, y que esos saltos alargaban las rondas de `ray.wait` (ver [F7](F7.md)). Las ejecuciones del 25 de noviembre pudieron verse afectadas. Se repite el experimento con el reloj comprobado antes y después de cada ejecución. Evidencia: `node_stable_run1..3.json` y `.jsonl`.

**Predicción (antes de ejecutar):**

1. **Las rondas seguirán superando el plazo**, aunque menos: aquí, a diferencia de F3 y F7, hay una causa propia, porque Ray intenta contactar con un nodo que ya no existe. Se espera alguna ronda claramente por encima de 2 s.
2. **El retraso de la recuperación cuando decide el controlador (~85 s) se mantendrá**: depende de cuándo Ray declara muerto el nodo, no del reloj.
3. Las 10 comprobaciones se cumplirán.

**Incertidumbre:** si todas las rondas se quedan en ~2 s, el punto 1 es falso y las rondas lentas de esta página también venían del reloj.

**Resultado:** tres ejecuciones el 26 de noviembre de 2026, Ray 2.58.0, Python 3.12.3, WSL2, con el reloj comprobado antes y después de cada una (a lo sumo un salto de ~1 s en 10 s). **10/10 en las ejecuciones 1 y 3 y 9/10 en la 2** (explicado abajo).

| Medida | 25 nov. (Ej. 1 / 2 / 3) | 26 nov., reloj estable (Ej. 1 / 2 / 3) |
|---|---|---|
| Ronda de consulta más lenta (plazo: 2 s) | 10,6 / 8,5 / 9,8 s | **2,002 / 2,001 / 2,002 s** |
| Sospecha (`UNKNOWN`) | 9,7 / 9,5 / 2,1 s | 2,3 / 2,3 / 2,4 s |
| `FAILED` | 14,8 / 18,4 / 14,9 s | **6,3 / 6,3 / 6,4 s**, siempre por decisión del controlador |
| Respuestas `ActorUnavailableError` | 0 / 0 / 2 | 6 / 6 / 6 |
| Reemplazos que fallaron y hubo que reemplazar | 0 / 2 / 2 | 0 / 2 / 0 |
| De la entrada del nodo nuevo a la recuperación | 1,4 / **84,3 / 88,7 s** | **1,5 / 1,7 / 1,4 s** |
| Procesos perdidos vivos cuando corrían sus reemplazos | ninguno | ninguno |

**La comprobación que falló en la ejecución 2** fue "sin capacidad, los reemplazos esperan en `PENDING` sin multiplicarse": `PENDING` pasó de 2 a 0 en el último ciclo observado. Los reemplazos se habían asignado al nodo perdido, y esta vez Ray declaró muerto ese nodo **a los 14,8 s**, mientras se observaba la espera. Los reemplazos fallaron entonces, antes de que entrara el nodo nuevo. No se multiplicó nada (6 intentos en todos los ciclos) y en cuanto entró el nodo nuevo se reemplazaron y funcionaron en 1,7 s. Es la comprobación la que supone que la espera dura todo el periodo observado; no se ha cambiado a posteriori, para no ajustar el criterio al resultado.

**Comparación con la predicción de la repetición:**

| # | Predicción | Observado | ¿Coincide? |
|---|---|---|---|
| 1 | Las rondas seguirán superando el plazo | Ninguna pasó de 2,002 s | **No** |
| 2 | Se mantendrá el retraso de ~85 s cuando decide el controlador | El controlador decidió primero en las tres, y la recuperación tardó ~1,5 s | **No** |
| 3 | 10 comprobaciones en todas | 10, 9 y 10 | En parte |

**Qué cambia en las conclusiones de esta página:**

- **Las rondas largas y el retraso de ~85 s no son propios de Ray**: con el reloj estable no aparecen. La explicación más probable es que el detector de fallos de Ray (latidos con plazos) y `ray.wait` se desajustan cuando el reloj salta. No se puede demostrar que el reloj saltara el 25 de noviembre, porque entonces no se comprobaba, pero es el único cambio entre las dos tandas.
- **Sigue en pie la membresía desactualizada, aunque mucho más breve:** en la ejecución 2 los reemplazos se asignaron al nodo muerto, porque Ray todavía no lo había declarado muerto, y fallaron a los ~8 s de pedirse. Con el reloj estable, Ray declara muerto el nodo en ~15 s y el coste es un reemplazo de más, no 85 s.
- **Sigue en pie todo lo demás:** el objetivo no cambia, el otro nodo no se entera, nunca hay dos procesos para una réplica y los reemplazos esperan sin multiplicarse.
- **Ray contesta `ActorUnavailableError` a los actores del nodo perdido** (6 veces en cada ejecución), y el controlador lo trata como incertidumbre, sin reemplazar en el acto. Como esa respuesta llega antes que el plazo, el abandono llega algo antes (6,3 s) que en [F3](F3.md) (7,0 s).
- **Lo que implica para la versión nativa no cambia:** la detección de fallos de nodo debe ser propia y el planificador debe usar la misma información de membresía que la detección. Además, **sus plazos deben medirse con relojes monotónicos**, porque un reloj que salta engaña a cualquier detector basado en tiempos.
