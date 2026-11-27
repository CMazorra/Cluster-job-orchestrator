# Escenarios de fallo y carga: fase Ray

Recorrido completo del catálogo de escenarios del curso (F0–F21) frente a lo que tiene el prototipo con Ray: qué se ejecutó, qué falta y qué no aplica, y por qué. Un escenario solo se marca como hecho si se ejecutó con Ray real, con la predicción escrita antes, tres ejecuciones y evidencia guardada.

## Resumen

| Estado | Escenarios |
|---|---|
| ✅ Ejecutados | F0, F1, caída de un nodo, F2, F3, F4, F7, F8, F9, F16, F17 |
| ➖ No aplican en esta fase | F5, F6, F10–F15, F18–F21 |

## Ejecutados

| Escenario | Registro | Resultado | Hallazgos principales |
|---|---|---|---|
| **F0** Ejecución normal de referencia | [F0.md](F0.md) | 8/8 y 8/8 en tres ejecuciones | Toda fase converge en dos ciclos de control (~0,5 s) |
| **F1** Caída de un proceso | [F1.md](F1.md) | 10/10 en tres ejecuciones | Se pierde el estado local; sobreviven el objetivo y las demás instancias; un reemplazo en ~0,5 s |
| **Caída de un nodo** y entrada de otro (campaña mínima del curso; F1 a nivel de nodo) | [F1_NODE.md](F1_NODE.md) | 10/10 en tres ejecuciones | Detección ~15 s; rondas de consulta de ~10 s pese al plazo de 2 s; con la membresía de Ray desactualizada, los reemplazos se asignan al nodo muerto y la recuperación tarda ~85 s |
| **F2** Caída y reinicio del controlador | [F2.md](F2.md) | 5/5 en tres ejecuciones | Ray destruye las instancias ~17 ms después de caer el controlador; el controlador reiniciado responde `JOB_NOT_FOUND`; solo reenviar el trabajo lo restaura (~1,9 s) y los IDs de intento se reutilizan. Es la limitación de durabilidad del [contrato](CONTRACT.md) (5.2), ahora con evidencia |
| **F3** Proceso lento o colgado | [F3.md](F3.md) | 12/12 en tres ejecuciones | Sospecha a los 2 s y abandono a los 7 s, tras exactamente K = 3 consultas; nunca dos procesos para una réplica |
| **F4** Pérdida de respuestas de salud | [F4.md](F4.md) | 6/6 en seis ejecuciones (p = 0,1 y p = 0,3) | El 94 % de las sospechas se aclara sin reemplazo; nunca más intentos que réplicas. Aun así, con K = 3 y un 30 % de pérdida hay un abandono falso cada ~25 s por réplica (11 observados frente a ~14 esperados), que mata un proceso sano |
| **F7** Reparación tras una pausa | [F7.md](F7.md) | 7/7 en tres ejecuciones | Frontera exacta en K·plazo + (K − 1)·ciclo = 7 s: pausas de 4 y 6 s solo producen sospecha y el proceso conserva su contador; una de 12 s se abandona a los 7,0 s, con un reemplazo. Un reloj desincronizado alargaba las rondas de consulta hasta 10 s |
| **F8** Respuesta perdida tras un efecto | [F8.md](F8.md) | 7/7 en tres ejecuciones | Repetir la misma petición es seguro: un trabajo y ningún actor duplicado. Un cliente que recalcula su petición tras perder la respuesta aplica el cambio dos veces (pidió 4, obtuvo 6) |
| **F9** Petición duplicada | [F9.md](F9.md) | 7/7 en tres ejecuciones | Los duplicados inmediatos no tienen efecto; una copia tardía de `set_replicas` deshace un cambio posterior y arranca 3 procesos (no hay ID de petición ni versión) |
| **F16** Sobrecarga | [F16.md](F16.md) | 7/7 en tres ejecuciones | 36 pendientes sin crecimiento y nunca tomados por procesos sin respuesta. Pero cada ronda espera el plazo completo por ellos: el ciclo pasa de 0,5 s a ~2,5 s y se retrasa la detección de fallos |
| **F17** Amplificación de reintentos | [F17.md](F17.md) | 7/7 en tres ejecuciones (y 6/6 sin la corrección, como comparación) | Sin control, una carga que se cae al arrancar provocaba 53 reemplazos en 30 s; con la espera creciente añadida al dominio, 5. A cambio, la recuperación tras arreglar el fallo tarda lo que quede de la espera en curso |

## No aplican en esta fase

| Escenario | Por qué no aplica | ¿Cuándo aplicará? |
|---|---|---|
| **F5/F6** Partición de red (minoritaria o simétrica) | Todo corre en una máquina y Ray hace el transporte: no hay una red propia que cortar. La parte de "corte temporal" que sí aplica la cubre [F7](F7.md) | En la versión nativa, con nodos en contenedores y redes separadas |
| **F10** Descubrimiento obsoleto | No hay un mecanismo de descubrimiento propio: lo resuelve Ray. Se observó de forma indirecta en la [caída de un nodo](F1_NODE.md), donde la información de membresía desactualizada de Ray retrasó la recuperación | En la versión nativa, con registro y arrendamientos propios |
| **F11** Lectura obsoleta de una réplica | Hay una sola copia del estado | En la versión nativa, al replicar el controlador |
| **F12** Escrituras concurrentes en conflicto | Las operaciones se procesan en secuencia por contrato | En la versión nativa, con clientes concurrentes |
| **F13/F14** Fallo del coordinador o participante en 2PC | No hay transacciones distribuidas | Solo si la versión nativa las introduce |
| **F15** Fallo del líder | No hay líder: un único controlador fijo | En la versión nativa: la tolerancia a dos fallos obliga a replicar el controlador, y lo habitual es que haya un líder |
| **F18** *Churn* de membresía descentralizada | La membresía no es descentralizada. La entrada y salida de nodos se cubre en la [caída de un nodo](F1_NODE.md) | Solo si la versión nativa descentraliza la membresía |
| **F19** Actualizaciones CRDT desconectadas | No hay réplicas que se fusionen | No previsto |
| **F20** Operación no autorizada | Los clientes son de confianza en esta fase | En la versión nativa, con su modelo de seguridad |
| **F21** Repetición de una mutación autenticada | No hay autenticación en esta fase | En la versión nativa, con su modelo de seguridad |

## Cómo se ejecutan

Los experimentos se ejecutan desde `Ray-Phase1/` con Ray real; las instrucciones están en el [README](../../README.md). Cada uno imprime sus comprobaciones y puede guardar su evidencia en [`evidence/`](evidence/).
