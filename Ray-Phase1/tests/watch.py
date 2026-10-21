"""Observar llamadas remotas y procesos con Ray.

Ejercicio del día 1 (Persona B) del plan del proyecto P14: preparar Ray y
observar llamadas remotas y procesos.

Lo que este script deja demostrado:

* **Solicitud** (``.remote()``) y **resultado** (``ray.get()``) son pasos
  distintos. Pedir dos tareas antes de esperar no cuesta lo mismo que pedir y
  esperar una por una.
* Una llamada remota ocurre **en otro proceso**: el PID del actor y el PID del
  driver son distintos.
* El **estado local** del actor vive en su proceso: persiste entre llamadas y
  el driver no lo ve sin pasar por un método.
* Pedir más trabajo del que cabe en CPU no es un fallo: la instancia **espera**
  recursos.

No es un test del dominio: se ejecuta a mano y su salida es evidencia del
experimento, no un aserto.

Uso (dentro de WSL2, con el venv de Ray):

    python tests/watch.py --parte all
    python tests/watch.py --parte a --salida /tmp/dia1.json

Las opciones ``--cpu`` y ``--duracion`` existen para poder repetir la medición
sin tocar el código; los valores por defecto son los del ejercicio del día 1.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any

import ray

PREFIJO = "[dia1]"

CPU_LOGICAS = 2
DURACION_S = 2.0
PARTES = ("a", "b", "c")


# --- Utilidades ---------------------------------------------------------------


def _node_id() -> str:
    """Nodo donde se ejecuta la llamada actual.

    Se intenta el método y se cae al atributo porque ambas formas han
    existido en Ray y no queremos que un cambio de versión rompa el ejercicio.
    """
    ctx = ray.get_runtime_context()
    getter = getattr(ctx, "get_node_id", None)
    if callable(getter):
        return str(getter())
    return str(ctx.node_id)


def _tabla_md(encabezados: list[str], filas: list[list[str]]) -> str:
    lineas = ["| " + " | ".join(encabezados) + " |"]
    lineas.append("|" + "|".join("---" for _ in encabezados) + "|")
    for fila in filas:
        lineas.append("| " + " | ".join(fila) + " |")
    return "\n".join(lineas)


def _pids(resultados: list[dict[str, Any]]) -> str:
    return ", ".join(str(r["pid"]) for r in resultados)


def _listar_actores() -> list[dict[str, Any]]:
    """Actores vivos segun la API de estado de Ray.

    Es la fuente independiente: el actor dice su propio PID desde dentro, y la
    API de estado dice que PID es y en que nodo esta. Si concuerdan, la
    observacion es solida.

    Se intenta la API nueva (``ray.util.state``) y luego la antigua. Si
    ninguna esta disponible no es un fallo del ejercicio: se registra y se
    sigue, porque el resto de mediciones no dependen de ella.
    """
    try:
        from ray.util.state import list_actors  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        try:
            crudos: list[Any] = list(ray.list_actors().values())
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"API de estado no disponible: {exc!r}"}]
    else:
        try:
            crudos = list(list_actors())
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"API de estado no disponible: {exc!r}"}]

    salida: list[dict[str, Any]] = []
    for actor in crudos:
        estado = getattr(actor, "state", None)
        salida.append(
            {
                "actor_id": str(getattr(actor, "actor_id", "")),
                "class_name": str(getattr(actor, "class_name", "")),
                "node_id": str(getattr(actor, "node_id", "")),
                "pid": getattr(actor, "pid", None),
                "state": getattr(estado, "value", str(estado)),
            }
        )
    return salida


# --- Carga remota ------------------------------------------------------------


@ray.remote(num_cpus=1)
def dormir(segundos: float) -> dict[str, Any]:
    """Tarea independiente: duerme y dice quién ha ejecutado la llamada.

    Devolver el PID es la prueba de que la llamada corrió en otro proceso.
    """
    inicio = time.time()
    time.sleep(segundos)
    return {
        "pid": os.getpid(),
        "node_id": _node_id(),
        "inicio": inicio,
        "fin": time.time(),
    }


@ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
class Contador:
    """Actor mínimo con estado local.

    ``max_restarts=0`` y ``max_task_retries=0`` son los valores por defecto de
    Ray, pero se escriben a propósito: Ray no repone el actor y la decisión de
    reemplazo es del controlador. Si Ray lo reiniciara, se escondería la
    pérdida del estado local.
    """

    def __init__(self) -> None:
        self.valor = 0

    def incrementar(self) -> int:
        self.valor += 1
        return self.valor

    def status(self) -> dict[str, Any]:
        return {
            "valor": self.valor,
            "pid": os.getpid(),
            "node_id": _node_id(),
        }


# --- Partes ------------------------------------------------------------------


def parte_a(duracion: float) -> dict[str, Any]:
    """Solicitar y esperar: una por una, o las dos antes de esperar."""
    print(f"{PREFIJO} --- Parte A: solicitar y esperar ---")

    t0 = time.perf_counter()
    primero = ray.get(dormir.remote(duracion))
    segundo = ray.get(dormir.remote(duracion))
    a1_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    refs = [dormir.remote(duracion) for _ in range(2)]
    a2 = ray.get(refs)
    a2_s = time.perf_counter() - t0

    print(f"{PREFIJO} A1  pedir y esperar una por una : 2 ray.get, {a1_s:.3f} s")
    print(f"{PREFIJO}     pids: [{_pids([primero, segundo])}]")
    print(f"{PREFIJO} A2  pedir ambas antes de esperar : 1 ray.get,  {a2_s:.3f} s")
    print(f"{PREFIJO}     pids: [{_pids(a2)}]")

    tabla = _tabla_md(
        ["modo", "num. de ray.get", "tiempo total (s)", "pids"],
        [
            ["A1 pedir y esperar una por una", "2", f"{a1_s:.3f}", _pids([primero, segundo])],
            ["A2 pedir ambas antes de esperar", "1", f"{a2_s:.3f}", _pids(a2)],
        ],
    )
    print(f"{PREFIJO} {tabla}")

    return {
        "a1": {"segundos": a1_s, "ray_get": 2, "resultados": [primero, segundo]},
        "a2": {"segundos": a2_s, "ray_get": 1, "resultados": a2},
    }


def parte_b() -> dict[str, Any]:
    """Actor: llamada remota, estado local y nodo donde lo colocó Ray."""
    print(f"{PREFIJO} --- Parte B: actor y estado local ---")

    pid_driver = os.getpid()
    node_driver = _node_id()

    actor = Contador.remote()
    try:
        inicial = ray.get(actor.status.remote())
        tras_uno = ray.get(actor.incrementar.remote())
        tras_dos = ray.get(actor.incrementar.remote())
        final = ray.get(actor.status.remote())
        vivos = _listar_actores()
    finally:
        ray.kill(actor)

    pid_actor = int(final["pid"])
    print(f"{PREFIJO} pid del driver : {pid_driver}")
    print(f"{PREFIJO} pid del actor  : {pid_actor}  (distinto: la llamada es remota)")
    print(f"{PREFIJO} node_id driver : {node_driver}")
    print(f"{PREFIJO} node_id actor  : {final['node_id']}")
    print(f"{PREFIJO} estado inicial : valor={inicial['valor']}")
    print(f"{PREFIJO} tras 2 incrementos: valor={final['valor']} (estado local del actor)")
    print(f"{PREFIJO} el driver no lee ese estado sin pasar por status()")
    print(f"{PREFIJO} actores segun la API de estado de Ray:")
    for vivo in vivos:
        if "error" in vivo:
            print(f"{PREFIJO}   {vivo['error']}")
            continue
        print(
            f"{PREFIJO}   {vivo['class_name']} actor_id={vivo['actor_id']} "
            f"pid={vivo['pid']} node_id={vivo['node_id']} state={vivo['state']}"
        )

    return {
        "pid_driver": pid_driver,
        "pid_actor": pid_actor,
        "node_driver": node_driver,
        "node_actor": final["node_id"],
        "valor_inicial": inicial["valor"],
        "valor_tras_dos_incrementos": final["valor"],
        "incrementos": [tras_uno, tras_dos],
        "actores_vivos": vivos,
    }


def parte_c(duracion: float) -> dict[str, Any]:
    """Pedir más del que cabe: la tercera tarea espera CPU, no falla."""
    print(f"{PREFIJO} --- Parte C: concurrencia frente a recursos ---")

    envio = time.time()
    t0 = time.perf_counter()
    refs = [dormir.remote(duracion) for _ in range(3)]
    resultados = ray.get(refs)
    total_s = time.perf_counter() - t0

    ordenados = sorted(resultados, key=lambda r: r["inicio"])
    print(f"{PREFIJO} 3 tareas de 1 CPU con {CPU_LOGICAS} CPU lógicas")
    print(f"{PREFIJO} tiempo total: {total_s:.3f} s")
    print(f"{PREFIJO} linea de tiempo (s desde el envio):")
    for i, r in enumerate(ordenados, start=1):
        print(
            f"{PREFIJO}   tarea {i}: "
            f"[{r['inicio'] - envio:.3f} --> {r['fin'] - envio:.3f}]  pid {r['pid']}"
        )

    solapan = 0
    for anterior, actual in zip(ordenados, ordenados[1:]):
        if actual["inicio"] < anterior["fin"]:
            solapan += 1
    print(f"{PREFIJO} pares de tareas que se solapan: {solapan}")
    print(f"{PREFIJO} la 3a tarea no fallo: espero CPU libre (PENDING, no FAILED)")

    return {
        "segundos": total_s,
        "tareas": ordenados,
        "pares_solapados": solapan,
    }


# --- Arranque ----------------------------------------------------------------


def _banner(args: argparse.Namespace) -> dict[str, Any]:
    if args.direccion:
        ray.init(address=args.direccion, include_dashboard=True, ignore_reinit_error=True)
        modo = f"cluster externo ({args.direccion})"
    else:
        ray.init(num_cpus=args.cpu, include_dashboard=True, ignore_reinit_error=True)
        modo = f"cluster local ({args.cpu} CPU logicas)"
    meta = {
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "modo": modo,
        "driver_pid": os.getpid(),
        "driver_node_id": _node_id(),
        "cpu_logicas": args.cpu,
        "duracion_s": args.duracion,
        "cluster_resources": {k: v for k, v in ray.cluster_resources().items()},
        "available_resources": {k: v for k, v in ray.available_resources().items()},
    }
    print(f"{PREFIJO} ==== observar llamadas remotas y procesos (dia 1) ====")
    print(f"{PREFIJO} ray {meta['ray_version']} | python {meta['python_version']}")
    print(f"{PREFIJO} {modo}")
    print(f"{PREFIJO} driver pid {meta['driver_pid']} | node {meta['driver_node_id']}")
    print(f"{PREFIJO} recursos del cluster : {meta['cluster_resources']}")
    print(f"{PREFIJO} recursos disponibles : {meta['available_resources']}")
    return meta


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parte",
        choices=(*PARTES, "all"),
        default="all",
        help="que parte del ejercicio ejecutar",
    )
    parser.add_argument(
        "--salida",
        type=Path,
        default=None,
        help="archivo JSON donde volcar los resultados crudos",
    )
    parser.add_argument("--cpu", type=int, default=CPU_LOGICAS, help="CPU lógicas")
    parser.add_argument(
        "--direccion",
        default=None,
        help="conectar a un cluster ya arrancado (p.ej. 'auto') en vez de levantar uno local",
    )
    parser.add_argument("--duracion", type=float, default=DURACION_S, help="segundos por tarea")
    args = parser.parse_args()

    resultados: dict[str, Any] = {}
    try:
        meta = _banner(args)
        if args.parte in ("a", "all"):
            resultados["parte_a"] = parte_a(args.duracion)
        if args.parte in ("b", "all"):
            resultados["parte_b"] = parte_b()
        if args.parte in ("c", "all"):
            resultados["parte_c"] = parte_c(args.duracion)
    finally:
        ray.shutdown()

    documento = {"meta": meta, **resultados}
    if args.salida is not None:
        args.salida.parent.mkdir(parents=True, exist_ok=True)
        args.salida.write_text(
            json.dumps(documento, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"{PREFIJO} resultados escritos en {args.salida}")

    print(f"{PREFIJO} fin")
    return 0


if __name__ == "__main__":
    sys.exit(main())
