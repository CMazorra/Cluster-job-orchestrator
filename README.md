# Cluster Job Orchestrator

Orquestador de trabajos en clúster. El cliente declara cuántas instancias de una carga quiere, con qué recursos y dónde pueden ejecutarse; el orquestador compara continuamente ese objetivo con lo que observa y crea, detiene o reemplaza instancias para acercarlos, también cuando los procesos fallan o dejan de responder.

**Estado:** prototipo con Ray terminado ([cierre de la fase](Ray-Phase1/docs/PHASE1_SUMMARY.md)). La siguiente fase reimplementa el mismo comportamiento sin Ray.

## Estructura

```
Ray-Phase1/
  orchestrator/       dominio y controlador (no dependen de Ray)
  ray_runtime.py      capa Ray: el actor Ticker y la gestión de sus procesos
  experiments/        experimentos que se ejecutan a mano con Ray real
  tests/              pruebas unitarias, sin Ray
  docs/               arquitectura, contrato, mapa de dependencias y registros de experimentos
  requirements.txt
AI_USE.md             declaración de uso de asistentes de IA
```

## Documentación

| Documento | Contenido |
|---|---|
| [Arquitectura](Ray-Phase1/docs/ARCHITECTURE.md) | Componentes, componentes de Ray de los que dependen, dueño de cada estado y ciclo de control |
| [Contrato v1.0](Ray-Phase1/docs/CONTRACT.md) | Operaciones, especificación, estados, errores y garantías (congelado) |
| [Mapa de dependencias](Ray-Phase1/docs/DEPENDENCY_MAP.md) | Qué hace Ray por nosotros y qué tendrá que decidir la versión nativa |
| [Cierre de la fase Ray](Ray-Phase1/docs/PHASE1_SUMMARY.md) | Entregables, semántica de fallos observada y preguntas abiertas |
| [F0](Ray-Phase1/docs/F0.md), [F1](Ray-Phase1/docs/F1.md), [F3](Ray-Phase1/docs/F3.md) | Referencia normal, caída de un proceso y proceso que deja de responder, con su [evidencia](Ray-Phase1/docs/evidence/) |

## Requisitos

- **Pruebas unitarias:** Python 3.10 o posterior, en cualquier sistema. No necesitan Ray.
- **Experimentos:** Linux o WSL2 con Python 3.12 y **Ray 2.58.0** (`Ray-Phase1/requirements.txt`). Ray en Windows nativo no está soportado de forma estable.

## Pruebas unitarias

```sh
cd Ray-Phase1
python -m unittest discover -s tests -v
```

## Experimentos con Ray

Instalación, una sola vez, dentro de WSL2 o Linux:

```sh
python3 -m venv ~/.venvs/cluster-job-orchestrator
~/.venvs/cluster-job-orchestrator/bin/python -m pip install -r Ray-Phase1/requirements.txt
```

Ejecución, **siempre desde `Ray-Phase1/`** (los procesos de Ray importan `ray_runtime` desde ahí):

```sh
source ~/.venvs/cluster-job-orchestrator/bin/activate
cd Ray-Phase1
python experiments/two_instances.py               # dos instancias en procesos distintos
python experiments/scale_sequence.py              # objetivos 1 -> 3 -> 1 -> 0
python experiments/placement_check.py             # restricción de ubicación
python experiments/fault_recovery.py --fault crash   # un proceso muere
python experiments/fault_recovery.py --fault hang    # un proceso deja de responder
```

Cada script imprime sus comprobaciones (`[OK]` o `[FAIL]`) y termina con código 1 si alguna falla. Con `--output` guarda los resultados en JSON y, donde existe, `--events` guarda el historial de eventos en JSONL. Entre ejecuciones conviene hacer `ray stop --force`.
