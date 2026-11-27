```yaml
ai_use:
  used: true
  tools:
    - Codex
    - Claude Code
    - opencode
  activities:
    - architecture discussion
    - project planning
    - implementation
    - test generation
    - code review
    - debugging
    - environment setup guidance
    - documentation
  verification:
    - unit tests with python -m unittest (64 cases, no Ray needed)
    - experiments against real Ray 2.58.0 in WSL2, three runs per fault scenario
    - fault scenarios checked against predictions written before running (F1, F2, F3, F4, F7, F8, F9, F16, F17, node failure)
    - Ray's state API used as an independent source for process identity and liveness
    - manual review against the course documents
    - ActorUnavailableError handled as uncertainty: Ray raised it for actors on a lost node
    - WSL clock checked before and after each run for F7, F8, F9, F16 and the repeated F3 and node failure runs
  not_verified:
    - more than one physical machine and real network faults (message loss was simulated on the controller side)
```
