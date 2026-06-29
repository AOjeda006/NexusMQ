# Diagrama 7: Secuencia de una operación de E/S sobre el proactor

El ciclo de una operación de E/S asíncrona modelada como *proactor* por *completions* (ADR-0002): una corrutina hace `co_await` sobre un awaitable (`ReadAwaitable`, `RecvAwaitable`, `WriteAwaitable`…), que **encola** la operación en el anillo io_uring del reactor (`Proactor::submit_*`) y suspende la corrutina sin bloquear el hilo. El bucle del reactor (`poll_once`) sigue atendiendo otras tareas; cuando no hay nada que hacer, **bloquea** en `wait_completions` cediendo la CPU. Al terminar la operación, el reactor drena la *completion* del anillo y ejecuta su callback, que guarda el resultado (`result_`) y reanuda la corrutina justo donde quedó. En `await_resume`, el resultado estilo io_uring (`>= 0` éxito, `< 0` = `-errno`) se traduce a `expected<T>` (modelo de errores del núcleo, ADR-0009).

```mermaid
sequenceDiagram
    autonumber
    participant Coro as Corrutina (task)
    participant Aw as IoAwaitable (p. ej. RecvAwaitable)
    participant Pr as Proactor (anillo io_uring)
    participant Loop as Reactor::poll_once
    participant OS as Kernel (io_uring)

    Coro->>Aw: co_await async_recv(...)
    Aw->>Aw: await_ready() == false
    Note over Aw: await_suspend(handle):<br/>registra una Completion que guardará el<br/>resultado y reanudará la corrutina
    Aw->>Pr: submit_recv(fd, buffer, on_done)
    Pr->>OS: encola SQE (sin bloquear)
    Aw-->>Coro: suspende (cede el control al reactor)

    loop bucle del reactor
        Loop->>Loop: run_ready() + mailbox.drain()
        Loop->>Pr: wait_completions(max, deadline)
        Note over Loop,Pr: si no hay trabajo, bloquea cediendo la CPU<br/>(un wake o el deadline lo despiertan)
        OS-->>Pr: CQE lista (result = bytes o -errno)
        Pr->>Aw: ejecuta on_done(result)
        Aw->>Aw: result_ = result
        Aw->>Coro: handle.resume()
        Coro->>Aw: await_resume()
        Aw-->>Coro: expected<size_t> (result_to_size)
    end

    Note over Coro: continúa tras el co_await con el dato leído
```

> Las *completions* se sirven **fuera** del scheduler de corrutinas: `submit_*` nunca ejecuta el callback de forma síncrona (reentraría en una corrutina aún suspendiéndose), siempre se difiere a `wait_completions`/`run_completions`. El `buffer` y, en `connect`, la dirección deben sobrevivir hasta la *completion*: viven en el *frame* de la corrutina que hace `co_await`. Para enrutar a otro núcleo antes o después de la E/S, ver [sharding](./06-sharding.md) y [topología](./05-topologia-thread-per-core.md).
