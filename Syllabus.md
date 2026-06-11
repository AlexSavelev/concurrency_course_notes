# Syllabus курса concurrent programming

## Арка I. Фундамент

| No  | Title                                                                             |
| --- | --------------------------------------------------------------------------------- |
| 0   | [Glossary](<Glossary.md>)                                                         |
| 1   | [Networking. Single-threaded server](<Networking. Single-threaded server.md>)     |
| 2   | [Multithreaded server. Mutex](<Multithreaded server. Mutex.md>)                   |
| 3   | [Condition Variable. Thread Pool](<Condition Variable. Thread Pool.md>)           |
| 4   | [Spinlock. Atomic. Mutex](<Spinlock. Atomic. Mutex.md>)                           |
| 5   | [Cache Coherence. MESI. False Sharing](<Cache Coherence. MESI. False Sharing.md>) |

## Арка II. Масштабирование ввода-вывода и асинхронная композиция

| No  | Title                                                                       |
| --- | --------------------------------------------------------------------------- |
| 6   | [IO multiplexing](<IO multiplexing.md>)                                     |
| 7   | [Futures, Executors, Async](<Futures, Executors, Async.md>)                 |
| 8   | [Async. Callback. Fibers. Context](<Async. Callback. Fibers. Context.md>)   |
| 9   | [Fibers. Sheduler. Multiprocessing](<Fibers. Sheduler. Multiprocessing.md>) |
| 10  | [Stackless Coroutines](<Stackless Coroutines.md>)                           |

## Арка III. Lock-free и модель памяти

| No  | Title                                                   |
| --- | ------------------------------------------------------- |
| 11  | [Lock Free DS](<Lock Free DS.md>)                       |
| 12  | [Memory Model 1. SC. DRF](<Memory Model 1. SC. DRF.md>) |
| 13  | [Memory Model 2. Cpp20](<Memory Model 2. Cpp20.md>)     |

## Приложения и дополнительные темы

| No  | Title                                                         |
| --- | ------------------------------------------------------------- |
| A1  | [A1 Work Stealing Scheduler](<A1 Work Stealing Scheduler.md>) |
| A2  | [A2 Lazy Futures](<A2 Lazy Futures.md>)                       |
