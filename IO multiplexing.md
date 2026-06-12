# Неблокирующий ввод-вывод и мультиплексирование (select / poll / epoll / io_uring)

## 1. Отправная точка: сервер с thread pool и блокирующим I/O

Архитектура классического пула потоков (thread pool): фиксированное число воркеров (workers) разбирает задачи из общей потокобезопасной очереди; пустая очередь — воркеры спят на condition variable; добавление задачи будит одного.

```
┌───────────────────────────────────────────────────────────┐
│                          Процесс                          │
│  ┌─────────────────────────────────────────────────────┐  │
│  │            Общая очередь задач (thread-safe)        │  │
│  │   [task1] [task2] [task3] [task4] [task5] ...       │  │
│  └─────────────────────────────────────────────────────┘  │
│                            │                              │
│          ┌─────────────────┼─────────────────┐            │
│          ▼                 ▼                 ▼            │
│   ┌────────────┐    ┌────────────┐    ┌────────────┐      │
│   │  Worker 1  │    │  Worker 2  │    │  Worker N  │      │
│   │ while(true)│    │ while(true)│    │ while(true)│      │
│   │  pop+run   │    │  pop+run   │    │  pop+run   │      │
│   └────────────┘    └────────────┘    └────────────┘      │
└───────────────────────────────────────────────────────────┘
```

> Полная реализация пула (блокирующая очередь, корректное завершение через `close`+`notify_all`+`join`, exception-safety) разобрана в файле про condition variable и thread pool. Здесь она используется как данность; повторять код не будем.

**Почему пул, а не поток на задачу.** Создание потока на каждую задачу накладно:

| Ресурс | Затраты |
|---|---|
| Память под стек | ~8 МБ (виртуально, по умолчанию) |
| Время создания | ~10–100 мкс |
| Структуры ядра | `task_struct`, `thread_info` |
| Переключение контекста (context switch) | ~1–10 мкс |

Пул переиспользует фиксированный набор потоков: 10 000 задач обслуживаются, скажем, 8 воркерами без 10 000 стеков.

## 2. Проблема блокирующего I/O внутри задач

Содержательный пример — **сервер проверки простоты чисел**: клиент шлёт число текстом (`"12345\n"`), сервер отвечает `prime` / `not prime`.

```cpp
bool is_prime(uint64_t n) {                 // используется во всех серверах ниже
    if (n <= 1) return false;
    if (n <= 3) return true;
    if (n % 2 == 0 || n % 3 == 0) return false;
    for (uint64_t i = 5; i * i <= n; i += 6)
        if (n % i == 0 || n % (i + 2) == 0) return false;
    return true;
}

void handle_client(int fd) {
    char buf[1024];
    ssize_t k = read(fd, buf, sizeof(buf) - 1);   // ПРОБЛЕМА 1: блокирующий read
    if (k <= 0) { close(fd); return; }
    buf[k] = '\0';
    std::string resp = is_prime(std::stoull(buf)) ? "prime\n" : "not prime\n";
    write(fd, resp.c_str(), resp.size());          // ПРОБЛЕМА 2: блокирующий write
    close(fd);
}

// main: socket → setsockopt(SO_REUSEADDR) → bind → listen → ThreadPool pool(8);
// while (true) { int fd = accept(...); pool.enqueue([fd]{ handle_client(fd); }); }
```

**Демонстрация проблемы «медленного клиента».** Запустим клиентов, часть которых после `connect` засыпает, не отправляя данные:

```cpp
void bad_client(const char* host, int port, int sleep_seconds) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
    inet_pton(AF_INET, host, &a.sin_addr);
    connect(s, (sockaddr*)&a, sizeof(a));
    sleep(sleep_seconds);                  // серверный воркер заблокирован на read всё это время!
    std::string m = "12345\n"; write(s, m.c_str(), m.size());
    char buf[1024]; ssize_t n = read(s, buf, sizeof(buf) - 1);
    if (n > 0) { buf[n] = '\0'; }
    close(s);
}
```

Если медленных клиентов столько же, сколько воркеров (8), все воркеры залипают на `read`, и новые клиенты не обслуживаются, пока кто-то не освободится.

**Ключевое наблюдение:** проблема не в пуле, а в **блокирующем I/O** внутри задач. Воркер тратит время не на работу, а на ожидание медленной сети.

## 3. Неблокирующий I/O и busy loop

Переведём сокеты в **неблокирующий режим (non-blocking mode)**:

```cpp
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
```

Теперь `read`/`accept`/`write` при отсутствии готовности немедленно возвращают `-1` с `errno == EAGAIN` (или `EWOULDBLOCK`), а не блокируются.

**Идея однопоточного busy loop:** один поток в бесконечном цикле принимает новые соединения (неблокирующий `accept`), затем проходит по всем активным клиентам и пытается неблокирующе прочитать/обработать/ответить. «Тупящий» клиент даёт `EAGAIN`, и поток сразу переходит к следующему — никто никого не блокирует.

```cpp
struct Client {
    int fd;
    enum State { READING, PROCESSING, WRITING, CLOSED } state;   // CLOSED — явная пометка (исправлено)
    std::string in_buf, out_buf;
    uint64_t number = 0;
    bool result = false;
};

class NonBlockingServer {
    int server_fd;
    std::vector<Client> clients;

    void accept_new() {
        int fd = accept(server_fd, nullptr, nullptr);
        if (fd < 0) { /* EAGAIN — нет новых */ return; }
        set_nonblocking(fd);
        clients.push_back({fd, Client::READING, "", "", 0, false});
    }

    void handle_read(Client& c) {
        char buf[4096];
        ssize_t n = read(c.fd, buf, sizeof(buf));
        if (n < 0) { if (errno != EAGAIN && errno != EWOULDBLOCK) { close(c.fd); c.state = Client::CLOSED; } return; }
        if (n == 0) { close(c.fd); c.state = Client::CLOSED; return; }   // EOF
        c.in_buf.append(buf, n);
        size_t nl = c.in_buf.find('\n');
        if (nl != std::string::npos) {
            try { c.number = std::stoull(c.in_buf.substr(0, nl)); c.state = Client::PROCESSING; }
            catch (...) { c.out_buf = "invalid number\n"; c.state = Client::WRITING; }
        }
    }

    void handle_write(Client& c) {
        if (c.out_buf.empty()) c.out_buf = c.result ? "prime\n" : "not prime\n";
        ssize_t n = write(c.fd, c.out_buf.data(), c.out_buf.size());
        if (n < 0) { if (errno != EAGAIN && errno != EWOULDBLOCK) { close(c.fd); c.state = Client::CLOSED; } return; }
        c.out_buf.erase(0, n);
        if (c.out_buf.empty()) { close(c.fd); c.state = Client::CLOSED; }
    }

public:
    void run() {
        while (true) {
            accept_new();
            for (auto& c : clients) {
                switch (c.state) {
                    case Client::READING:    handle_read(c); break;
                    case Client::PROCESSING: c.result = is_prime(c.number); c.state = Client::WRITING; break;
                    case Client::WRITING:    handle_write(c); break;
                    case Client::CLOSED:     break;
                }
            }
            std::erase_if(clients, [](const Client& c) { return c.state == Client::CLOSED; });  // явный флаг, без догадок
        }
    }
};
```

**Плюсы busy loop:** очень низкая задержка (данные обрабатываются почти сразу, без переключений контекста — поток постоянно активен); простота относительно асинхронных моделей.

**Минусы:**
1. **100% CPU на холостом ходу** — даже без клиентов поток крутится, опрашивая `accept` (всегда `EAGAIN`) и пустой список.
2. **Сложность O(N)** — каждую итерацию обходим всех клиентов, даже неготовых; при 100 000 соединений и одном готовом мы всё равно трогаем все 100 000.
3. **Сисколл на каждый fd** — каждый `read` неготового сокета — это переход в ядро и обратно впустую.

**Вывод:** busy loop годится для малого числа соединений и low-latency, но не масштабируется. Нужен механизм усыпить поток до появления событий **сразу на множестве** дескрипторов.

## 4. Мультиплексирование: select / poll / epoll

### 4.0. Две модели: reactor и proactor

Прежде чем перебирать API, зафиксируем главный кадр, в который они укладываются:

- **Reactor (модель готовности, readiness):** ядро уведомляет «дескриптор **готов** к операции», а саму операцию (`read`/`write`) выполняете вы. Так работают `select`, `poll`, `epoll`. На операцию приходится минимум два сисколла: «дождаться готовности» + «сделать I/O».
- **Proactor (модель завершения, completion):** вы просите ядро **выполнить** операцию, и оно уведомляет «операция **завершена**, вот результат/данные». Так работают `io_uring` (Linux) и IOCP (Windows). Сисколл «сделать I/O» исчезает — он совмещён с уведомлением.

Весь раздел ниже — про reactor; §5 (io_uring) — про proactor. Держа это различие в голове, легко понять, зачем вообще нужен io_uring.

**Синхронный против асинхронного вызова (определение).** Зафиксируем понятие, которым дальше пользуемся. Вызов функции (`read`, `accept`, …) предоставляет некоторую **логическую операцию**. Вызов **синхронный**, если возврат управления из него означает, что операция **завершена** (из `read` вернулись — данные прочитаны). Вызов **асинхронный**, если он возвращает управление **сразу**, а операция при этом может быть ещё не завершена; результат приходит позже — через колбэк/handler или уведомление о завершении. Reactor оставляет ваши `read`/`write` синхронными (пусть и неблокирующими), добавляя лишь асинхронное ожидание готовности; proactor делает асинхронной саму операцию.

**Во что превращается код в асинхронной (колбэчной) модели.** Когда поверх reactor строят колбэчный фреймворк (например, Boost.Asio, разбор сервера — в файле про async/callback/context), происходит характерная трансформация. Явные циклы программы **растворяются**: в эхо-сервере есть два логических цикла (принимать клиентов; для каждого читать-писать), но в асинхронной версии остаётся **ровно один** цикл — `epoll`-овый **event loop** внутри `io_context.run()`, которому подчинено всё остальное. Прежние циклы распадаются в **цепочки/граф обработчиков** (handler'ов), которые «толкают» сами себя на следующих итерациях event loop. Программа становится **явным автоматом**: локальные переменные превращаются в поля объекта-сессии, позиция в коде (после `read` / после `write`) — в состояние. Ошибки передаются **кодами возврата**, а не исключениями: исключению из обработчика лететь некуда, кроме самого event loop, где его не обработать. А время жизни сессии вручную продлевают через `shared_ptr`, захваченный в колбэке, — пока запланирован следующий колбэк, объект жив. Эта «вывернутая наизнанку» форма кода (callback hell) и мотивирует файберы/корутины, которые скрывают разрезы и возвращают синхронный вид (см. файлы про context switch и stackless-корутины).

### 4.1. select

```cpp
int select(int nfds, fd_set* readfds, fd_set* writefds, fd_set* exceptfds, timeval* timeout);
```

Пользователь заполняет битовые маски `fd_set` нужными дескрипторами; `select` блокируется, пока хотя бы один fd не станет готов; по возврату маски **перезаписываются** — остаются только готовые fd (поэтому перед каждым вызовом маски нужно пересоздавать).

```cpp
class SelectServer {
    int server_fd, max_fd;
    fd_set master_set, read_set;
public:
    void run() {
        while (true) {
            read_set = master_set;                                  // копия (O(N))
            int activity = select(max_fd + 1, &read_set, nullptr, nullptr, nullptr);
            if (activity < 0) { if (errno == EINTR) continue; perror("select"); continue; }
            if (FD_ISSET(server_fd, &read_set)) {
                int fd = accept(server_fd, nullptr, nullptr);
                if (fd >= 0) { set_nonblocking(fd); FD_SET(fd, &master_set); max_fd = std::max(max_fd, fd); }
            }
            for (int fd = 0; fd <= max_fd; ++fd) {                   // линейный обход (O(N))
                if (fd == server_fd || !FD_ISSET(fd, &read_set)) continue;
                char buf[4096];
                ssize_t n = read(fd, buf, sizeof(buf));
                if (n <= 0) { close(fd); FD_CLR(fd, &master_set); }
                else { /* обработка */ }
            }
        }
    }
};
```

**Недостатки select:**
- Жёсткий лимит на номер дескриптора `FD_SETSIZE` (обычно 1024) — больше 1024 клиентов без перекомпиляции ядра не обслужить. Метод из 1980-х, когда этого хватало.
- Маски при каждом вызове копируются user→kernel.
- По возврату нужно просканировать все биты `0..nfds-1` (O(N)).
- Три раздельные маски (чтение/запись/исключения) — неудобно.

### 4.2. poll

```cpp
int poll(struct pollfd* fds, nfds_t nfds, int timeout);
struct pollfd { int fd; short events; short revents; };   // revents заполняет ядро
```

Передаётся массив `pollfd` произвольной длины. **Лучше select:** нет лимита на номер fd; одно поле объединяет типы событий; массив можно переиспользовать, очищая `revents`.

```cpp
class PollServer {
    int server_fd;
    std::vector<pollfd> fds;   // fds[0] — слушающий сокет
public:
    void run() {
        while (true) {
            int activity = poll(fds.data(), fds.size(), -1);
            if (activity < 0) { if (errno == EINTR) continue; perror("poll"); continue; }
            if (fds[0].revents & POLLIN) {
                int fd = accept(server_fd, nullptr, nullptr);
                if (fd >= 0) { set_nonblocking(fd); fds.push_back({fd, POLLIN, 0}); }
            }
            for (size_t i = 1; i < fds.size(); ++i) {                // всё ещё O(N)
                if (!(fds[i].revents & POLLIN)) continue;
                char buf[4096]; ssize_t n = read(fds[i].fd, buf, sizeof(buf));
                if (n <= 0) { close(fds[i].fd); fds.erase(fds.begin() + i); --i; }
                else { /* обработка */ }
            }
        }
    }
};
```

**Недостатки poll:** весь массив по-прежнему копируется в ядро при каждом вызове; по возврату — линейное сканирование (O(N)); при сотнях тысяч fd эффективность падает. Внутри ядра poll тоже линейно сканирует, просто без лимита на номер fd.

### 4.3. epoll (Linux)

```cpp
int  epoll_create1(int flags);                                   // создать инстанс, вернуть epfd
int  epoll_ctl(int epfd, int op, int fd, struct epoll_event* ev);// ADD / MOD / DEL
int  epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);
// struct epoll_event { uint32_t events; epoll_data_t data; };   // events: EPOLLIN, EPOLLOUT, EPOLLET, ...
```

Однопоточный epoll-сервер для нашего протокола (edge-triggered, чтение до `EAGAIN`):

```cpp
class EpollServer {
    int server_fd, epoll_fd;
    struct State { enum { READING, WRITING } st; std::string in, out; };
    std::unordered_map<int, State> conns;

    void mod(int fd, uint32_t events) { epoll_event ev{events, {}}; ev.data.fd = fd; epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev); }
    void add(int fd, uint32_t events) { epoll_event ev{events, {}}; ev.data.fd = fd; epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev); }
    void del(int fd) { epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr); close(fd); conns.erase(fd); }

    void on_accept() {
        while (true) {                              // в ET читаем все готовые соединения
            int fd = accept4(server_fd, nullptr, nullptr, SOCK_NONBLOCK);
            if (fd < 0) break;                      // EAGAIN
            conns[fd] = {State::READING, "", ""};
            add(fd, EPOLLIN | EPOLLET);
        }
    }
    void on_read(int fd) {
        auto& s = conns[fd]; char buf[4096]; ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0) s.in.append(buf, n);   // до EAGAIN
        if (n == 0) { del(fd); return; }
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) { del(fd); return; }
        size_t nl = s.in.find('\n');
        if (nl != std::string::npos) {
            try { s.out = is_prime(std::stoull(s.in.substr(0, nl))) ? "prime\n" : "not prime\n"; }
            catch (...) { s.out = "invalid number\n"; }
            s.st = State::WRITING; mod(fd, EPOLLOUT | EPOLLET);
        }
    }
    void on_write(int fd) {
        auto& s = conns[fd];
        while (!s.out.empty()) {
            ssize_t n = write(fd, s.out.data(), s.out.size());
            if (n < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) return; del(fd); return; }
            s.out.erase(0, n);
        }
        del(fd);
    }
public:
    void run() {
        epoll_event events[1024];
        add(server_fd, EPOLLIN | EPOLLET);
        while (true) {
            int n = epoll_wait(epoll_fd, events, 1024, -1);
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == server_fd) on_accept();
                else if (events[i].events & EPOLLIN)  on_read(fd);
                else if (events[i].events & EPOLLOUT) on_write(fd);
            }
        }
    }
};
```

**Ключевые отличия epoll от select/poll:**
- **Разделение регистрации и ожидания.** Дескрипторы регистрируются один раз (`epoll_ctl`), а не передаются массивом при каждом ожидании — нет копирования больших наборов.
- **Структура в ядре.** Зарегистрированные fd хранятся в **красно-чёрном дереве (red-black tree)** → `O(log N)` на добавление/удаление. Для каждого fd ядро ставит callback, который при готовности кладёт fd в **список готовых**; `epoll_wait` возвращает этот список.
- **Сложность ожидания `O(K)`**, где K — число готовых событий (а не `O(N)` от всех зарегистрированных).

| Характеристика | select / poll | epoll |
|---|---|---|
| Сложность ожидания | O(N) | O(K), K — число готовых |
| Копирование набора fd | при каждом вызове | только при `epoll_ctl` |
| Добавление/удаление fd | перестроение масок/массива | O(log N) (rb-дерево) |
| Лимит fd | 1024 (select) | десятки–сотни тысяч |

**Недостаток epoll (он же мотивация io_uring):** для самого I/O всё равно нужен отдельный `read`/`write` после того, как epoll сообщил о готовности — **два сисколла на операцию** (`epoll_wait` + `read`/`write`). Это reactor-модель.

### 4.4. Edge-triggered против level-triggered

- **Level-triggered (LT)** — по умолчанию: пока fd готов (есть данные), `epoll_wait` возвращает его на каждом вызове. Удобно: можно читать порциями.
- **Edge-triggered (ET)** — флаг `EPOLLET`: fd возвращается только при **переходе** «не готов → готов»; пока не придёт новая порция, событие не повторится. Поэтому в ET **обязательно** читать/писать в цикле до `EAGAIN`, иначе остаток данных «зависнет» до следующего события.

```cpp
void handle_read_ET(int fd) {
    char buf[4096]; ssize_t n;
    while ((n = read(fd, buf, sizeof(buf))) > 0) process(buf, n);
    if (n < 0 && errno != EAGAIN) { /* реальная ошибка */ }
}
```

### 4.5. Многопоточный сервер на epoll

Одного event loop мало, если обработка событий упирается в одно ядро. Есть два рабочих подхода.

**(A) Шардинг по `SO_REUSEPORT` — предпочтительный.** На каждое ядро — свой поток со своим epoll-инстансом и своим слушающим сокетом, забинженным на тот же порт через `SO_REUSEPORT`. Ядро само балансирует входящие соединения по сокетам (хеш по 4-tuple). Потоки **не делят** ни epoll, ни accept-очередь → нет contention и thundering herd, масштабирование близко к линейному.

```cpp
void worker(int port) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));   // у каждого потока СВОЙ сокет на том же порту
    /* bind(port); listen(); set_nonblocking(s); */
    EpollServer{ /* свой epfd, слушающий s */ }.run();
}
// for (int i = 0; i < num_cores; ++i) std::thread(worker, 8080).detach();
```

**(B) Общий epoll, несколько воркеров.** Если несколько потоков делают `epoll_wait` на **одном** epfd, возникают две тонкости (и здесь же — исправление частого заблуждения):

- **Грохочущее стадо (thundering herd).** Само по себе `EPOLLET` **не гарантирует** пробуждение ровно одного потока (вопреки распространённой формулировке) — ET лишь меняет семантику взведения события. Гарантию «разбудить лишь одного из ждущих на общем epfd» даёт флаг **`EPOLLEXCLUSIVE`** (Linux 4.5+) при регистрации слушающего сокета.
- **Безопасная передача fd воркеру.** Если оставить клиентский fd в общем epoll в LT/ET без защиты, два воркера могут получить событие по одному и тому же fd и наперегонки его обрабатывать. Решение — **`EPOLLONESHOT`**: после первого события fd «деактивируется» и больше не возвращается, пока вы явно не пере-вооружите его `EPOLL_CTL_MOD`. Это гарантирует, что в любой момент с fd работает ровно один поток; типовой паттерн для связки «общий epoll + thread pool».

```cpp
add(listen_fd, EPOLLIN | EPOLLEXCLUSIVE);            // на accept будят одного воркера
add(client_fd, EPOLLIN | EPOLLET | EPOLLONESHOT);    // событие отдаётся одному воркеру
// обработав, воркер: mod(client_fd, EPOLLIN | EPOLLET | EPOLLONESHOT);  // пере-вооружить
```

На практике шардинг (A) проще и быстрее; (B) с `EPOLLONESHOT` нужен, когда соединение «гуляет» между воркерами или передаётся в пул для CPU-heavy работы.

## 5. io_uring: модель завершения

**Мотивация:** убрать «лишний» сисколл reactor-модели и снизить накладные расходы при интенсивном I/O. io_uring (Linux 5.1, 2019) — это proactor: ядро **само выполняет** операции и кладёт результаты.

```
Пользовательское пространство              Ядро
┌─────────────────────────┐               ┌─────────────────┐
│  Submission Queue (SQ)  │ ── mmap ──────│                 │
│  [ SQE | SQE | SQE | …] │   (общая      │  I/O scheduler  │
│           │             │    память)    │                 │
│           ▼             │               │                 │
│  Completion Queue (CQ)  │ ◄─────────────│                 │
│  [ CQE | CQE | CQE | …] │               └─────────────────┘
└─────────────────────────┘
```

- Две **разделяемые кольцевые очереди (ring buffers)**, отображённые через `mmap` и в user space, и в ядро; синхронизация — атомарными `head`/`tail`.
- **SQ (Submission Queue):** очередь запросов. Пользователь заполняет **SQE (Submission Queue Entry)**: fd, opcode (`IORING_OP_READ`, `IORING_OP_ACCEPT`, …), буфер (адрес+длина), флаги, `user_data` (для обратной связи).
- **CQ (Completion Queue):** очередь результатов. Ядро по завершении пишет **CQE (Completion Queue Entry)**: статус + `user_data`.

Уведомить ядро о новых SQE можно двумя способами:
1. сисколлом `io_uring_enter` (режим syscall);
2. в режиме **`IORING_SETUP_SQPOLL`** ядро **само опрашивает** SQ выделенным потоком — тогда подача запросов вообще без сисколлов.

**Плюсы по сравнению с epoll:**
- **Меньше сисколлов:** I/O совмещён с уведомлением (в epoll — `epoll_wait` + `read`); с SQPOLL сисколлов может не быть вовсе.
- **Батчинг (batching):** подаём много SQE, уведомляем ядро одним вызовом.
- **Меньшая задержка** за счёт отсутствия переключения контекста на каждый syscall.

**Минусы по сравнению с epoll:**
- io_uring **сам по себе не устраняет копирование** данных в пользовательский буфер (нужны отдельные zero-copy техники — `splice`, registered buffers).
- Разделяемая память + многопоточный доступ к кольцам требуют аккуратной синхронизации.
- Историческая незрелость безопасности: ранние версии имели уязвимости (Google какое-то время запрещал io_uring в Chrome). Со временем улучшается.
- Заметно **сложнее код**, чем epoll.

**Скелет сервера на liburing** (упрощённо; диспетчеризация по `user_data`):

```cpp
#include <liburing.h>
struct Conn { int fd; enum { ACCEPT, READ, WRITE } op; std::string in, out; char buf[4096]; };

class IoUringServer {
    io_uring ring;
    int server_fd;

    void submit_accept() {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        io_uring_prep_accept(sqe, server_fd, nullptr, nullptr, 0);
        auto* c = new Conn{server_fd, Conn::ACCEPT};
        io_uring_sqe_set_data(sqe, c);
        io_uring_submit(&ring);
    }
    void submit_read(Conn* c) {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        io_uring_prep_read(sqe, c->fd, c->buf, sizeof(c->buf), 0);
        c->op = Conn::READ; io_uring_sqe_set_data(sqe, c);
        io_uring_submit(&ring);
    }
    void submit_write(Conn* c) {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        io_uring_prep_write(sqe, c->fd, c->out.data(), c->out.size(), 0);
        c->op = Conn::WRITE; io_uring_sqe_set_data(sqe, c);
        io_uring_submit(&ring);
    }
public:
    void run() {
        submit_accept();
        io_uring_cqe* cqe;
        while (io_uring_wait_cqe(&ring, &cqe) == 0) {
            Conn* c = static_cast<Conn*>(io_uring_cqe_get_data(cqe));
            int res = cqe->res;                         // результат операции (или -errno)
            if (c->op == Conn::ACCEPT) {
                submit_accept();                        // снова ждём новых
                if (res >= 0) submit_read(new Conn{res, Conn::READ});
            } else if (c->op == Conn::READ) {
                if (res <= 0) { close(c->fd); delete c; }
                else {
                    c->in.append(c->buf, res);
                    if (c->in.find('\n') != std::string::npos) {
                        try { c->out = is_prime(std::stoull(c->in)) ? "prime\n" : "not prime\n"; }
                        catch (...) { c->out = "invalid number\n"; }
                        submit_write(c);
                    } else submit_read(c);
                }
            } else { /* WRITE */ close(c->fd); delete c; }
            io_uring_cqe_seen(&ring, cqe);
        }
    }
};
```

**Продвинутые возможности io_uring:**

```cpp
io_uring_register_files(&ring, fds, nfds);   // fixed files: меньше оверхед на разрешение fd
io_uring_register_buffers(&ring, iov, n);    // registered buffers: путь к zero-copy
io_uring_prep_splice(sqe, in, ..., out, ..., len, flags);  // составные операции
io_uring_prep_timeout(sqe, &ts, count, flags);             // таймеры в кольце
sqe->flags |= IOSQE_IO_LINK;                 // линк: следующая операция после этой
```

**Режимы:**
```cpp
// 1) syscall-режим
io_uring_submit(&ring);          // syscall
io_uring_wait_cqe(&ring, &cqe);  // syscall
// 2) polling-режим: ядро само опрашивает SQ — подача без сисколлов
io_uring_params p{}; p.flags = IORING_SETUP_SQPOLL;
io_uring_queue_init_params(QUEUE_DEPTH, &ring, &p);
```

### 5.1. Многопоточный io_uring

- **Ring на поток** — простейшая и обычно лучшая схема: у каждого воркера своё кольцо, делёжки нет. В связке с `SO_REUSEPORT` распределяет и приём соединений (мультишот-accept ниже).
- **`IORING_SETUP_ATTACH_WQ`** — несколько колец разделяют общий ядрённый пул рабочих (io-wq), чтобы не плодить лишние потоки ядра.
- **Multishot-операции** — `IORING_OP_*` в multishot-режиме (multishot accept, `IORING_RECV_MULTISHOT`): один поданный SQE порождает **много** CQE по мере событий, резко сокращая число submit'ов (свежие ядра 5.19+/6.x).
- **Provided / ring-mapped buffers** (`IORING_REGISTER_PBUF_RING`) — ядро само выбирает буфер из заранее предоставленного кольца **в момент завершения**, а не вы заранее под каждый in-flight `read`; ближе к zero-copy приёму и без преаллокации на соединение.

Скелет «ring-per-thread»: на каждое ядро — свой поток с собственным кольцом и собственным слушающим сокетом на том же порту через `SO_REUSEPORT` (ядро балансирует соединения, кольца не делятся → нет contention):

```cpp
void worker(uint16_t port) {
    int lfd = make_listener(port);              // socket+SO_REUSEPORT+bind+listen на одном порту во всех потоках
    IoUringServer srv;                          // своё io_uring на этот поток
    io_uring_queue_init(QUEUE_DEPTH, &srv.ring, 0);
    srv.server_fd = lfd;
    srv.run();                                  // тот же цикл submit/wait_cqe, что и в §5 (на своём кольце)
}

int main() {
    unsigned n = std::thread::hardware_concurrency();
    std::vector<std::thread> pool;
    for (unsigned i = 0; i < n; ++i) pool.emplace_back(worker, 8080);
    for (auto& t : pool) t.join();
}
```

Здесь нет общего состояния между воркерами вовсе — это share-nothing-схема (ср. с work-stealing в приложении A1). Альтернатива — одно кольцо на несколько потоков с `IORING_SETUP_ATTACH_WQ`, но ring-per-thread обычно быстрее.

### 5.2. О низкой задержке

Когда борются за нано- и микросекунды (HFT и пр.): `epoll_wait` сам по себе — сисколл, и его исключают переходом на io_uring SQPOLL; sq-поток ядра прикрепляют к выделенному ядру (`sqthread`-аффинити), приложение — через `isolcpus` и привязку IRQ к отдельным ядрам; включают busy-poll/NAPI на сетевой карте; в пределе — **kernel bypass** (DPDK, AF_XDP), полностью минуя стек ядра. Это «карта», куда уходят за задержкой; детали — вне темы.

## 6. Итоговое сравнение и выбор

| Параметр | Thread pool + blocking | Non-blocking busy loop | select | poll | epoll | io_uring |
|---|---|---|---|---|---|---|
| Сисколлов на операцию | 1–2 (read/write) | 1 на проверку каждого fd | 1 (select) + 1 (read) | 1 (poll) + 1 (read) | 1 (epoll_wait) + 1 (read) | 0–1 (с polling) |
| Сложность ожидания | O(1) на поток | O(N) обход | O(N) | O(N) | O(K) готовых | O(K) готовых |
| Копирование набора fd | — | — | маски каждый раз | массив каждый раз | только при ctl | кольца в общей памяти |
| Макс. соединений | ограничено потоками | теоретически без лимита | 1024 | без лимита | без лимита | без лимита |
| CPU на холостом ходу | ~0% | 100% одного ядра | ~0% | ~0% | ~0% | ~0% (или ~100% при SQPOLL) |
| Латентность | высокая (контекст) | минимальная | средняя | средняя | низкая | минимальная |
| Сложность кода | низкая | средняя | низкая | низкая | средняя | высокая |

**Выбор по сценарию:**
- Несколько сотен соединений, латентность некритична → `poll`/`epoll` в LT.
- Тысячи–десятки тысяч соединений, высокая пропускная способность → `epoll` в ET + неблокирующий I/O (+ шардинг по `SO_REUSEPORT`).
- Минимальная латентность, HFT → `io_uring` с SQPOLL + изоляция и аффинити ядер.
- Очень высокий темп операций I/O → `io_uring` с registered files/buffers и multishot.

**Выводы:**
- Для большинства высоконагруженных серверов (веб, БД) **epoll** — стандарт де-факто.
- **io_uring** даёт более радикальную оптимизацию для интенсивного I/O и требовательных к задержке систем (хранилища, БД, HFT), но ценой сложности.
- На практике редко работают с epoll/io_uring напрямую — поверх лежат обёртки (Boost.Asio, libevent, Tokio в Rust), дающие удобный асинхронный интерфейс. Как из неблокирующего I/O собрать удобную асинхронную композицию (futures, executors, корутины) — следующая тема.
