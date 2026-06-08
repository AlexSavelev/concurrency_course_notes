# Сети и синхронный однопоточный TCP-сервер

## 1. Основы сетевого взаимодействия

### 1.1. Протоколы и модель OSI

**Протокол (protocol)** — формализованный набор правил, задающий формат и порядок сообщений. В практике доминирует стек TCP/IP, который грубо соотносится с уровнями модели OSI:

- **L7, прикладной (application):** HTTP, FTP, SMTP.
- **L4, транспортный (transport):** TCP (надёжный поток), UDP (датаграммы).
- **L3, сетевой (network):** IP (адресация, маршрутизация).
- **L2, канальный (data link):** Ethernet, Wi-Fi.
- **L1, физический (physical):** биты и сигналы.

Прикладному разработчику видны в основном L7 и L4; нижние уровни работают «под капотом» через **инкапсуляцию (encapsulation)** — каждый уровень оборачивает данные верхнего в свой заголовок.

### 1.2. TCP как надёжный поток байтов

**TCP (Transmission Control Protocol)** даёт **дуплексный надёжный поток байтов (reliable byte stream)** между двумя процессами. Надёжность означает: повторная пересылка потерянных сегментов, сохранение порядка (нумерация), отбрасывание дубликатов, управление потоком (flow control) и борьба с перегрузками (congestion control). С точки зрения приложения соединение — это просто файловый дескриптор, в который пишут и из которого читают.

Ключевое следствие, которое студенты недооценивают: TCP — это **поток байтов, а не сообщений**. Границы `write` не сохраняются (см. §4).

### 1.3. Порты и сокеты

**Порт (port)** — 16-битный идентификатор процесса-получателя на хосте. Порты 0–1023 (well-known) требуют привилегий. Клиент обычно получает **эфемерный порт (ephemeral port)** от ядра автоматически.

**Сокет (socket)** — абстракция одного конца соединения; в UNIX это файловый дескриптор, поэтому к нему применимы `read`, `write`, `close`. Соединение уникально идентифицируется четвёркой `(src_ip, src_port, dst_ip, dst_port)` — поэтому на одном серверном порту живут сотни тысяч соединений одновременно.

Основные системные вызовы: `socket` (создать), `bind` (привязать к адресу), `listen` (перейти в пассивный режим), `accept` (принять соединение), `connect` (клиент инициирует соединение), `read`/`write` или `recv`/`send` (обмен), `close`/`shutdown` (закрыть).

## 2. Системные вызовы для TCP-сокетов

```c
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
```

### 2.1. `socket` — создание сокета

```c
int sock = socket(int domain, int type, int protocol);
```
- `domain`: `AF_INET` (IPv4), `AF_INET6` (IPv6), `AF_UNIX` (локальные).
- `type`: `SOCK_STREAM` (TCP), `SOCK_DGRAM` (UDP). Можно сразу добавить `SOCK_NONBLOCK | SOCK_CLOEXEC`.
- `protocol`: обычно `0` (протокол по умолчанию для типа).

Возвращает дескриптор или `-1` с установкой `errno`.

### 2.2. `bind` — привязка к адресу. Порядок байтов

```c
struct sockaddr_in addr;
memset(&addr, 0, sizeof(addr));
addr.sin_family = AF_INET;
addr.sin_port = htons(8080);          // порт в сетевом порядке байт
addr.sin_addr.s_addr = INADDR_ANY;    // 0.0.0.0 — все интерфейсы
bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
```

Адреса заполняются в **сетевом порядке байтов (network byte order, big-endian)**. Преобразования: `htons`/`htonl` (host → network), `ntohs`/`ntohl` (обратно). `INADDR_ANY` — слушать на всех интерфейсах; конкретный адрес задаётся через `inet_pton`.

### 2.3. `SO_REUSEADDR`

После перезапуска сервера старый сокет может висеть в `TIME_WAIT`, и `bind` вернёт `EADDRINUSE`. Опция снимает запрет на повторную привязку и ставится **до** `bind`:

```c
int opt = 1;
setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
```

Важно не путать с `SO_REUSEPORT` — это другая опция (балансировка соединений между несколькими слушающими сокетами; см. файл про многопоточный сервер).

### 2.4. `listen` — пассивный режим и backlog

```c
listen(server_fd, SOMAXCONN);
```

Второй аргумент — **backlog**: максимум полностью установленных (`ESTABLISHED`) соединений, ожидающих `accept`. Фактический предел ограничен `/proc/sys/net/core/somaxconn`. Тонкость, важная для понимания (детали — в §7): в ядре Linux на самом деле **две очереди**:
- **SYN queue** — наполовину открытые соединения (`SYN_RECV`), лимит `tcp_max_syn_backlog`;
- **accept queue** — завершившие рукопожатие, ждущие `accept`, лимит `backlog`.

### 2.5. `accept` и `accept4`

```c
struct sockaddr_in client_addr;
socklen_t len = sizeof(client_addr);
int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &len);
```

`accept` блокируется до появления соединения и возвращает **новый** дескриптор для общения с конкретным клиентом; слушающий сокет продолжает принимать новые. Это как многоканальный телефон: один номер (порт) для входящих, отдельные линии для разговоров.

На Linux предпочтительнее `accept4`, позволяющий атомарно выставить флаги:

```c
int client_fd = accept4(server_fd, (struct sockaddr*)&client_addr, &len,
                        SOCK_NONBLOCK | SOCK_CLOEXEC);
```

`SOCK_CLOEXEC` закрывает гонку: между `accept` и ручным `fcntl(fd, F_SETFD, FD_CLOEXEC)` другой поток мог бы сделать `fork`+`exec`, и дескриптор утёк бы в посторонний процесс.

### 2.6. Обмен: `read`/`write`, `recv`/`send`

`read` может вернуть **меньше** байт, чем запрошено (фрагментация потока), а `write` — записать меньше (заполнен буфер отправки). Это не ошибка, а норма TCP. Возврат `0` у `read` означает EOF — другая сторона закрыла соединение (прислала FIN). Корректная обработка частичных операций — в §4.

### 2.7. `close` и `shutdown`

`close` уменьшает счётчик ссылок на дескриптор; при достижении нуля инициируется закрытие соединения (FIN). `shutdown(fd, SHUT_WR)` закрывает только направление записи (half-close), не трогая чтение, — полезно, когда нужно сообщить «я всё отправил», но дочитать ответ.

## 3. Полный синхронный однопоточный сервер

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8080
#define BACKLOG 128

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(server_fd); exit(EXIT_FAILURE);
    }
    if (listen(server_fd, BACKLOG) < 0) {
        perror("listen"); close(server_fd); exit(EXIT_FAILURE);
    }
    printf("Listening on port %d\n", PORT);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;   // прерван сигналом — повторяем
            perror("accept");
            continue;                       // не валим сервер из-за одного клиента
        }

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        printf("Connection from %s:%d\n", ip, ntohs(client_addr.sin_port));

        char buffer[1024];
        ssize_t n = read(client_fd, buffer, sizeof(buffer) - 1);
        if (n > 0) {
            buffer[n] = '\0';
            write(client_fd, buffer, n);    // эхо
        }
        close(client_fd);
    }
}
```

**Главная черта** такого сервера: он обрабатывает клиентов строго **последовательно (sequential)**. Пока сервер занят одним, остальные ждут в accept-очереди или получают отказ. Это и есть мотивация для следующей темы.

### 3.1. Инструменты отладки

- `nc localhost 8080` (netcat) — голое TCP-соединение, передаёт байты как есть; `telnet` — постарше, навязывает свой протокол.
- `ss -tln` — слушающие сокеты; `ss -tn` — установленные; `netstat -tuna` — аналогично.
- После запуска виден сокет в `LISTEN`; после подключения — два `ESTABLISHED` (по одному с каждой стороны).

## 4. Фрейминг сообщений: поток ≠ сообщение

Поскольку TCP — поток байтов, **границы записей не сохраняются**: один `write(100 байт)` может прийти как два `read` по 50 байт, а два `write` по 50 — как один `read` на 100. Поэтому прикладной протокол обязан сам размечать границы сообщений. Два классических подхода:

1. **Префикс длины (length-prefix):** перед телом передаётся его длина фиксированным полем (например, 4 байта в сетевом порядке). Получатель читает длину, затем ровно столько байт.
2. **Разделитель (delimiter):** сообщения отделяются маркером (`\n`, `\r\n\r\n` в HTTP). Требует экранирования или гарантии, что маркер не встретится в теле.

Отсюда — обязательные хелперы, читающие/пишущие **ровно** N байт поверх частичных операций:

```c
// Прочитать ровно n байт (или вернуть < n при EOF/ошибке)
ssize_t readn(int fd, void *buf, size_t n) {
    size_t left = n;
    char *p = (char*)buf;
    while (left > 0) {
        ssize_t r = read(fd, p, left);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r == 0) break;                 // EOF
        left -= r; p += r;
    }
    return n - left;
}

// Записать ровно n байт
ssize_t writen(int fd, const void *buf, size_t n) {
    size_t left = n;
    const char *p = (const char*)buf;
    while (left > 0) {
        ssize_t w = write(fd, p, left);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        left -= w; p += w;
    }
    return n;
}
```

## 5. TCP-клиент

```c
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(8080);
    // inet_pton, а НЕ устаревший inet_addr: у inet_addr неоднозначный код ошибки
    // (INADDR_NONE == 0xFFFFFFFF совпадает с валидным 255.255.255.255).
    if (inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr) <= 0) {
        perror("inet_pton"); return 1;
    }

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect"); return 1;
    }

    const char *msg = "Hello from client";
    write(sock, msg, strlen(msg));

    char buffer[1024];
    ssize_t n = read(sock, buffer, sizeof(buffer) - 1);
    if (n > 0) { buffer[n] = '\0'; printf("Server: %s\n", buffer); }

    close(sock);
}
```

Клиент обычно **не вызывает `bind`** — ядро назначает эфемерный порт. `bind` перед `connect` нужен лишь когда требуется фиксированный исходящий порт/адрес.

## 6. Тонкости TCP, влияющие на корректность

### 6.1. Состояния соединения (конечный автомат)

`LISTEN` → `SYN_RECV` → `ESTABLISHED` (тройное рукопожатие); при закрытии — `FIN_WAIT1/2`, `CLOSE_WAIT`, `LAST_ACK`, `TIME_WAIT`. **`TIME_WAIT`** держится `2 * MSL` (Maximum Segment Lifetime) у стороны, **закрывшей первой**: чтобы «доживающие» в сети пакеты не попали в новое соединение с той же четвёркой. Если первым закрывает сервер, его сокеты копятся в `TIME_WAIT` и мешают перезапуску — отсюда `SO_REUSEADDR`.

### 6.2. `EINTR` — прерывание сигналом

Блокирующие вызовы (`accept`, `read`, `write`) могут прерваться сигналом и вернуть `-1` с `errno == EINTR`. В надёжном коде такой вызов повторяют (как в `readn`/`writen` выше).

### 6.3. `SIGPIPE` при записи в закрытое соединение

Запись в сокет, закрытый другой стороной, по умолчанию шлёт `SIGPIPE`, завершающий процесс. Решения: `signal(SIGPIPE, SIG_IGN)` (глобально), флаг `MSG_NOSIGNAL` в `send` (на запись), опция `SO_NOSIGPIPE` (BSD/macOS).

### 6.4. `EMFILE` — исчерпание дескрипторов (production-ловушка)

При нехватке дескрипторов `accept` возвращает `EMFILE`/`ENFILE`, но соединение **остаётся** в accept-очереди — и сервер уходит в busy-loop, мгновенно повторяя неудачный `accept`. Классический приём: заранее держать «запасной» открытый дескриптор; при `EMFILE` закрыть его, выполнить `accept`, тут же закрыть принятый сокет (вежливо отказав клиенту) и снова открыть запасной. Хорошая иллюстрация того, что в системном коде ошибки — часть нормального потока управления.

## 7. Низкая задержка

Базовый сервер корректен, но не оптимизирован под задержку. Минимальный набор того, что стоит знать (детали неблокирующего ввода-вывода и `io_uring`):

- **`TCP_NODELAY` и алгоритм Нагла.** Нагл (Nagle's algorithm) копит мелкие записи, пока не подтверждён предыдущий сегмент, чтобы экономить пакеты. В паре с **delayed ACK** на той стороне это даёт стойл до ~40 мс на типичном request/response. Для торговых и RPC-протоколов это неприемлемо — `setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one))` обязателен.
- **`SO_BUSY_POLL` / busy-polling.** Опрашивать сетевую очередь активно, не уходя в сон, — меньше задержка пробуждения ценой жжёного CPU.
- **Zero-copy.** `sendfile`, `MSG_ZEROCOPY` — передача без копирования между буферами пользователя и ядра; снижает нагрузку на память при больших объёмах.
- **Kernel bypass.** DPDK, AF_XDP — работа с NIC в обход стека ядра; так строят суб-микросекундные шлюзы.
- **`io_uring`.** Кольцевые буферы, разделяемые с ядром, резко сокращают число сисколлов на операцию.

Идея, проходящая через весь курс: blocking `read`/`write` — это пол по задержке, а не потолок. Каждый сисколл и каждое переключение контекста стоят денег, и борьба за латентность — это борьба за их сокращение.

## 8. Мостик к следующей теме

Последовательный сервер не способен обслуживать клиентов одновременно. Простейший выход — поток на клиента; масштабируемый — пул потоков и неблокирующий ввод-вывод. Этим займёмся в теме о многопоточном сервере, попутно разобрав примитивы синхронизации, без которых разделяемое состояние сервера становится источником гонок.
