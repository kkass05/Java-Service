# TXT → Kafka → SeaweedFS

Учебное backend-приложение для передачи небольших TXT-файлов через Kafka в SeaweedFS и получения через REST API. Содержимое передаётся как `byte[]`: кодировка, BOM и переводы строк сохраняются без изменений.

## Архитектура

```text
TXT → REST API / Java Producer → Kafka → Java Consumer → SeaweedFS (S3)
                                                             ↓
                                               REST API → просмотр / скачивание
```

Producer встроен в API. API и Consumer запускаются отдельными контейнерами одного Java-проекта с профилями `api` и `consumer`. Имя файла передаётся в ключе Kafka, исходные байты — в значении одного сообщения. Бакет `txt-files` создаётся автоматически.

## Технологии

| Компонент | Технология / версия |
| --- | --- |
| Язык | Java 21 |
| Backend | Spring Boot 3.5.16, Spring Web, Spring Kafka |
| Брокер | Apache Kafka 3.9.1, KRaft |
| Хранилище | SeaweedFS 3.85, S3 API |
| S3-клиент | AWS SDK for Java 2.31.54 |
| Сборка | Maven 3.9.9 в Docker |
| Запуск | Docker Compose |

## Запуск

Понадобится Docker с Compose. На Windows запустите Docker Desktop с Linux containers. Локальные Java и Maven для запуска через Docker не требуются.

Все команды выполняйте **из каталога проекта**, где находятся `compose.yaml` и `example.txt`. Например, в PowerShell:

```powershell
cd C:\Users\user\Documents\progging\HW_KN
docker compose up --build
```

После клонирования перейдите в каталог своего репозитория вместо пути из примера. Первый запуск скачивает образы и Maven-зависимости, выполняет тесты и собирает приложение. Дождитесь сообщения `Started TxtApplication` от API и Consumer.

Запуск в фоне и диагностика:

```powershell
docker compose up -d --build
docker compose ps
docker compose logs -f api consumer
```

API: http://localhost:8080. Kafka и SeaweedFS доступны внутри сети Compose. Данные хранятся в томах `kafka-data` и `seaweed-data`.

## Проверка через PowerShell

В проект включён `example.txt`:

```text
Hello World!
This is my first Kafka project.
```

Из каталога проекта отправьте файл:

```powershell
curl.exe --fail -i -F "file=@example.txt" http://localhost:8080/api/files/upload
```

Ответ `202 Accepted` означает, что Kafka приняла сообщение:

```json
{"name":"example.txt","status":"queued"}
```

Сохранение асинхронное. Проверьте список; если имя ещё не появилось, повторите запрос через несколько секунд:

```powershell
curl.exe --fail http://localhost:8080/api/files
```

Ожидаемый ответ: `["example.txt"]`. Скачайте файл и сравните SHA-256:

```powershell
curl.exe --fail -o downloaded.txt http://localhost:8080/api/files/example.txt
(Get-FileHash example.txt -ErrorAction Stop).Hash -eq (Get-FileHash downloaded.txt -ErrorAction Stop).Hash
```

Ожидаемый результат — `True`. Скачанный `downloaded.txt` исключён из Git. В Postman: POST → Body → form-data → поле `file` с типом File.

## Просмотр в браузере

Откройте [просмотр example.txt](http://localhost:8080/api/files/example.txt?download=false). Параметр `download=false` возвращает `Content-Disposition: inline`: браузер показывает текст как UTF-8, исходные байты не меняются. Другая кодировка может отображаться неправильно; скачанный файл сохраняет исходную кодировку.

[Ссылка без параметра](http://localhost:8080/api/files/example.txt) возвращает `Content-Disposition: attachment` и запускает скачивание. Пустая вкладка при скачивании не означает, что файл пустой: проверьте загрузки браузера.

## REST API

| Метод | Endpoint | Назначение |
| --- | --- | --- |
| POST | `/api/files/upload` | Загрузить TXT: multipart/form-data, поле `file` |
| GET | `/api/files` | Отсортированный JSON-массив имён сохранённых файлов |
| GET | `/api/files/{name}` | Скачать исходный файл |
| GET | `/api/files/{name}?download=false` | Показать текст в браузере как UTF-8 |

Имена с пробелами и специальными символами в URL нужно URL-кодировать.

| Статус | Значение |
| --- | --- |
| 200 | Список, скачивание или просмотр получены |
| 202 | Сообщение принято Kafka; сохранение ещё может выполняться |
| 400 | Некорректное имя или параметры запроса |
| 404 | Файл ещё не сохранён или не существует |
| 413 | Превышен лимит загрузки |
| 503 | Ошибка обращения к хранилищу или ожидания отправки в Kafka |

При таймауте сообщение могло попасть в Kafka: проверьте сохранённые файлы.

## Ограничения

- Максимальный размер — 900 KiB (921 600 байт); пустые файлы допускаются.
- Имя должно оканчиваться на `.txt`, иметь не более 200 символов, без `/`, `\` и управляющих символов.
- Проверяется расширение; содержимое и кодировка не проверяются.
- Повторная загрузка с тем же именем заменяет объект после обработки Consumer. До замены GET может вернуть предыдущую версию.
- Consumer подтверждает offset после успешной записи S3. При ошибке повторяет попытки без пропуска сообщения, задерживая последующие файлы.
- Доставка at-least-once: повторная обработка записывает те же байты в тот же ключ. Одна партиция сохраняет порядок; репликация 1 рассчитана на учебный запуск.
- Аутентификации REST API нет. S3-ключи `demo` / `demo-secret` в `s3.json` и `application.yml` — публичные учебные значения. Конфигурация предназначена для локального запуска.

## Структура проекта

| Файл / каталог | Назначение |
| --- | --- |
| `src/main/java/com/example/txt/TxtApplication.java` | Точка входа Spring Boot |
| `src/main/java/com/example/txt/FileController.java` | REST API и Producer |
| `src/main/java/com/example/txt/FileConsumer.java` | Обработка сообщений Kafka |
| `src/main/java/com/example/txt/FileStorage.java` | Создание бакета, запись, чтение и список объектов S3 |
| `src/main/java/com/example/txt/PipelineConfig.java` | Kafka topic, повторы и S3-клиент |
| `src/main/java/com/example/txt/ApiErrors.java` | Обработка ошибок API |
| `src/main/resources/application.yml` | Настройки приложения |
| `src/test/java/com/example/txt/` | Тесты передачи байтов и S3-запроса |
| `compose.yaml` | Kafka, SeaweedFS, API и Consumer |
| `Dockerfile` | Сборка с тестами и запуск на Java 21 |
| `s3.json` | Учебные учётные данные SeaweedFS |
| `pom.xml` | Maven-зависимости |
| `example.txt` | Пример исходного файла |

S3-клиент отключает потоковое обрамление `aws-chunked` и необязательные контрольные суммы запроса для совместимости с SeaweedFS 3.85. Это предотвращает сохранение служебных данных подписи в содержимом объекта.

## Сборка и тесты

При установленном JDK 21 и Maven:

```powershell
mvn verify
```

Или через Docker без локального JDK:

```powershell
docker compose build
```

Dockerfile выполняет `mvn verify`. Три теста проверяют передачу байтов с BOM, CRLF и не-ASCII между компонентами с mock-зависимостями, отклонение некорректных имён и S3 HTTP-запрос без изменения тела. Полную цепочку проверяйте загрузкой, скачиванием и сравнением SHA-256 выше.

## Остановка и устранение проблем

`docker compose down` останавливает проект с сохранением данных. **`docker compose down -v` удаляет тома и все данные проекта.**

- `curl: (26)` или ошибка `Get-FileHash`: проверьте `Get-Location` и `Test-Path example.txt`; перейдите в каталог проекта или используйте абсолютный путь.
- Docker Engine недоступен: запустите Docker Desktop и дождитесь готовности.
- Порт 8080 занят: освободите его или измените левую часть `8080:8080` в Compose и используйте новый порт в URL.
- Файл не появился после загрузки: проверьте `docker compose logs --tail=100 consumer seaweedfs`.
- После изменения кода обновите контейнеры: `docker compose up -d --build`.
