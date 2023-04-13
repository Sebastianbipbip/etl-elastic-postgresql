# etl-elastic-postgres

## Описание

ETL сервис для выгрузки логов из ElasticSearch в Postgresql

## Установка

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Использование

```bash
python source/service.py
```

### Параметы запуска

Все параметры опциональны, если есть нужные переменнные окружения:

- `-s` `--stream` - нужный стрим для выгрузки. По умолчанию ELASTIC_STREAM в переменной окружения. Доступны:
  - `1c-graylog` - стрим 1c_tb_api в GrayLog
  - `graylog` - стрим nginx в GrayLog
  - `microservices` - стрим microservices в GrayLog
- `--date` - дата, с которой нужно начать выгружать данные в формате **%Y-%m-%d %H:%M:%S**
- `--loglevel` - уровень логирования
- `--elastic_url` - url для подключения к ElasticSearch. По умолчанию ELASTIC_URL в переменной окружения
- `--postgres_url` - url для подключения к Postgresql. По умолчанию POSTGRES_URL в переменной окружения. Формат: `postgresql: // [user[:password] @][host][:port][ / dbname][?param1 = value1 & ...]`

## Структура репозитория

### `source/lib/mapping.py`

Набор датаклассов с описанием параметров запроса в ElasticSearch и структурой полей таблиц в БД.

#### Параметры ElasticSearch

- `index` - alias нужного индекса, используется для поиска нужного индекса, так как имя индекса зависит от даты
- `uid` - уникальный идентификатор стрима, используется разбивки данных по стримам в GrayLog
- `query` - запрос в ElasticSearch. Описывается только шаблон, далее может быть перезаписан динамически в скрипте. Например, изменить сортировку или количество запрашиваемых данных за раз

#### Параметры Postgresql

- `table` - название таблицы в БД
- `mapping` - описывает запрос для создания таблицы в БД, будет создана, если её ещё не существует
- `fields` - генерирует список полей таблицы, используется для формирования запроса insert в БД

