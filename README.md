# Проект 3-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 3-го спринта.

Проектная работа по ETL и автоматизации подготовки данных.

Новые инкременты с информацией о продажах приходят по API.
Загружаем файлы в таблицы staging, трансформируем данные в таблицы фактов и измерений.

### Структура репозитория
1. Папка `migrations` хранит файлы миграции.
2. В папке `src` хранятся все необходимые исходники: 
    * Папка `dags` содержит DAG's Airflow.

### Как запустить контейнер
Запустите локально команду:

```
docker run -d --rm -p 3000:3000 -p 15432:5432 --name=de-project-sprint-3-server cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-3:latest
```

После того как запустится контейнер, у вас будут доступны:
1. Visual Studio Code
2. Airflow
3. Database
