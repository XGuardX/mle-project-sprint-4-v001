# Подготовка виртуальной машины

## Склонируйте репозиторий

Склонируйте репозиторий проекта:

```
git clone https://github.com/yandex-praktikum/mle-project-sprint-4-v001.git
```

## Активируйте виртуальное окружение

Используйте то же самое виртуальное окружение, что и созданное для работы с уроками. Если его не существует, то его следует создать.

Создать новое виртуальное окружение можно командой:

```
python3 -m venv env_recsys_start
```

После его инициализации следующей командой

```
. env_recsys_start/bin/activate
```

установите в него необходимые Python-пакеты следующей командой

```
pip install -r requirements.txt
```

### Скачайте файлы с данными

Для начала работы понадобится три файла с данными:
- [tracks.parquet](https://storage.yandexcloud.net/mle-data/ym/tracks.parquet)
- [catalog_names.parquet](https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet)
- [interactions.parquet](https://storage.yandexcloud.net/mle-data/ym/interactions.parquet)
 
Скачайте их в директорию локального репозитория. Для удобства вы можете воспользоваться командой wget:

```
wget https://storage.yandexcloud.net/mle-data/ym/tracks.parquet

wget https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet

wget https://storage.yandexcloud.net/mle-data/ym/interactions.parquet
```

## Запустите Jupyter Lab

Запустите Jupyter Lab в командной строке

```
jupyter lab --ip=0.0.0.0 --no-browser
```

# Расчёт рекомендаций

Код для выполнения первой части проекта находится в файле `recommendations.ipynb`. Изначально, это шаблон. Используйте его для выполнения первой части проекта.

# Сервис рекомендаций

Код сервиса рекомендаций находится в файле `recommendations_service.py`.

## Запуск сервиса рекомендаций

1. Установите необходимые зависимости:
   ```
   pip install fastapi uvicorn pandas pyarrow requests
   ```

2. Убедитесь, что файлы с данными находятся в директории:
   - `recsys/recommendations/recommendations.parquet`
   - `recsys/recommendations/top_popular.parquet`
   - `recsys/recommendations/similar.parquet`
   - `recsys/recommendations/als_recommendations.parquet`

3. Запустите необходимые микросервисы:
   ```
   # Сервис похожих элементов
   uvicorn features_service:app --host 127.0.0.1 --port 8010

   # Сервис истории
   uvicorn history_service:app --host 127.0.0.1 --port 8020

   # Основной сервис рекомендаций
   uvicorn recommendations_service:app --host 127.0.0.1 --port 8000
   ```

## Тестирование сервиса

Код для тестирования сервиса находится в файле `test_service.py`.

1. Запустите все три сервиса (см. инструкции выше)

2. Запустите тесты:
   ```
   python test_service.py
   ```

3. Результаты тестирования сохраняются в файле `test_service.log`

Тесты проверяют три сценария:
- Пользователь без персональных рекомендаций (ID: 1)
- Пользователь с персональными рекомендациями (ID: 2)
- Пользователь с персональными рекомендациями и онлайн-историей (ID: 3)

## Стратегия смешивания рекомендаций
В нашем сервисе мы используем следующую стратегию для смешивания онлайн и оффлайн рекомендаций:

Чередование: Мы чередуем элементы из офлайн и онлайн рекомендаций (берём по одному элементу из каждого источника).
Добавление оставшихся: После чередования добавляем оставшиеся рекомендации из обоих источников.
Удаление дубликатов: Удаляем повторяющиеся ID, сохраняя порядок первого появления.
Ограничение количества: Возвращаем только запрошенное количество (k) рекомендаций.

Эта стратегия гарантирует, что пользователь получит разнообразные рекомендации, учитывающие как его долгосрочные предпочтения (офлайн), так и недавнюю активность (онлайн).
