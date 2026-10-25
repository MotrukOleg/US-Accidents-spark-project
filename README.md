# branch-for-develop


## Запуск ноутбуків у Docker

Одне середовище для всіх ноутбуків і `main.py`: Python 3.11, Java 17, залежності з `requirements.txt` + Jupyter Lab.

1. Покладіть датасет у `data/US_Accidents_March23.csv` (тека `data/` монтується в контейнер, в образ не копіюється).
2. Зберіть образ і запустіть Jupyter:

   ```bash
   docker compose up --build
   ```

3. Відкрийте http://localhost:8888 — відкриється тека `notebooks/`. Spark UI: http://localhost:4040.

Порядок запуску: `*_data_preparation.ipynb` → `*_model.ipynb` (моделі читають `data/processed/<task>`, зберігають у `data/models`).

Пайплайн `main.py` у тому ж середовищі:

```bash
docker compose run --rm notebooks python main.py
```

**Пам'ять.** Локально ноутбуки підготовки даних запускають Spark з `spark.driver.memory=16g`; у Docker — з `8g` (змінна `SPARK_DRIVER_MEMORY` у `docker-compose.yml`, бо Docker Desktop зазвичай отримує лише половину RAM хоста). Інше значення: `SPARK_DRIVER_MEMORY=12g docker compose up`. Щоб дати Docker більше пам'яті, у Docker Desktop на Windows (WSL2) ліміт задається в `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=24GB
```

після чого — `wsl --shutdown` і перезапуск Docker Desktop.

**Пам'ять для ноутбуків моделей.** `*_model.ipynb` тримають увесь train у пам'яті (float64, ~9 ГБ), тож Docker потрібно ~14–16 ГБ RAM. На машині з меншою пам'яттю (наприклад, ноутбук на 16 ГБ) навчайте на частині train — змінна `TRAIN_FRACTION` (частка від 0 до 1, за замовчуванням `1`):

```bash
TRAIN_FRACTION=0.3 docker compose up
```

Повний запуск `*_model.ipynb` (підбір параметрів + 8 / 4 моделі на повному train) триває ~1–2 год на кожен ноутбук.

**Mac (Apple Silicon).** Образ збирається нативно під arm64 — збирайте його на самому маку (`docker compose up --build`), а не переносьте образ, зібраний на Windows (amd64 працюватиме через емуляцію в рази повільніше). Пам'ять для Docker задається в Docker Desktop → Settings → Resources.
