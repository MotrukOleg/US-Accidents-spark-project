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

**Пам'ять.** Ноутбуки підготовки даних запускають Spark з `spark.driver.memory=16g`, тож Docker має бути доступно щонайменше ~20 ГБ RAM. У Docker Desktop на Windows (WSL2) ліміт задається в `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=24GB
```

після чого — `wsl --shutdown` і перезапуск Docker Desktop.
