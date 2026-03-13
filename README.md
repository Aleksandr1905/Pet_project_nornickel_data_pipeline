# Норникель: Анализ зависимости акции от цен металлов

## Описание
Проект собирает данные по акции GMKN (T-Invest API) и ценам металлов (Yahoo Finance), 
считает корзину и визуализирует отклонения в Grafana.

## Стек
- Apache Airflow
- PostgreSQL
- Grafana
- Python

## Структура
- `dags/` — DAG для Airflow
- `sql/` — SQL-запросы и вью
- `grafana/` — экспорт дашборда

## Запуск
1. Скопировать `.env.example` в `.env` и заполнить токены
2. `docker-compose up -d`
3. Включить DAG в Airflow
4. Импортировать дашборд в Grafana
