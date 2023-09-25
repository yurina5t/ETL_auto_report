## Автоматизация отчетности приложения с помощью Airflow

### [1. Автоматизация отчетности по ленте новостей](https://github.com/yurina5t/ETL_auto_report/blob/main/etl_bot.py)

Автоматизируем базовую отчетность приложения по ленте новостей. Наладим автоматическую отправку аналитической сводки в телеграм каждое утро в 11:00.    
Необходимые шаги:   
1. Создаем телеграм-бота с помощью @BotFather  (chat_id получаем по ссылке https://api.telegram.org/bot<токен_вашего_бота>/getUpdates или методом bot.getUpdates())
2. Пишем скрипт для сборки отчета по ленте новостей с информацией о значениях ключевых метрик за предыдущий день и с значениями метрик за предыдущие 7 дней с ключевыми метриками: 
DAU, Просмотры, Лайки, CTR.
3. Автоматизируем отправку отчета с помощью Airflow. Код для сборки отчета размещаем в GitLab.

__DAG в Airflow__

<img width="1468" alt="image" src="https://github.com/yurina5t/ETL_auto_report/assets/93882842/a9bf1ede-1d08-4757-a1ca-b9d1f72cf067"> 
<img width="1470" alt="image" src="https://github.com/yurina5t/ETL_auto_report/assets/93882842/b68cd04e-a7c5-463f-8554-4b40b900f0c8">

__Отчетность в telegram-чате__

<img width="752" alt="Снимок экрана 2023-09-25 в 09 39 11" src="https://github.com/yurina5t/ETL_auto_report/assets/93882842/92ce256e-2ba0-48c1-88a5-6fd8cd46cfa9">

### [2. Автоматизация отчетности по всему приложению](https://github.com/yurina5t/ETL_auto_report/blob/main/bot_full_report.py)

1. Собираем отчет по работе всего приложения как единого целого. 
2. Продумываем, какие метрики необходимо отобразить в этом отчете? Как можно показать их динамику? Приложить к отчету графики или файлы, чтобы сделать его более наглядным и информативным. 
Отчет должен помогать отвечать бизнесу на вопросы о работе всего приложения совокупно. 
3. Автоматизируем отправку отчета с помощью Airflow.

__DAG в Airflow__

<img width="1511" alt="dag_ayurina_full_report" src="https://github.com/yurina5t/ETL_auto_report/assets/93882842/fd5c641c-ebbe-4efc-b129-69a9de1b0942">
<img width="1511" alt="dag_ayurina_full_report - Graph" src="https://github.com/yurina5t/ETL_auto_report/assets/93882842/c2f14284-12f5-41d2-8cb1-4ec025ea7587">

__Отчетность в telegram-чате__
<img width="730" alt="Снимок экрана 2023-09-25 в 09 43 59" src="https://github.com/yurina5t/ETL_auto_report/assets/93882842/8b9259ec-8f16-4da6-9df7-2550095b0c42">
<img width="730" alt="Снимок экрана 2023-09-25 в 09 44 23" src="https://github.com/yurina5t/ETL_auto_report/assets/93882842/7a73df62-ed05-4991-9693-4388ae953c8a">
