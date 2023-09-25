#pip install telegram
#pip install python-telegram-bot
import datetime as dt
from datetime import datetime, timedelta, date
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from io import StringIO
import os
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#Соединение с Clickhouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20230820',
    'user':'student',
    'password':'dpo_python_2020'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'ayurina',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 15),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'


# функция для DAG'а    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_ayurina_report():
    
    # функция для извлечения данных
    @task()
    def extract_text():
        # SQL-запрос для извлечения данных для сообщения
        query_text = f"""
            SELECT 
                toDate(time) as Date,
                count(distinct user_id) AS DAU,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                countIf(action = 'like') / countIf(action = 'view') AS CTR
            FROM 
                simulator_20230820.feed_actions 
            WHERE 
                toDate(time) = today() - 1
            GROUP BY 
                Date
            """
        df_text = ph.read_clickhouse(query_text, connection=connection)
        return df_text
    
    @task()
    def extract_chart():
        # SQL-запрос для графика (метрики за последние 7 дней)
        query_chart = f"""
            SELECT 
                toDate(time) as Date,
                count (distinct user_id) AS DAU,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                countIf(action = 'like') / countIf(action = 'view') AS CTR
            FROM 
                simulator_20230820.feed_actions 
            WHERE 
                toDate(time) >= today() - 7 and toDate(time) < today()
            GROUP BY 
                Date
            """
        df_chart = ph.read_clickhouse(query_chart, connection=connection)
        return df_chart
        
    @task()   
    def transform_text(df_text):
        # Вычисляем дату предыдущего дня
        yesterday = datetime.today() - timedelta(days=1)
        yesterday_str = yesterday.strftime('%d-%m-%Y')
         # Создаем текстовый отчет
        report = f'*📑 Отчет по ленте новостей за {yesterday_str}:*\n'
        report += f'CTR: {df_text["CTR"].values[0]:.2%}\n'
        report += f'DAU: {df_text["DAU"].values[0]}\n'
        report += f'Просмотры: {df_text["views"].values[0]}\n'
        report += f'Лайки: {df_text["likes"].values[0]}\n'
        return report
    
    @task()
    def transform_chart(df_chart):
        #Установка стиля и размера фигуры
        sns.set(rc={'figure.figsize':(20,12)})
        plt.style.use("bmh")
        # Создаем сетку графиков
        fig, axes = plt.subplots(nrows=2, ncols=2)
    
        # График DAU
        sns.lineplot(data=df_chart, x='Date', y='DAU', ax=axes[0, 0], linewidth=2, linestyle='-')
        axes[0, 0].set_title('График DAU')
        # График CTR
        sns.lineplot(data=df_chart, x='Date', y='CTR', ax=axes[1, 1], linewidth=2, linestyle='-')
        axes[1, 1].set_title('График CTR')
        # График Просмотров
        sns.lineplot(data=df_chart, x='Date', y='views', ax=axes[0, 1], linewidth=2, linestyle='-')
        axes[0, 1].set_title('График Просмотров')
        # График Лайков
        sns.lineplot(data=df_chart, x='Date', y='likes', ax=axes[1, 0], linewidth=2, linestyle='-')
        axes[1, 0].set_title('График Лайков')
        # Общий заголовок для сетки
        plt.suptitle('Динамика метрик за последние 7 дней', fontsize=20, fontweight='bold')
        plt.tight_layout()  # Для избегания наложения меток
        # Сохраняем
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics_chart.png'
        plt.close()
        return plot_object
        
    @task
    def load(report, plot_object):
        chat_id = "скрыт"
        bot = telegram.Bot(token="bot_id скрыт")
    
        bot.sendMessage(chat_id=chat_id, text=report, parse_mode='Markdown')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df_text = extract_text()
    df_chart = extract_chart()
    report = transform_text(df_text)
    plot_object = transform_chart(df_chart)
    load(report, plot_object)
        

dag_ayurina_report = dag_ayurina_report()
