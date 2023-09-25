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
    'database': 'simulator_20230820',
    'user': 'student',
    'password': 'dpo_python_2020'
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

#эмодзи графиков
up = "📈"
down = "📉"
equal = "↔️"

# функция для DAG'а    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_ayurina_full_report():
    # функция для извлечения данных
    @task()
    def extract_feed_action():
        # SQL-запрос для извлечения данных 
        query_feed = f"""
            SELECT 
                toDate(time) as Date,
                count(distinct user_id) AS DAU,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                countIf(action = 'like') / countIf(action = 'view') AS CTR
            FROM 
                simulator_20230820.feed_actions 
            WHERE 
                toDate(time) >= today() - 14 and toDate(time) < today()
            GROUP BY Date
            order by Date
            """
        df_feed = ph.read_clickhouse(query_feed, connection=connection)
        return df_feed
    
    @task()
    def extract_feed_chart():
        # SQL-запрос для графика по разрезам
        query_chart = """
            SELECT
                toDate(time) AS Date,
                count(user_id) as users,
                countIf(os = 'Android') AS Android,
                countIf(os = 'iOS') AS iOS,
                countIf(source = 'organic') AS organic,
                countIf(source = 'ads') AS ads
            FROM
                simulator_20230820.feed_actions
            WHERE
                toDate(time) BETWEEN today() - 14 AND yesterday()
            GROUP BY Date
            ORDER BY Date
        """
        df_chart = ph.read_clickhouse(query_chart, connection=connection)
        return df_chart
    
    @task()
    def extract_message():
        # SQL-запрос для графика (метрики за последние 7 дней)
        query_mes = f"""
            SELECT 
                toDate(time) as Date,
                count (distinct user_id) AS DAU,
                count(user_id) AS messages
            FROM 
                simulator_20230820.message_actions 
            WHERE 
                toDate(time) >= today() - 14 and toDate(time) < today()
            GROUP BY Date
            order by Date
            """
        df_messages = ph.read_clickhouse(query_mes, connection=connection)
        return df_messages
    
    @task()
    def transform_data(df):
        # Преобразование данных
        if 'DAU' in df.columns:
            df['DAU'] = df['DAU'] / 1000  # Преобразование DAU в тысячи
        if 'likes' in df.columns:
            df['likes'] = df['likes'] / 1000  # Преобразование likes в тысячи
        if 'views' in df.columns:
            df['views'] = df['views'] / 1000  # Преобразование views в тысячи
        if 'likes' in df.columns and 'views' in df.columns:
            df['CTR'] = df['likes'] / df['views']  # Рассчет CTR
        if 'messages' in df.columns:
            df['messages'] = df['messages'] / 1000  # Преобразование likes в тысячи
        return df
    
    @task()
    def transform_text(df_feed, df_messages):
        # Вычисляем дату за вчера
        yesterday = datetime.today() - timedelta(days=1)
        yesterday_str = yesterday.strftime('%d-%m-%Y')
        # Рассчитаем метрики для разных периодов времени
        # Получение данных за вчера
        yesterday_feed = df_feed.iloc[-1]
        yesterday_messages = df_messages.iloc[-1]
        # Получение данных за позавчера
        revious_day_feed = df_feed.iloc[-2]
        revious_day_messages = df_messages.iloc[-2]
        # Получение данных за день неделю назад
        week_ago_feed = df_feed.iloc[-8]
        week_ago_messages = df_messages.iloc[-8]
        # Получение данных за 1 и 2 недели назад
        last_week_feed = df_feed.iloc[-7:]
        last_two_weeks_feed = df_feed.iloc[-14:]
        last_week_messages = df_messages.iloc[-7:]
        last_two_weeks_messages = df_messages.iloc[-14:]
        # Рассчет динамики за день и неделю
        def calculate_change(current, previous):
            return ((current - previous) / previous) * 100
        DAU_change_feed_day = calculate_change(yesterday_feed['DAU'], revious_day_feed['DAU'])
        views_change_feed_day = calculate_change(yesterday_feed['views'], revious_day_feed['views'])
        likes_change_feed_day = calculate_change(yesterday_feed['likes'], revious_day_feed['likes'])
        CTR_change_feed_day = calculate_change(yesterday_feed['CTR'], revious_day_feed['CTR'])

        DAU_change_feed_week = calculate_change(yesterday_feed['DAU'], week_ago_feed['DAU'])
        views_change_feed_week = calculate_change(yesterday_feed['views'], week_ago_feed['views'])
        likes_change_feed_week = calculate_change(yesterday_feed['likes'], week_ago_feed['likes'])
        CTR_change_feed_week = calculate_change(yesterday_feed['CTR'], week_ago_feed['CTR'])

        DAU_change_messages_day = calculate_change(yesterday_messages['DAU'], revious_day_messages['DAU'])
        DAU_change_messages_week = calculate_change(yesterday_messages['DAU'], week_ago_messages['DAU'])
        messages_change_messages_day = calculate_change(yesterday_messages['messages'], revious_day_messages['messages'])
        messages_change_messages_week = calculate_change(yesterday_messages['messages'], week_ago_messages['messages'])
        
        # Создание отчета
        report = f" 📑 Отчет по метрикам на {yesterday_str}:\n"
        report += "===========================\n"
        report += "Метрики для Ленты новостей:\n"
        report += "___________________________\n"
        report += f"DAU за вчера: {yesterday_feed['DAU']*1000}\n"
        report += f"Изменение DAU д/д: {DAU_change_feed_day:.2f}% {down if DAU_change_feed_day < 0 else up if DAU_change_feed_day > 0 else equal}\n"
        report += f"Изменение DAU н/н: {DAU_change_feed_week:.2f}% {down if DAU_change_feed_week < 0 else up if DAU_change_feed_week > 0 else equal}\n"
        report += f"Ср. знач. DAU за неделю: {round(last_week_feed['DAU'].mean()*1000,0)}\n"
        report += f"Ср. знач. DAU за 2 недели: {round(last_two_weeks_feed['DAU'].mean()*1000,0)}\n"
        report += "___________________________\n"
        report += f"Просмотры за вчера: {round(yesterday_feed['views']*1000,0)}\n"
        report += f"Изменение просмотров д/д: {views_change_feed_day:.2f}% {down if views_change_feed_day < 0 else up if views_change_feed_day > 0 else equal}\n"
        report += f"Изменение просмотров н/н: {views_change_feed_week:.2f}% {down if views_change_feed_week < 0 else up if views_change_feed_week > 0 else equal}\n"
        report += f"Ср. знач. просмотров за неделю: {round(last_week_feed['views'].mean()*1000,0)}\n"
        report += f"Ср. знач. просмотров за 2 недели: {round(last_two_weeks_feed['views'].mean()*1000,0)}\n"
        report += "___________________________\n"
        report += f"Лайки за вчера: {round(yesterday_feed['likes']*1000,0)}\n"
        report += f"Изменение лайков д/д: {likes_change_feed_day:.2f}% {down if likes_change_feed_day < 0 else up if likes_change_feed_day > 0 else equal}\n"
        report += f"Изменение лайков н/н: {likes_change_feed_week:.2f}% {down if likes_change_feed_week < 0 else up if likes_change_feed_week > 0 else equal}\n"
        report += f"Ср. знач. лайков за неделю: {round(last_week_feed['likes'].mean()*1000,0)}\n"
        report += f"Ср. знач. лайков за 2 недели: {round(last_two_weeks_feed['likes'].mean()*1000,0)}\n"
        report += "___________________________\n"
        report += f"CTR за вчера: {yesterday_feed['CTR']:.3f}\n"
        report += f"Изменение CTR д/д: {CTR_change_feed_day:.2f}% {down if CTR_change_feed_day < 0 else up if CTR_change_feed_day > 0 else equal}\n"
        report += f"Изменение CTR н/н: {CTR_change_feed_week:.2f}% {down if CTR_change_feed_week < 0 else up if CTR_change_feed_week > 0 else equal}\n"
        report += f"Ср. знач. CTR за неделю: {round(last_week_feed['CTR'].mean(),3)}\n"
        report += f"Ср. знач. CTR за 2 недели: {round(last_two_weeks_feed['CTR'].mean(),3)}\n"
        report += "===========================\n\n"
        report += "Метрики для мессенджера:\n"
        report += "___________________________\n"
        report += f"DAU в мессенджере за вчера: {round(yesterday_messages['DAU']*1000,0)}\n"
        report += f"Изменение DAU д/д: {DAU_change_messages_day:.2f}% {down if DAU_change_messages_day < 0 else up if DAU_change_messages_day > 0 else equal}\n"
        report += f"Изменение DAU н/н: {DAU_change_messages_week:.2f}% {down if DAU_change_messages_week < 0 else up if DAU_change_messages_week > 0 else equal}\n"
        report += f"Ср. знач. DAU за неделю: {round(last_week_messages['DAU'].mean()*1000,0)}\n"
        report += f"Ср. знач. DAU за 2 недели: {round(last_two_weeks_messages['DAU'].mean()*1000,0)}\n"
        report += "___________________________\n"
        report += f"Отправленных сообщений за вчера: {round(yesterday_messages['messages']*1000,0)}\n"
        report += f"Изменение отправленных сообщений д/д: {messages_change_messages_day:.2f}% {down if messages_change_messages_day < 0 else up if messages_change_messages_day > 0 else equal}\n"
        report += f"Изменение отправленных сообщений н/н: {messages_change_messages_week:.2f}% {down if messages_change_messages_week < 0 else up if messages_change_messages_week > 0 else equal}\n"
        report += f"Ср. знач. отправленных сообщений за неделю: {round(last_week_messages['messages'].mean()*1000,0)}\n"
        report += f"Ср. знач. отправленных сообщений за 2 недели: {round(last_two_weeks_messages['messages'].mean()*1000,0)}\n"
        report += "===========================\n"
        return report
    
    @task()
    def transform_chart(df_chart, df_feed, df_messages):
        # Установка стиля и размера фигуры
        sns.set(rc={'figure.figsize': (24, 12)})
        plt.style.use("bmh")
        # Создаем сетку графиков
        fig, axes = plt.subplots(nrows=3, ncols=2)
        # График DAU в ленте новостей
        sns.lineplot(data=df_feed, x='Date', y='DAU', ax=axes[0, 0], linewidth=2, linestyle='-')
        axes[0, 0].set_title('График DAU в ленте новостей')
        # График DAU в мессенджере
        sns.lineplot(data=df_messages, x='Date', y='DAU', ax=axes[0, 1], linewidth=2, linestyle='-')
        axes[0, 1].set_title('График DAU в ленте мессенджера')
        # График CTR
        sns.lineplot(data=df_feed, x='Date', y='CTR', ax=axes[1, 0], linewidth=2, linestyle='-')
        axes[1, 0].set_title('График CTR')
        # График Количества сообщений
        sns.lineplot(data=df_messages, x='Date', y='messages', ax=axes[1, 1], linewidth=2, linestyle='-')
        axes[1, 1].set_title('График Количества сообщений')
        # График Пользователей по ОС
        sns.lineplot(data=df_chart, x='Date', y='Android', ax=axes[2, 0], label='Android', linewidth=2, linestyle='-')
        sns.lineplot(data=df_chart, x='Date', y='iOS', ax=axes[2, 0], label='iOS', linewidth=2, linestyle='-')
        axes[2, 0].set_title('График Пользователей по ОС')
        # График Привлеченного трафика
        sns.lineplot(data=df_chart, x='Date', y='organic', ax=axes[2, 1], label='Organic', linewidth=2, linestyle='-')
        sns.lineplot(data=df_chart, x='Date', y='ads', ax=axes[2, 1], label='Ads', linewidth=2, linestyle='-')
        axes[2, 1].set_title('График Привлеченного трафика')
        # Общий заголовок для сетки
        plt.suptitle('Динамика метрик за последние 7 дней', fontsize=20, fontweight='bold')
        plt.tight_layout()  # Для избегания наложения меток
        # Сохраняем
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'full_chart.png'
        plt.close()
        return plot_object
    
    @task()
    def load(report, plot_object):
        chat_id = "скрыт"
        bot = telegram.Bot(token="bot_id скрыт")

        bot.sendMessage(chat_id=chat_id, text=report, parse_mode='Markdown')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df_feed = extract_feed_action()
    df_messages = extract_message()
    df_chart = extract_feed_chart()
    df_feed = transform_data(df_feed)  # Применение преобразований к данным Ленты новостей
    df_messages = transform_data(df_messages)  # Применение преобразований к данным мессенджера
    report = transform_text(df_feed, df_messages)
    plot_object = transform_chart(df_chart, df_feed, df_messages)
    load(report, plot_object)

dag_ayurina_full_report = dag_ayurina_full_report()
