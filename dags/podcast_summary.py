from airflow import DAG
from datetime import datetime
import pendulum
import xmltodict
import requests

@dag(
    dag_id='podcast_summary',
    schedule_interval='@daily',
    start_date=datetime(2023,2,1),
    catshup=False
)

def podcast_summary():

    @task()
    def get_episode():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = get_episode()

summary = podcast_summary()