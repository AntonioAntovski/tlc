from crontab import CronTab
from pipeline import Pipeline
from datetime import datetime


class CronFetcher:
    def __init__(self):
        self.cron = CronTab(user="antonio")
        self.pipeline = Pipeline()

    def create_cron_job(self):
        """
        Create a cron job for automatically fetching the new datasets.
        :return:
        """

        if not len(self.cron):
            cron_job = self.cron.new(
                command="/var/www/html/tlc/venv/bin/python /var/www/html/tlc/cron_fetcher.py"
            )
            cron_job.month.every(1)
            cron_job.enable()
            self.cron.write_to_user(user=True)

    def run(self):
        """
        Run the task of the cron job.
        :return:
        """

        self.pipeline.fetch(year=datetime.now().year, month=datetime.now().month - 1)


if __name__ == "__main__":
    cron_fetcher = CronFetcher()
    cron_fetcher.create_cron_job()
    cron_fetcher.run()
