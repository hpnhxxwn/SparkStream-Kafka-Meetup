import pytz

timezone = 'US/Pacific-New'

INSTALLED_APPS = [
    'django.contrib.admin',
    'SparkStream-Kafka-Meetup.Consumer',
    'stream.apps.StreamConfig'  # <-- this should be added
]

SECRET_KEY = ""