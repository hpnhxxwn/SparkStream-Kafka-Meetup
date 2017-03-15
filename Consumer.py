from kafka import KafkaConsumer
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')

#from django.conf import settings

project_home = "/Users/hpnhxxwn/Desktop/proj/DE/hw7/SparkStream-Kafka-Meetup"
if project_home not in sys.path:
    sys.path.append(project_home)


# set environment variable to tell django where your settings.py is
#os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
#SECRET_KEY=""
# serve django via WSGI
#from django.core.wsgi import get_wsgi_application
#application = get_wsgi_application()

#settings.configure(default_settings=settings, DEBUG=True)
#django.setup()

# Now this script or any imported module can use any part of Django it needs.
#from myapp import models

consumer = KafkaConsumer("meetup", bootstrap_servers = ['localhost:9092'])

"""
for message in consumer:
    print message.value
"""
# Custom Code Imports
#from RsvpStream import Rsvpstream # Cassandra Model
from utils import get_dict_val, load_json, datetime_from_epoch # util functions


# Managing schemas
from cqlengine.management import sync_table, create_keyspace

# Setup connection
from cqlengine import connection
# Connect to the meetup keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "meetup")
# create keyspace
# keyspace name, keyspace replication strategy, replication factor
create_keyspace('meetup', 'SimpleStrategy', 1)
# Sync your model with your cql table
"""
sync_table(Rsvpstream)
"""

def write_to_cassandra(**kwargs):
    """
    Write the data to cassandra.
    """
    try:
        Rsvpstream.create(
            venue_name = get_dict_val(kwargs, 'venue_name'),
            venue_lon = get_dict_val(kwargs, 'venue_lon'),
            venue_lat = get_dict_val(kwargs, 'venue_lat'),
            venue_id = get_dict_val(kwargs, 'venue_id'),
            visibility = get_dict_val(kwargs, 'visibility'),
            response = get_dict_val(kwargs, 'response'),
            guests = get_dict_val(kwargs, 'guests'),
            member_id = get_dict_val(kwargs, 'member_id'),
            member_name = get_dict_val(kwargs, 'member_name'),
            rsvp_id = get_dict_val(kwargs, 'rsvp_id'),
            rsvp_last_modified_time  = get_dict_val(kwargs, 'rsvp_last_modified_time'),
            event_name = get_dict_val(kwargs, 'event_name'),
            event_time = get_dict_val(kwargs, 'event_time'),
            event_url = get_dict_val(kwargs, 'event_url'),
            group_topic_names = get_dict_val(kwargs, 'group_topic_names'),
            group_country = get_dict_val(kwargs, 'group_country'),
            group_state = get_dict_val(kwargs, 'group_state'),
            group_city = get_dict_val(kwargs, 'group_city'),
            group_name = get_dict_val(kwargs, 'group_name'),
            group_lon = get_dict_val(kwargs, 'group_lon'),
            group_lat = get_dict_val(kwargs, 'group_lat'),
            group_id = get_dict_val(kwargs, 'group_id')
        )
    except Exception, e:
        print e

def fetch_data():
    """
    If data exists, load data using json.
    Get values out of json dict.
    Call write_to_cassandra to write the values to Cassandra.
    """
    if consumer:
        for message in consumer:
            message = load_json(message.value) #Convert to json
            venue = get_dict_val(message, 'venue')
            if venue:
                venue_name = get_dict_val(venue, 'venue_name')
                venue_lon = get_dict_val(venue, 'lon')
                venue_lat = get_dict_val(venue, 'lat')
                venue_id = get_dict_val(venue, 'venue_id')
            else:
                venue_name, venue_lon, venue_lat, venue_id = None, None, None, None
            visibility = get_dict_val(message, 'visibility')
            response = get_dict_val(message, 'response')
            guests = get_dict_val(message, 'guests')
            # Member who RSVP'd
            member = get_dict_val(message, 'member')
            if member:
                member_id = get_dict_val(member, 'member_id')
                member_name = get_dict_val(member, 'member_name')
            else:
                member_id, member_name = '', ''
            rsvp_id = get_dict_val(message, 'rsvp_id')
            #since epoch
            mtime = get_dict_val(message, 'mtime')
            if mtime:
                rsvp_last_modified_time = datetime_from_epoch(mtime)
            else:
                rsvp_last_modified_time = None
            # Event for the RSVP
            event = get_dict_val(message, 'event')
            if event:
                event_name = get_dict_val(event, 'event_name')
                time = get_dict_val(event, 'time')
                if time:
                    event_time = datetime_from_epoch(time)
                else:
                    event_time = None
                event_url = get_dict_val(event, 'event_url')
            else:
                event_name, event_id, event_time, event_url = '', '', '', ''
            # Group hosting the event
            group = get_dict_val(message, 'group')
            if group:
                group_topics = get_dict_val(group, 'group_topics')
                if group_topics:
                    group_topic_names = ','.join(
                        [get_dict_val(each_group_topic, 'topic_name')
                         for each_group_topic in group_topics]
                    )
                else:
                    group_topic_names = ''
                group_city = get_dict_val(group, 'group_city')
                group_country = get_dict_val(group, 'group_country')
                group_id = get_dict_val(group, 'group_id')
                group_name = get_dict_val(group, 'group_name')
                group_lon = get_dict_val(group, 'group_lon')
                group_state = get_dict_val(group, 'group_state')
                group_lat = get_dict_val(group, 'group_lat')
            else:
                group_topic_names, group_city, group_country, group_id, \
                group_name, group_lon, group_state, group_lat = \
                    '', '', '', '', '', '', '', ''

            # Write data to Cassandra database
            write_to_cassandra(venue_name = venue_name, venue_lon = venue_lon, \
                               venue_lat = venue_lat, venue_id = venue_id, visibility = visibility, \
                               response = response, guests = guests, member_id = member_id, \
                               member_name = member_name, rsvp_id = rsvp_id, \
                               rsvp_last_modified_time  = rsvp_last_modified_time, \
                               event_name = event_name, event_time = event_time, event_url = event_url, \
                               group_topic_names = group_topic_names, group_country = group_country, \
                               group_state = group_state, group_city = group_city, group_name = group_name, \
                               group_lon = group_lon, group_lat = group_lat, group_id = group_id
                               )

            result = u' '.join((str(group_topic_names), str(group_city), str(group_country), str(group_id), str(group_name), str(group_lon), str(group_state), str(group_lat))).encode('utf-8').strip()
            print(result)
            #print(group_topic_names + " " + group_city + " " + group_country + " " + group_id + " " + group_name + " " + group_lon + " " + group_state + " " + group_lat)

if __name__ == '__main__':
    fetch_data()
    kafka.close()
