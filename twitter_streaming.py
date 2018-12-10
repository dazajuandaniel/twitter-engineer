import json
import time
import tweepy
from tweepy.streaming import StreamListener
import config
import logging
import boto3

#Logging
logger = config.setup_custom_logger(config.SQS_QUEUE_NAME)

auth = tweepy.OAuthHandler(config.CONSUMER_KEY_SAI, config.CONSUMER_SECRET_SAI)
auth.set_access_token(config.ACCESS_TOKEN_SAI, config.ACCESS_SECRET_SAI)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
count = 0
class TwitterStream(StreamListener):
    """Listens to Streaming Data"""

    def __init__(self):
        sqs = boto3.resource('sqs')
        self.queue = sqs.get_queue_by_name(QueueName=config.SQS_QUEUE_NAME)

    def on_data(self, data):
        """ Passes data to Status """
        if 'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            logger.warning(warning['message'])
            return False

    def on_status(self, status):
        """ Based on Status """
        try:
            #tweet = json.loads(status)
            # Send to SQS Queue
            #response = self.queue.send_message(MessageBody=status)
            logger.info(count+1)
            #print (tweet)
        except Exception as e:
            logger.error(e)

    def on_error(self, status):
        """Called when a non-200 status code is returned"""
        if status == 420:
            self.on_timeout()

    def on_timeout(self):
        """ Limit Timeout """
        logger.warning("API Reach its limit, sleep for 10 minutes")
        time.sleep(600)
        return

if __name__ == '__main__':
    listen = TwitterStream()
    stream = tweepy.Stream(auth, listen)
    move = True
    while move:
        try:
            logger.info('Start Loop')
            stream.filter(locations=[141.157913,-38.022041,146.255569,-36.412349])
            move = False
            logger.info('Finish Loop')
        except Exception as e:
            logger.error('Error found')
            logger.error(e)
            loop = True
            stream.disconnect()
            time.sleep(600)
            continue

