#!/usr/bin/env python

import time
import tweepy
import json


class StreamWatcherListener(tweepy.StreamListener):

    def on_status(self, status):
        try:
            print("%s" % (json.dumps(status._json, ensure_ascii=False)))
        except:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass

    def on_error(self, status_code):
        print('An error has occured! Status code = %s' % status_code)
        return True  # keep stream alive

    def on_timeout(self):
        print('Snoozing Zzzzzz')


def main():
    # Prompt for login credentials and setup stream object
    consumer_key = 'njRmQbFJ64SQOTZR0dw'
    consumer_secret = 'A3N2t1DN3t3ZGrY6Tg645wA7LCnuUIOsXi449aThpY'
    access_token = '227720254-c0UNGcEp03fzNwed55PRHDdkznbyD2BzIuxlv5X7'
    access_token_secret = 'tsKi5WJy7GdZJDuhUrOWYMGYBMRCpiHHHJN8pXeoXEE'

    auth = tweepy.auth.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = tweepy.Stream(auth, StreamWatcherListener(), timeout=None)

    stream.filter([], ['glutenfree', 'celiac', 'gluten', 'paleo'])

    # Prompt for mode of streaming
    # valid_modes = ['sample', 'filter']
    # while True:
    #     mode = raw_input('Mode? [sample/filter] ')
    #     if mode in valid_modes:
    #         break
    #     print 'Invalid mode! Try again.'

    # if mode == 'sample':
    #     stream.sample()

    # elif mode == 'filter':
    #     follow_list = raw_input('Users to follow (comma separated): ').strip()
    #     track_list = raw_input('Keywords to track (comma seperated): ').strip()
    #     if follow_list:
    #         follow_list = [u for u in follow_list.split(',')]
    #         userid_list = []
    #         username_list = []
            
    #         for user in follow_list:
    #             if user.isdigit():
    #                 userid_list.append(user)
    #             else:
    #                 username_list.append(user)
            
    #         for username in username_list:
    #             user = tweepy.API().get_user(username)
    #             userid_list.append(user.id)
            
    #         follow_list = userid_list
    #     else:
    #         follow_list = None
    #     if track_list:
    #         track_list = [k for k in track_list.split(',')]
    #     else:
    #         track_list = None
    #     print follow_list
    #     stream.filter(follow_list, track_list)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nGoodbye!')
