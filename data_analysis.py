import inspect
import os
import sys
import pandas as pd
import string


def analyse_data():
    processed_tweets = pd.read_csv('data/clean_tweet_data.csv')

    print("The number of missing value(s) based on columns:\n{}".format(
        processed_tweets.isnull().sum()))
    print("The number of missing value(s): {}".format(
        processed_tweets.isnull().sum().sum()))
    print("Columns having missing value(s):{}".format(
        processed_tweets.columns[processed_tweets.isnull().any()]))

    currentdir = os.path.dirname(os.path.abspath(
        inspect.getfile(inspect.currentframe())))
    parentdir = os.path.dirname(currentdir)
    sys.path.insert(0, parentdir)

    tweets_df = pd.DataFrame(
        columns=['original_text', 'clean_text', 'sentiment', 'lang', 'hashtags'])

    tweets_df['original_text'] = processed_tweets['original_text'].to_list()
    tweets_df['clean_text'] = processed_tweets['clean_text'].to_list()
    tweets_df['sentiment'] = processed_tweets['sentiment'].to_list()
    tweets_df['lang'] = processed_tweets['lang'].to_list()
    tweets_df['hashtags'] = processed_tweets['hashtags'].to_list()
    tweets_df = tweets_df.fillna("")

    tweets_df['hashtags'] = tweets_df['hashtags'].astype(str)
    tweets_df['hashtags'] = tweets_df['hashtags'].apply(lambda x: x.lower())
    tweets_df['hashtags'] = tweets_df['hashtags'].apply(
        lambda x: x.translate(str.maketrans(' ', ' ', string.punctuation)))

    flattened_hashtags_df = pd.DataFrame(
        [hashtag for hashtags_list in tweets_df.hashtags
         for hashtag in hashtags_list.split(',')],
        columns=['hashtag'])

    flattened_hashtags_df.drop(
        flattened_hashtags_df[flattened_hashtags_df['hashtag'] == ""].index, inplace=True)
    flattened_hashtags_df.reset_index(drop=True, inplace=True)

    tweets_df = tweets_df.drop(['original_text'], axis=1)

    tweets_df.to_csv('./data/model_ready_data.csv', index=False)
    print('Model Ready Data Successfully Saved.!!!')


if __name__ == '__main__':
    analyse_data()
