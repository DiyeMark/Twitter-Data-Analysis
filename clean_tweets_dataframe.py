import pandas as pd


class Clean_Tweets:
    """
    this class will clean the tweeter dataframe

    Return
    ------
    dataframe
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')

    def drop_unwanted_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        remove rows that have column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count'].index
        df.drop(unwanted_rows, inplace=True)
        df = df[df['polarity'] != 'polarity']

        return df

    def drop_duplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        drop duplicate rows
        """
        df = df.drop_duplicates(subset='original_text')

        return df

    def convert_to_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        convert column created_at to datetime
        """
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

        return df

    def convert_to_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        convert columns polarity, subjectivity, favorite_count, retweet_count,
        followers_count and friends_count to numbers
        """
        df['polarity'] = pd.to_numeric(df['polarity'], errors='coerce')
        df['subjectivity'] = pd.to_numeric(df['subjectivity'], errors='coerce')
        df['favorite_count'] = pd.to_numeric(df['favorite_count'], errors='coerce')
        df['retweet_count'] = pd.to_numeric(df['retweet_count'], errors='coerce')
        df['followers_count'] = pd.to_numeric(df['followers_count'], errors='coerce')
        df['friends_count'] = pd.to_numeric(df['friends_count'], errors='coerce')

        return df

    def remove_non_english_tweets(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        remove non english tweets from lang
        """
        df = df.query("lang == 'en' ")

        return df


if __name__ == '__main__':
    tweet_df = pd.read_csv('data/processed_tweet_data.csv')
    tweet_cleaner = Clean_Tweets(tweet_df)

