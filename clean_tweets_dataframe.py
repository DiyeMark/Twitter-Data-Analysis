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

    def drop_null_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        convert original_text values to clean_text values
        """
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    def drop_unwanted_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        remove rows that have column names. This error originated from
        the data collection stage.
        """
        unwanted_rows = []
        for col in list(df.columns):
            unwanted_rows += df[df[col] == col].index

        df.drop(unwanted_rows, inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    def drop_duplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        drop duplicate rows
        """
        df = df.drop_duplicates(subset='original_text')
        df.reset_index(drop=True, inplace=True)

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
        # df['screen_count'] = pd.to_numeric(df['screen_count'], errors='coerce')
        df['followers_count'] = pd.to_numeric(df['followers_count'], errors='coerce')
        # df['friends_count'] = pd.to_numeric(df['friends_count'], errors='coerce')

        return df

    def remove_non_english_tweets(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        remove non english tweets from lang
        """
        df = df.query("lang == 'en' ")

        return df

    def polarity_category(self, p):
        """
        converst polarity to 3 group from floating value
        """
        if p > 0:
            return "positive"
        elif p < 0:
            return "negative"
        else:
            return "neutral"

    def subjectivity_category(self, p):
        """
        converst polarity to 3 group from floating value
        """
        if p > 0.75:
            return "very subjective"
        elif p > 5:
            return "subjective"
        elif p > .25:
            return "objective"
        else:
            return "very objective"


if __name__ == '__main__':
    tweet_df = pd.read_csv('data/processed_tweet_data.csv')
    tweet_cleaner = Clean_Tweets(tweet_df)
    # clean_df = tweet_cleaner.drop_null_rows(tweet_df)
    clean_df = tweet_cleaner.remove_non_english_tweets(tweet_df)
    clean_df = tweet_cleaner.drop_duplicate(clean_df)
    clean_df = tweet_cleaner.convert_to_numbers(clean_df)
    clean_df = tweet_cleaner.convert_to_datetime(clean_df)
    clean_df = tweet_cleaner.drop_unwanted_column(clean_df)
    clean_df.to_csv('data/clean_tweet_data.csv')
    print('File Successfully Saved.!!!')
