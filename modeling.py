import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from joblib import dump, load # used for saving and loading sklearn objects
from scipy.sparse import save_npz, load_npz # used for saving and loading sparse matrices
from sklearn.decomposition import NMF, LatentDirichletAllocation


def model():
    clean_tweets = pd.read_csv('data/clean_tweet_data.csv')
    clean_tweets = clean_tweets.fillna("")

    # Sentiment Analysis
    sentiment_analysis_tweet_data = clean_tweets.copy(deep=True)
    sentiment_analysis_tweet_data.drop(
        sentiment_analysis_tweet_data[sentiment_analysis_tweet_data['sentiment'] == -1].index, inplace=True)
    sentiment_analysis_tweet_data.reset_index(drop=True, inplace=True)
    tweet_train = sentiment_analysis_tweet_data.iloc[:4492, ]
    tweet_test = sentiment_analysis_tweet_data.iloc[4493:, ]

    unigram_vectorizer = CountVectorizer(ngram_range=(1, 1))
    unigram_vectorizer.fit(tweet_train['clean_text'].values)


if __name__ == '__main__':
    model()
