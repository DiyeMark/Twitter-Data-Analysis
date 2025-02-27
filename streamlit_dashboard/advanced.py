import sys
import os
from streamlit_helper import *
import plotly.express as px

sys.path.append(os.path.abspath(os.path.join('..')))
from clean_tweets_dataframe import *


def loadData():
    query = "select * from TweetInformation"
    df = db_execute_fetch(query, db_name="tweets", rdf=True)
    return df


def app():
    st.title('Advanced Visualization')
    tweet_df = pd.read_csv('../data/processed_tweet_data.csv')
    ct = Clean_Tweets(tweet_df)
    st.write(
        """
        7. **Parallel Categories Diagram** \n
          Finally here I have created parallel categories diagram.
        """)
    df = loadData()
    df["score"] = df["polarity"].apply(ct.polarity_category)
    df["subjectivity_score"] = df["subjectivity"].apply(
        ct.subjectivity_category)
    df = df.groupby(
        ['place', 'score', "subjectivity_score", 'subjectivity', "favorite_count", "followers_count",
         "retweet_count"]
    ).size().reset_index(name='counts')

    x = df.nlargest(25, "counts")
    fig = px.parallel_categories(x, dimensions=['place', "retweet_count", 'score', 'subjectivity_score'],
                                 color="subjectivity", color_continuous_scale=px.colors.sequential.Inferno)
    fig.update_layout(width=900, height=600)
    st.plotly_chart(fig)
