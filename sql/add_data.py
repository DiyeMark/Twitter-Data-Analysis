import os
import pandas as pd
import mysql.connector as mysql
from mysql.connector import Error


def db_connect(db_name=None):
    """
    Parameters
    ----------
    db_name :
        Default value = None)
    Returns
    -------
    """
    conn = mysql.connect(host='localhost',
                         user='twitter_user',
                         password='twitter_user_pass',
                         database=db_name,
                         buffered=True)

    cur = conn.cursor()
    return conn, cur


def emoji_db(db_name: str) -> None:
    conn, cur = db_connect(db_name)
    db_query = f"ALTER DATABASE {db_name} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;"
    cur.execute(db_query)
    conn.commit()


def create_db(db_name: str) -> None:
    """
    Parameters
    ----------
    db_name :
        str:
    db_name :
        str:
    db_name:str :
    Returns
    -------
    """
    conn, cur = db_connect()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    conn.commit()
    cur.close()


def create_tables(db_name: str) -> None:
    """
    Parameters
    ----------
    db_name :
        str:
    db_name :
        str:
    db_name:str :
    Returns
    -------
    """
    conn, cur = db_connect(db_name)
    sql_file = 'schema.sql'
    fd = open(sql_file, 'r')
    read_sql_file = fd.read()
    fd.close()

    sql_commands = read_sql_file.split(';')

    for command in sql_commands:
        try:
            res = cur.execute(command)
        except Exception as ex:
            print("Command skipped: ", command)
            print(ex)
    conn.commit()
    cur.close()

    return


def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parameters
    ----------
    df :
        pd.DataFrame:
    df :
        pd.DataFrame:
    df:pd.DataFrame :
    Returns
    -------
    """
    cols_2_drop = ['original_text']
    try:
        df = df.drop(columns=cols_2_drop, axis=1)
        df = df.fillna(0)
    except KeyError as e:
        print("Error:", e)

    return df


def insert_to_tweet_table(db_name: str, df: pd.DataFrame, table_name: str) -> None:
    """
    Parameters
    ----------
    db_name :
        str:
    df :
        pd.DataFrame:
    table_name :
        str:
    db_name :
        str:
    df :
        pd.DataFrame:
    table_name :
        str:
    db_name:str :
    df:pd.DataFrame :
    table_name:str :
    Returns
    -------
    """
    conn, cur = db_connect(db_name)

    df = preprocess_df(df)

    for _, row in df.iterrows():
        print(row)
        sql_query = f"""INSERT INTO {table_name} (created_at, status, source, clean_text, 
        polarity, subjectivity, lang, favorite_count, retweet_count, original_author, screen_count, followers_count, 
        friends_count, hashtags, user_mentions, place, place_coordinate)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        data = (row[0], row[1], row[2], row[3], (row[5]), (row[6]), row[7], row[8], row[9], row[10], row[11], row[12],
                row[13], row[15], row[16], row[17], row[18])

        try:
            # Execute the SQL command
            cur.execute(sql_query, data)

            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return


def db_execute_fetch(*args, many=False, table_name='', rdf=True, **kwargs) -> pd.DataFrame:
    """
    Parameters
    ----------
    *args :
    many :
         (Default value = False)
    table_name :
         (Default value = '')
    rdf :
         (Default value = True)
    **kwargs :
    Returns
    -------
    """
    connection, cursor1 = db_connect(**kwargs)
    if many:
        cursor1.executemany(*args)
    else:
        cursor1.execute(*args)

    # get column names
    field_names = [i[0] for i in cursor1.description]

    # get column values
    res = cursor1.fetchall()

    # get row count and show info
    nrow = cursor1.rowcount
    if table_name:
        print(f"{nrow} records fetched from {table_name} table")

    cursor1.close()
    connection.close()

    # return result
    if rdf:
        return pd.DataFrame(res, columns=field_names)
    else:
        return res


if __name__ == "__main__":
    create_db(db_name='tweets')
    emoji_db(db_name='tweets')
    create_tables(db_name='tweets')

    processed_tweet_df = pd.read_csv('../data/processed_tweet_data.csv')
    model_ready_tweet_df = pd.read_csv('../data/model_ready_data.csv')

    insert_to_tweet_table(db_name='tweets', df=processed_tweet_df,
                          table_name='TweetInformation')
