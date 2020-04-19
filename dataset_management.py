import os
import csv
from dateutil import parser
import re
import math
import ast
import numpy as np
from scipy import optimize
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import tufte


# Analyze users behaviour
# dataset_descriptive: Analyze users behaviour: tweets and retweets and tw and retweets per user per hour,
#                      per day of the week and per month


####################################################################################################################
# functions to understand the structure of the dataset


def most_frequent_tags(start_date, stop_date, path_folder_sorted_merged, path_folder_plots, count):
    """
    Function to save the most popular tags per hour and per day in 2 csv files
    :param start_date: (datetime) start date of the analysis
    :param stop_date: (datetime) stop date of the analysis
    :param path_folder_sorted_merged: (str) path of the folder with the dataset
    :param path_folder_plots: (str) path of the folder with the plots where to save the csv
    :param count: (int) n of most popular tags to select
    :return: 2 csv with the most popular tags per hour and per day
    """

    start_date_sec = int(datetime.timestamp(start_date))
    stop_date_sec = int(datetime.timestamp(stop_date))
    chunks_time_series_tot = select_chunk(start_date_sec, stop_date_sec, path_folder_sorted_merged)
    n_tot_hourly, n_tot_daily, tot_tweets = pd.Series(name='hourly'), pd.Series(name='daily'), pd.DataFrame()
    for n, chunk in enumerate(chunks_time_series_tot):
        print(n)
        _, tot_tweets_chunk = select_tags_keywords(None, None, chunk, True)
        tot_tweets = tot_tweets.append(tot_tweets_chunk, ignore_index=True)
        tot_tweets_chunk = None  # free memory

    index_date_h = pd.date_range(start_date, stop_date - timedelta(hours=1), freq='H')
    most_frequent_tags_hourly = pd.DataFrame(index=list(range(1, count+1)), columns=index_date_h.values)
    index_date_d = pd.date_range(start_date, stop_date, freq='D')
    most_frequent_tags_daily = pd.DataFrame(index=list(range(1, count+1)), columns=index_date_d.values)

    tot_tweets.hashtags = tot_tweets.hashtags.str.split(':', expand=True)  # split tags duplicate tweets: 1 for each tag

    for h in index_date_h.values:  # tags for each hour
        freq_tags = tot_tweets[(tot_tweets.date >= h) & (tot_tweets.date < h + np.timedelta64(1, 'h'))].\
            hashtags.value_counts().head(count)  # just the most frequent tags
        most_frequent_tags_hourly[h] = freq_tags.index.tolist()
    print(most_frequent_tags_hourly)

    for d in index_date_d.values:  # tags for each day
        print(str(d))
        print(str(d + np.timedelta64(1, 'D')))
        freq_tags = tot_tweets[(tot_tweets.date >= d) & (tot_tweets.date < d + np.timedelta64(1, 'D'))].\
            hashtags.value_counts().head(count)  # just the most frequent tags
        print(freq_tags)
        most_frequent_tags_daily[d] = freq_tags.index.tolist()
    print(most_frequent_tags_daily)

    # write time series to csv
    most_frequent_tags_hourly.to_csv(
        os.path.join(path_folder_plots,
                     str(start_date.date())+'_'+str(stop_date.date())+"_hourly_most_popular_tags.csv"))
    most_frequent_tags_daily.to_csv(
        os.path.join(path_folder_plots,
                     str(start_date.date())+'_'+str(stop_date.date())+"_daily_most_popular_tags.csv"))


def print_text_tweets(keywords, start_date, stop_date, path_folder_sorted_merged, hashtags):
    """
    Function to print the matching tweets for the time interval for the keywords and its hashtags
    :param keywords: (str) keywords
    :param start_date: (datetime) start date
    :param stop_date:  (datetime) stop date
    :param path_folder_sorted_merged: (str) path of the folder with the dataset
    :param hashtags: (list) list of the tags regarding the keywords
    :return: print the matching tweets
    """

    start_date_sec = int(datetime.timestamp(start_date))
    stop_date_sec = int(datetime.timestamp(stop_date))
    chunks_time_series_tot = select_chunk(start_date_sec, stop_date_sec, path_folder_sorted_merged)
    for n, chunk in enumerate(chunks_time_series_tot):
        all_tweets_that_match, _ = select_tags_keywords(keywords, hashtags, chunk, False)
        for i in all_tweets_that_match.text.values:
            print(i)


####################################################################################################################
# setting functions


def select_chunk(start_date_sec, stop_date_sec, path_folder_sorted_merged):
    """
    Function to select the chunk/chunks regarding the specified time interval
    :param start_date_sec: (int) start date in seconds
    :param stop_date_sec: (int) stop date in seconds
    :param path_folder_sorted_merged: (str) path of the folder with the sorted merged zipped csvs (the dataset)
    :return: list of the paths of all the the selected chunks
    """

    selected_chunks = []
    for chunk in os.listdir(path_folder_sorted_merged):
        match = re.match(r"tweet_(\d+)-(\d+).csv.gz", chunk)
        start_file = int(match.group(1))
        stop_file = int(match.group(2))
        if stop_date_sec >= start_file and start_date_sec <= stop_file:
            selected_chunks.append(os.path.join(path_folder_sorted_merged, chunk))
    return selected_chunks


def from_csv_to_time_series(csv_path):
    """
    Function to convert in series a csv containing such data
    """
    time_series = pd.read_csv(csv_path, header=None, parse_dates=[0], index_col=0, squeeze=True)
    return time_series


def to_time_series(all_tweets_that_match, tot_tweets, ts_daily_tot, ts_hourly_tot, ts_hourly_tot_abs, ts_hourly_tweets,
                   ts_hourly_retweets, ts_tweets_users):
    """
    Function to update the time series tot by hour or by day with indexes in date format with the data of the new chunk
    :param all_tweets_that_match: (dataframe) all the tweets that regard the keywords in the chunk under consideration
    :param tot_tweets: (dataframe) all the tweetsvin the chunk under consideration
    :param ts_daily_tot: (series) previous time series for daily percentage of tweets and retweets about the keywords
    :param ts_hourly_tot: (series) previous time series for daily percentage of tweets and retweets about the keywords
    :param ts_hourly_tot_abs: (series) previous time series for the hourly tweets and retweets about the keywords
    :param ts_hourly_tweets: (series) previous time series for the hourly percentage of tweets about the keywords
    :param ts_hourly_retweets: (series) previous time series for the hourly percentage of retweets about the keywords
    :param ts_tweets_users: (series) previous time series for the hourly tweets per user that tweet about the keywords
    :return: updated list of the updated time series with the count of the matching tweets of the new chunk
    """

    n_tags_keywords_daily = all_tweets_that_match.groupby(
        [all_tweets_that_match.date.map(lambda x: x.date())]).id.count()
    n_tags_keywords_hourly_tot = all_tweets_that_match.groupby(
        [all_tweets_that_match.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour))]).id.count()
    # tot time series with all the tweets in that period
    n_tot_hourly = tot_tweets.groupby(
        [tot_tweets.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour))]).id.count()
    n_tot_daily = tot_tweets.groupby([tot_tweets.date.map(lambda x: x.date())]).id.count()
    tot_tweets = 0  # free memory
    # perc time series and time series appended to the previous one
    ts_hourly_tot = ts_hourly_tot.add(n_tags_keywords_hourly_tot.divide(n_tot_hourly, fill_value=0), fill_value=0)
    ts_hourly_tot_abs = ts_hourly_tot_abs.add(n_tags_keywords_hourly_tot, fill_value=0).reindex(
        ts_hourly_tot.index.values, fill_value=0)  # holes as zeros
    ts_daily_tot = ts_daily_tot.add(n_tags_keywords_daily.divide(n_tot_daily, fill_value=0), fill_value=0)

    # time series n tweets/ for each user mean
    ts_hourly_tweets_users_chunk = all_tweets_that_match.groupby(
        [all_tweets_that_match.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour)),
         all_tweets_that_match.user_id]).size().groupby(level=0).mean()
    n_tags_keywords_hourly_tot, n_users = 0, 0  # free memory
    ts_tweets_users = ts_tweets_users.add(ts_hourly_tweets_users_chunk, fill_value=0)  # no holes for missing data
    ts_hourly_tweets_users_chunk = 0  # free memory

    # time series tweets by hour with indexes in date format
    n_tags_keywords_hourly_tw = all_tweets_that_match[all_tweets_that_match.retweet_id.isnull()].groupby(
        [all_tweets_that_match.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour))]).id.count()
    # perc time series and time series appended to the previous one
    ts_hourly_tweets = ts_hourly_tweets.add(n_tags_keywords_hourly_tw.divide(n_tot_hourly, fill_value=0), fill_value=0)
    n_tags_keywords_hourly_tw = 0  # free memory

    # time series retweets by hour with indexes in date format
    n_tags_keywords_hourly_rtw = all_tweets_that_match[all_tweets_that_match.retweet_id.notnull()].groupby(
        [all_tweets_that_match.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour))]).id.count()
    # perc time series and time series appended to the previous one
    ts_hourly_retweets = ts_hourly_retweets.add(n_tags_keywords_hourly_rtw.divide(n_tot_hourly, fill_value=0),
                                                fill_value=0)
    n_tags_keywords_hourly_rtw, n_tot_hourly = 0, 0  # free memory

    return [ts_daily_tot, ts_hourly_tot, ts_hourly_tot_abs, ts_hourly_tweets, ts_hourly_retweets, ts_tweets_users]


def users_analysis_chunk(all_tweets_that_match):
    """
    Function to produce a time series with indexes in date format with the count of unique users at that hour
    :param all_tweets_that_match: (dataframe) all the tweets that regard the keywords
    :return: hourly time series with the users that enter the conversation in that moment
    """

    unique_matches = all_tweets_that_match.drop_duplicates(subset='user_id', keep='first')
    ts_unique_users = unique_matches.groupby(
        [unique_matches.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour))]).user_id.count()

    return ts_unique_users


def select_tags_keywords(keywords, hashtags, chunk, reduced_version):
    """
    Function to select the matching tweets for the chunk under consideration for the keywords and their hashtags
    :param keywords: (str) keywords
    :param hashtags: (list) list of the tags regarding the keywords
    :param chunk: (str) path of the chunk under consideration
    :param reduced_version: (boolean) to save memory just a reduced version of the tweets without text
    :return: 1) dataset with matching tweets for the chunk under consideration for the keywords and their hashtags
             2) dataset with all the tweets for the chunk under consideration
    """
    col_names_tweet = ["id", "user_id", "text", "date", "lang", "location", "hashtags", "urls", "retweet_id"]
    reader_chunk = pd.read_csv(chunk, compression='gzip', index_col=False, names=col_names_tweet
                               )[['date', 'id', 'user_id', 'retweet_id', "text", "hashtags"]]  # data frame reduced
    reader_chunk.date = reader_chunk.date.map(lambda x: datetime.fromtimestamp(x))  # convert from sec into datetime
    if keywords is not None:
        filter_match_keywords = reader_chunk.text.str.contains(keywords, flags=re.IGNORECASE, regex=False,
                                                               na=False)
        # tweets that contain or the selected hashtags or the keywords
        if hashtags is None:
            if reduced_version:  # to save memory or complete tweets
                all_tweets_that_match = reader_chunk[filter_match_keywords][['date', 'id', 'user_id', 'retweet_id']]
            else:
                all_tweets_that_match = reader_chunk[filter_match_keywords]
        else:
            hashtags_celeb_preprocessed = []  # select just the hashtags taken independently
            for tag_celeb in hashtags:
                hashtags_celeb_preprocessed.append(r'\b'+tag_celeb+r'\b')
            filter_match_hashtags = reader_chunk.hashtags.str.replace(':', ' ').\
                str.contains('|'.join(hashtags_celeb_preprocessed), regex=True, flags=re.IGNORECASE, na=False)
            if reduced_version:  # to same memory or complete tweets
                all_tweets_that_match = reader_chunk[filter_match_keywords | filter_match_hashtags][[
                    'date', 'id', 'user_id', 'retweet_id']]
            else:
                all_tweets_that_match = reader_chunk[filter_match_keywords | filter_match_hashtags]
    else:
        if hashtags is not None:
            hashtags_celeb_preprocessed = []  # select just the hashtags taken independently
            for tag_celeb in hashtags:
                hashtags_celeb_preprocessed.append(r'\b'+tag_celeb+r'\b')
            filter_match_hashtags = reader_chunk.hashtags.str.replace(':', ' ').\
                str.contains('|'.join(hashtags_celeb_preprocessed), regex=True, flags=re.IGNORECASE, na=False)
            if reduced_version:  # to same memory or complete tweets
                all_tweets_that_match = reader_chunk[filter_match_hashtags][['date', 'id', 'user_id', 'retweet_id']]
            else:
                all_tweets_that_match = reader_chunk[filter_match_hashtags]
        else:
            all_tweets_that_match = None

    return all_tweets_that_match, reader_chunk


def tags_keywords_chunk(keywords, hashtags, chunk, ts_daily_tot, ts_hourly_tot, ts_hourly_tot_abs,
                        ts_hourly_tweets, ts_hourly_retweets, ts_tweets_users):
    """
    Function to update the time series with the matching tweets in the new chunk
    :param keywords: (str) keywords
    :param hashtags: (list) list of the tags regarding the keywords
    :param chunk: (str) path of the chunk under consideration
    :param ts_daily_tot: (series) previous time series for daily percentage of tweets and retweets about the keywords
    :param ts_hourly_tot: (series) previous time series for daily percentage of tweets and retweets about the keywords
    :param ts_hourly_tot_abs: (series) previous time series for the hourly tweets and retweets about the keywords
    :param ts_hourly_tweets: (series) previous time series for the hourly percentage of tweets about the keywords
    :param ts_hourly_retweets: (series) previous time series for the hourly percentage of retweets about the keywords
    :param ts_tweets_users: (series) previous time series for the hourly tweets per user that tweet about the keywords
    :return: updated time series with the matching tweets in the new chunk
    """

    # tweets that contain or the selected hashtags or the keywords
    all_tweets_that_match, reader_chunk = select_tags_keywords(keywords, hashtags, chunk, True)
    time_series = [ts_daily_tot, ts_hourly_tot, ts_hourly_tot_abs, ts_hourly_tweets, ts_hourly_retweets,
                   ts_tweets_users]
    # updated time series with indexes in date format
    time_series = to_time_series(all_tweets_that_match, reader_chunk, *time_series)

    return time_series, all_tweets_that_match[['date', 'user_id']]


def tags_keywords_chunk_baseline_analysis(death_date, before_date, ts_daily, ts_hourly, ts_hourly_tot_abs,
                                          ts_unique_users_beginning, times_sd):
    """
    Function to analyse the period before the shock of the death
    :param death_date: (datetime) day of the death
    :param before_date: (datetime) day from where we have to start the analysis of the baseline
    :param ts_daily: (series) daily time series of the percentage of tweets/retweets of the keywords before to be cut
    :param ts_hourly: (series) hourly time series of the percentage of tweets/retweets of the keywords before to be cut
    :param ts_hourly_tot_abs: (series) daily absolute time series of tweets/retweets of the keywords before to be cut
    :param ts_unique_users_beginning: (series) hourly time series of the unique users of the keywords before to be cut
    :param times_sd: (int) times of the sd before we can consider that a deviation is not noise
    :return: 1) list of the baseline  daily, hourly, hourly in absolute values and of the unique users
             2) datetime of the beginning of the discussion
             3) daily standard deviation of the baseline, to be used to determine the stopping date
    """

    # compute baseline
    baseline_daily = ts_daily[before_date.date():(death_date-timedelta(days=1)).date()].median()
    baseline_hourly = ts_hourly[before_date:death_date-timedelta(hours=1)].median()
    baseline_hourly_abs = ts_hourly_tot_abs[before_date:death_date-timedelta(hours=1)].median()
    baseline_unique_users = ts_unique_users_beginning[before_date:death_date-timedelta(hours=1)].median()
    # standard date of the initial phase before the discussion starts
    std_daily = ts_daily[before_date.date():(death_date-timedelta(days=1)).date()].std()

    # compute start_discussion_date: the date they start to discuss
    start_disc_date = before_date  # if no match
    # for loop in reverse order
    for day, perc_daily in ts_daily[before_date.date():death_date.date()].sort_index(ascending=False).iteritems():
        if perc_daily <= baseline_daily + times_sd*std_daily:  # first day that they do not talk about it
            print("Found start_disc_date")
            start_disc_date = datetime(day.year, day.month, day.day)  # to datetime
            break

    print("baseline_daily: " + str(baseline_daily))
    print("baseline_hourly: " + str(baseline_hourly))
    print("baseline_hourly_abs: " + str(baseline_hourly_abs))
    print("start_disc_date: " + str(start_disc_date))
    print("daily standard deviation: " + str(std_daily))
    baselines = [baseline_daily, baseline_hourly, baseline_hourly_abs, baseline_unique_users]
    if std_daily is np.NaN:  # if no standard deviation make it null
        std_daily = 0

    return baselines, start_disc_date, std_daily


def tags_keywords_chunk_t_max_analysis(time_series_daily):
    """
    Function to determine the day when the daily time series has its maximum to find the stop date
    :param time_series_daily: daily time series
    :return: datetime of the day when the daily time series has its maximum
    """

    # compute t_max_date
    t_max_day = time_series_daily.idxmax()  # max_id
    t_max = datetime(t_max_day.year, t_max_day.month, t_max_day.day)  # to datetime

    print("t_max: " + str(t_max))

    return t_max


def tags_keywords_chunk_stop_disc_analysis(before_date, baseline_daily, t_max_day, time_series_daily, days_under,
                                           times_sd, std):
    """
    Function to cut the time series and determine the stop discussion date, when the activity went to <= baseline + std
    for days_under consecutive days
    :param before_date: (datetime) date of the beginning of the analysis
    :param baseline_daily: (float) daily baseline
    :param t_max_day: (datetime) day when the daily time series has its maximum
    :param time_series_daily: (series) daily time series of the percentage of tweets/retweets before to be cut
    :param days_under: (int) consecutive days that the activity is back to normal (<= baseline + std) for end discussion
    :param times_sd: (int) times of the sd before we can consider that a deviation is not noise
    :param std: (float) daily standard deviation
    :return: cut daily time series and determine the stop discussion date
    """

    stop_disc_date = None
    d_under = 0
    for day, perc_daily in time_series_daily[t_max_day.date()::].iteritems():  # time series for decay: from t_max
        if perc_daily <= baseline_daily + times_sd*std:  # check if for n consecutive days activity under the base + sd
            d_under += 1
        else:  # if the activity level goes back to be higher than the daily activity level
            d_under = 0
        if d_under == days_under:  # if it is tot days that the daily activity level was under the basel + sd then break
            stop_disc_date = datetime(day.year, day.month, day.day) - timedelta(days=days_under)  # right before silence
            time_series_daily = time_series_daily[before_date.date():stop_disc_date.date()]  # daily ts complete
            break

    print("stop_disc_date: " + str(stop_disc_date))

    return time_series_daily, baseline_daily, stop_disc_date


def count_time_step_no_end(keywords, hashtags, death_date, path_folder_sorted_merged,
                           days_before_baseline, days_under, times_sd):
    """
    Function to obtain the cut the time series, baselines, start discussion date, stop discussion date and all the
    posts that match for the keywords under consideration for all the available chunks
    :param keywords: (str) keywords
    :param hashtags: (list) hashtags relating the keywords under consideration
    :param death_date: (datetime) death date
    :param path_folder_sorted_merged: (str) path of the folder with the dataset
    :param days_before_baseline: (int) number of days used to determine the baseline
    :param days_under: (int) consecutive days that the activity is back to normal (<= baseline + std) for end discussion
    :param times_sd: (int) times of the sd before we can consider that a deviation is not noise
    :return: the time series, baselines, start discussion date, stop discussion date and all the matching posts
    """

    death_date = datetime(death_date.year, death_date.month, death_date.day)  # to datetime
    death_sec = int(datetime.timestamp(death_date))
    before_date = death_date - timedelta(days=days_before_baseline)  # weeks before to observe the baseline
    before_sec = int(datetime.timestamp(before_date))  # from datetime to sec: timestamp()/opp: datetime.fromtimestamp()
    chunks_time_series_tot = select_chunk(before_sec, math.inf, path_folder_sorted_merged)
    # one week after to observe the max
    week_after_sec = int(datetime.timestamp(death_date + timedelta(days=7)))
    chunks_till_week_after_death = select_chunk(death_sec, week_after_sec, path_folder_sorted_merged)
    # till death to observe the baseline
    chunks_till_death = select_chunk(before_sec, death_sec, path_folder_sorted_merged)
    start_disc_date, stop_disc_date, t_max_day = None, None, None
    ts_daily_tot, ts_hourly_tot, ts_hourly_tweets = pd.Series(), pd.Series(), pd.Series()
    ts_hourly_retweets, ts_unique_users, ts_tweets_users = pd.Series(), pd.Series(), pd.Series()
    ts_hourly_tot_abs, all_tweets_that_match = pd.Series(), pd.DataFrame()
    time_series = [ts_daily_tot, ts_hourly_tot, ts_hourly_tot_abs, ts_hourly_tweets, ts_hourly_retweets,
                   ts_tweets_users]
    baseline_hourly, baseline_daily, baseline_hourly_abs, baseline_unique_users, std = None, None, None, None, None
    for n, chunk in enumerate(chunks_time_series_tot):

        print('chunk: ' + str(n))
        # time series of the chunk and users lists
        time_series, tweets_that_match_chunk = \
            tags_keywords_chunk(keywords, hashtags, chunk, *time_series)
        ts_daily_tot, ts_hourly_tot, ts_hourly_tot_abs, ts_hourly_tweets, ts_hourly_retweets, ts_tweets_users = \
            time_series
        all_tweets_that_match = all_tweets_that_match.append(tweets_that_match_chunk, ignore_index=True)

        if chunk == chunks_till_death[-1]:  # compute baseline
            ts_unique_users_beginning = users_analysis_chunk(all_tweets_that_match)
            # holes as zeros
            ts_unique_users_beginning = ts_unique_users_beginning.reindex(ts_hourly_tot.index.values, fill_value=0)
            baselines, start_disc_date, std = \
                tags_keywords_chunk_baseline_analysis(death_date, before_date, ts_daily_tot, ts_hourly_tot,
                                                      ts_hourly_tot_abs, ts_unique_users_beginning, times_sd)
            baseline_daily, baseline_hourly, baseline_hourly_abs, baseline_unique_users = baselines
        if chunk == chunks_till_week_after_death[-1]:  # compute max
            t_max_day = tags_keywords_chunk_t_max_analysis(ts_daily_tot)
        if t_max_day is not None:  # compute end of time series
            if stop_disc_date is None:
                ts_daily_tot, baseline_daily, stop_disc_date = tags_keywords_chunk_stop_disc_analysis(
                    before_date, baseline_daily, t_max_day, ts_daily_tot, days_under, times_sd, std)
            if stop_disc_date is not None:  # if end of the time series is found break
                # include stop_date and fill missing values of dates
                index_date = pd.date_range(start_disc_date, stop_disc_date+timedelta(hours=23), freq='H')
                ts_hourly_tot = ts_hourly_tot[start_disc_date:stop_disc_date + timedelta(hours=23)].reindex(
                    index_date, fill_value=0)  # cut and reindex
                ts_hourly_tot_abs = ts_hourly_tot_abs[start_disc_date:stop_disc_date+timedelta(hours=23)].reindex(
                    index_date, fill_value=0)  # cut and reindex
                ts_hourly_tweets = ts_hourly_tweets[start_disc_date:stop_disc_date+timedelta(hours=23)].reindex(
                    index_date, fill_value=0)  # cut and reindex
                ts_hourly_retweets = ts_hourly_retweets[start_disc_date:stop_disc_date+timedelta(hours=23)].reindex(
                    index_date, fill_value=0)  # cut and reindex
                ts_tweets_users = ts_tweets_users[start_disc_date:stop_disc_date+timedelta(hours=23)].reindex(
                    index_date, fill_value=0)  # cut and reindex

                # users_analysis
                ts_unique_users = users_analysis_chunk(all_tweets_that_match)[
                                  start_disc_date:stop_disc_date+timedelta(hours=23)].reindex(index_date, fill_value=0)
                break

    if stop_disc_date is None:  # if the end of the discussion hasn't been found yet
        ts_hourly_tot = ts_hourly_tot[start_disc_date:]
        ts_hourly_tot_abs = ts_hourly_tot_abs[start_disc_date:]
        ts_hourly_tweets = ts_hourly_tweets[start_disc_date:]
        ts_hourly_retweets = ts_hourly_retweets[start_disc_date:]
        ts_tweets_users = ts_tweets_users[start_disc_date:]
        ts_unique_users = users_analysis_chunk(all_tweets_that_match)[start_disc_date:]

    time_series = [ts_daily_tot, ts_hourly_tot, ts_hourly_tot_abs, ts_hourly_tweets, ts_hourly_retweets,
                   ts_tweets_users]
    baselines = [baseline_daily, baseline_hourly, baseline_hourly_abs, baseline_unique_users, std]

    return time_series, ts_unique_users, baselines, start_disc_date, stop_disc_date, all_tweets_that_match


def plot_series(time_series, x_label, y_label, title, path_file):
    """
    Function to plot the time series
    :param time_series: (series) time series to be plot
    :param x_label: (str) text for the label in the x axis
    :param y_label: (str) text for the label in the y axis
    :param title: (str) text for the title
    :param path_file: (str) path of the location where to save the time series
    :return: plot the time series saved in the path_file
    """

    if not time_series.empty:  # if the database is not empty
        x_figsize = 10
        fig, ax = plt.subplots(1, 1, figsize=(x_figsize, x_figsize*(np.sqrt(5)-1)/2))
        # tufte.plot_style(ax, plot_type='line')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y %H'))
        ax.plot(pd.DatetimeIndex(time_series.index.values).to_pydatetime(), time_series.values,
                color=tufte.colors('ts'))  # Data
        ax.set_title(title, fontsize=24)
        ax.set_xlabel(x_label, fontsize=12)
        ax.set_ylabel(y_label, fontsize=12)
        fig.autofmt_xdate()  # autoformat date x ticks
        fig.savefig(path_file)
        plt.close(fig)  # close open figure


####################################################################################################################
# useful functions


def dataset_descriptive(start_date, stop_date, path_folder_sorted_merged, path_folder_plots):
    """
    Function for the description of the behaviour of the user sample through plots in tufte style
    :param start_date: (datetime) start date of the analysis
    :param stop_date: (datetime) stop date of the analysis
    :param path_folder_sorted_merged: (str) path of the folder with the dataset
    :param path_folder_plots: (str) path of the plot where to save the plots
    :return: 3 plots with tweets and retweets and tw and retweets per user per hour, per day of the week and per month
    """

    start_date_sec = int(datetime.timestamp(start_date))
    stop_date_sec = int(datetime.timestamp(stop_date))
    chunks_time_series_tot = select_chunk(start_date_sec, stop_date_sec, path_folder_sorted_merged)
    n_tot_hourly, n_user_hourly = pd.Series(name='hourly'), pd.Series(name='daily')

    for n, chunk in enumerate(chunks_time_series_tot):
        print(n)
        _, tot_tweets_chunk = select_tags_keywords(None, None, chunk, True)
        # tot tweets and retweets
        n_tot_hourly_chunk = tot_tweets_chunk.groupby(
            [tot_tweets_chunk.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour))]).id.count()[
                             start_date:stop_date - timedelta(hours=1)]
        n_tot_hourly = n_tot_hourly.add(n_tot_hourly_chunk, fill_value=0)
        # tweets and retweets per user
        n_user_hourly_chunk = tot_tweets_chunk.groupby(
            [tot_tweets_chunk.date.map(lambda x: datetime(x.year, x.month, x.day, x.hour)),
             tot_tweets_chunk.user_id]).id.count()[start_date:stop_date - timedelta(hours=1)].groupby('date').median()
        n_user_hourly = n_user_hourly.add(n_user_hourly_chunk, fill_value=0)

        tot_tweets_chunk = None  # free memory
    n_tot_hourly_chunk, n_user_hourly_chunk = 0, 0  # free memory

    index_date_h = [parser.parse(str(h)).strftime("%H") for h in n_tot_hourly.index.values]  # new index with hour
    index_date_d = [parser.parse(str(d)).strftime("%A") for d in n_tot_hourly.index.values]  # new index with weekly day
    index_date_m = [parser.parse(str(d)).strftime("%B") for d in n_tot_hourly.index.values]  # new index with month
    dataset_descr = pd.DataFrame(data={'hour of the day': index_date_h, 'day of the week': index_date_d,
                                       'month': index_date_m, 'tweets and retweets': n_tot_hourly.values,
                                       'tweets and retweets per user': n_user_hourly.values})
    # for the right order of day of the week and month
    week_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
              'November', 'December']

    dataset_descr.to_csv(
        os.path.join(path_folder_plots, str(start_date.date()) + '_' + str(stop_date.date()) + "_descriptive.csv"))

    #################################################################################################################
    # plot dataset analysis per hour

    x_figsize = 10
    fig, (ax0, ax1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]},
                                   figsize=(x_figsize, x_figsize*(np.sqrt(5)-1)/2))

    # frequency tweets and retweets
    dataset_hourly_tot = pd.DataFrame()
    for k, v in dataset_descr[['hour of the day', "tweets and retweets"]].groupby('hour of the day'):
        s1 = pd.DataFrame({k: v["tweets and retweets"].values})
        dataset_hourly_tot = pd.concat([dataset_hourly_tot, s1], axis=1)
    fig, ax0 = tufte.bplot(fig, ax0, dataset_hourly_tot, ticklabelsize=10, linewidth_perc=5, n_max_values=0)
    ax0.set_ylabel("number of posts", fontsize=12)

    # frequency tweets and retweets per user
    dataset_hourly_per_user = pd.DataFrame()
    for k, v in dataset_descr[['hour of the day', "tweets and retweets per user"]].groupby('hour of the day'):
        s1 = pd.DataFrame({k: v["tweets and retweets per user"].values})
        dataset_hourly_per_user = pd.concat([dataset_hourly_per_user, s1], axis=1)
    fig, ax1 = tufte.bplot(fig, ax1, dataset_hourly_per_user, ticklabelsize=10, linewidth_perc=5, n_max_values=0)
    ax1.set_xlabel("hour of the day", fontsize=12)
    ax1.set_ylabel("number of posts per user", fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(path_folder_plots, 'dataset_hourly.png'), bbox_inches='tight')
    plt.close(fig)  # close open figure

    #################################################################################################################
    # plot dataset analysis per day of the week

    fig, (ax0, ax1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]},
                                   figsize=(x_figsize, x_figsize*(np.sqrt(5)-1)/2))

    # frequency tweets and retweets
    dataset_weekly = pd.DataFrame()
    for k, v in dataset_descr[['day of the week', "tweets and retweets"]].groupby('day of the week'):
        s1 = pd.DataFrame({k: v["tweets and retweets"].values})
        dataset_weekly = pd.concat([dataset_weekly, s1], axis=1)
    fig, ax0 = tufte.bplot(fig, ax0, dataset_weekly[week_days], ticklabelsize=10, linewidth_perc=5, n_max_values=0)
    ax0.set_ylabel("number of posts", fontsize=12)
    ax0.set_xticks([])  # remove x ticks

    # frequency tweets and retweets per user
    dataset_weekly_per_user = pd.DataFrame()
    for k, v in dataset_descr[['day of the week', "tweets and retweets per user"]].groupby('day of the week'):
        s1 = pd.DataFrame({k: v["tweets and retweets per user"].values})
        dataset_weekly_per_user = pd.concat([dataset_weekly_per_user, s1], axis=1)
    fig, ax1 = tufte.bplot(fig, ax1, dataset_weekly_per_user[week_days], ticklabelsize=10, linewidth_perc=5,
                           n_max_values=0)
    ax1.set_xlabel("day of the week", fontsize=12)
    ax1.set_ylabel("number of posts per user", fontsize=8)
    fig.autofmt_xdate()  # format x_data
    fig.tight_layout()
    fig.savefig(os.path.join(path_folder_plots, 'dataset_weekly.png'), bbox_inches='tight')
    plt.close(fig)  # close open figure

    #################################################################################################################
    # plot dataset analysis per month

    fig, (ax0, ax1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]},
                                   figsize=(x_figsize, x_figsize*(np.sqrt(5)-1)/2))

    # frequency tweets and retweets
    dataset_monthly = pd.DataFrame()
    for k, v in dataset_descr[['month', "tweets and retweets"]].groupby('month'):
        s1 = pd.DataFrame({k: v["tweets and retweets"].values})
        dataset_monthly = pd.concat([dataset_monthly, s1], axis=1)
    fig, ax0 = tufte.bplot(fig, ax0, dataset_monthly[months], ticklabelsize=10, linewidth_perc=5, n_max_values=0)
    ax0.set_ylabel("number of posts", fontsize=12)

    # frequency tweets and retweets per user
    dataset_monthly_per_user = pd.DataFrame()
    for k, v in dataset_descr[['month', "tweets and retweets per user"]].groupby('month'):
        s1 = pd.DataFrame({k: v["tweets and retweets per user"].values})
        dataset_monthly_per_user = pd.concat([dataset_monthly_per_user, s1], axis=1)
    fig, ax1 = tufte.bplot(fig, ax1, dataset_monthly_per_user[months], ticklabelsize=10, linewidth_perc=5,
                           n_max_values=0)
    ax1.set_xlabel("month", fontsize=12)
    ax1.set_ylabel("number of posts per user", fontsize=8)
    fig.autofmt_xdate()  # format x_data
    fig.tight_layout()
    fig.savefig(os.path.join(path_folder_plots, 'dataset_monthly.png'), bbox_inches='tight')
    plt.close(fig)  # close open figure
