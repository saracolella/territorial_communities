import os
import re
import logging
import json
import hashlib
from pathlib import Path
from dateutil import parser
from dateutil.relativedelta import relativedelta
import csv
import datetime
import gzip
import requests
import rest
import fields_of_interest

# Download the rest archive in json file for a list of users.
# download_new_user: check if the criteria of "Italianess" and of activity (tweeted in the last month) are still
#                    satisfied and save the archive. Useful to filter users after a certain time and for the level of
#                    consistency for the tweet language.
# download_archive_json: save the archive without any check

# After having converted the json into sorted and merged chunks of zipped csv rename them.
# rename_chunks: rename chunks according the time interval: the first and last id of the tweets contained in the chunk


####################################################################################################################
# function that just help to understand the structure of a file


def lines_from_tweet(tweet):
    """
    Function to retrieve the fields of interest for the tweet from the tweets
    :param tweet: dictionary
    :return: lines: tweet, retweets, retweet_user, t_h_t,  rt_h_t, t_u_t, rt_u_t
    """

    tweet_line = []
    retweet_line = []
    retweet_user_line = []
    t_h_t_line = []
    rt_h_t_line = []
    t_u_t_line = []
    rt_u_t_line = []

    if 'retweeted_status' in tweet:
        retweet_id = tweet['retweeted_status']['id']
        retweet = tweet['retweeted_status']  # if a retweet is present save it and also the correspondent user
        retweet_user_line.append([retweet['user']['id'], retweet['user']['screen_name'].replace("\n", " ").replace(
            "\r", " "), retweet['user']["lang"], fields_of_interest.create_time_zone_from_tweet(retweet),
                                  retweet["user"]["description"].replace("\n", " ").replace("\r", " "),
                                  fields_of_interest.create_location_description_from_tweet(retweet).replace(
                                      "\n", " ").replace("\r", " ")])
        try:
            retweet_line.append([retweet['id'], retweet['user']['id'],
                                 str(retweet['full_text']).replace("\n", " ").replace("\r", " "),
                                 parser.parse(retweet['created_at']).strftime("%Y-%m-%d %H:%M:%S"),
                                 retweet["lang"], fields_of_interest.create_place_from_tweet(retweet)])
        except KeyError:
            retweet_line.append([retweet['id'], retweet['user']['id'],
                                 str(retweet['text']).replace("\n", " ").replace("\r", " "),
                                 parser.parse(retweet['created_at']).strftime("%Y-%m-%d %H:%M:%S"),
                                 retweet["lang"], fields_of_interest.create_place_from_tweet(retweet)])

        # check if there are hashtags, if so add them to the list t_h_t
        if 'hashtags' in retweet['entities']:
            tags = [h['text'] for h in retweet['entities']['hashtags']]
            # Deduplicate tags since they may be used multiple times per tweet
            tags = deduplicate_lowercase(tags)
            if len(tags) > 0:
                for tag in tags:
                    rt_h_t_line.append([retweet['id'], tag])

        # check if there are urls, if so add them to the list t_u_t
        if 'urls' in retweet['entities']:
            urls = [u['expanded_url'] for u in retweet['entities']['urls']]
            # Deduplicate urls since they may be used multiple times per tweet
            urls = deduplicate(urls)
            if len(urls) > 0:
                for u in urls:
                    if len(u) > 4000:  # if url is too long then it can't be directly an index and it should be encoded
                        u = hashlib.md5(u.encode()).hexdigest()
                    rt_u_t_line.append([retweet['id'], u])

    else:
        retweet_id = None
    try:
        tweet_line.append([tweet['id'], tweet['user']['id'], str(tweet['full_text']).replace("\n", " ").replace(
            "\r", " "), parser.parse(tweet['created_at']).strftime("%Y-%m-%d %H:%M:%S"), tweet["lang"],
                           fields_of_interest.create_place_from_tweet(tweet), retweet_id])
    except KeyError:
        tweet_line.append([tweet['id'], tweet['user']['id'], str(tweet['text']).replace("\n", " ").replace("\r", " "),
                           parser.parse(tweet['created_at']).strftime("%Y-%m-%d %H:%M:%S"),
                           tweet["lang"], fields_of_interest.create_place_from_tweet(tweet), retweet_id])

    # check if there are hashtags, if so add them to the list t_h_t
    if 'hashtags' in tweet['entities']:
        tags = [h['text'] for h in tweet['entities']['hashtags']]
        # Deduplicate tags since they may be used multiple times per tweet
        tags = deduplicate_lowercase(tags)
        if len(tags) > 0:
            for tag in tags:
                t_h_t_line.append([tweet['id'], tag])

    # check if there are urls, if so add them to the list t_u_t
    if 'urls' in tweet['entities']:
        urls = [u['expanded_url'] for u in tweet['entities']['urls']]
        # Deduplicate urls since they may be used multiple times per tweet
        urls = deduplicate(urls)
        if len(urls) > 0:
            for u in urls:
                if len(u) > 4000:  # if url is too long then it can't be directly an index and it should be encoded
                    u = hashlib.md5(u.encode()).hexdigest()
                t_u_t_line.append([tweet['id'], u])

    return tweet_line, retweet_line, retweet_user_line, t_h_t_line, rt_h_t_line, t_u_t_line, rt_u_t_line


def read_encoded_file(data_folder, filename):
    # download_archive.read_encoded_file('', 'archive_frequent_users_2018')
    """
    Function to read the compressed file
    :param data_folder: folder where the file is
    :param filename: name of the file to be read is
    :return: print compressed file line by line
    """

    for line in list(csv.reader(open_file(data_folder, filename, 'r', 'gz')))[:100]:
        print(line)


def null_bytes(disk_out, file_name):
    # download_archive.null_butes('', 'replaced_archive_frequent_users_2018_tweet.csv')

    """
    Function to check if a file contains a null byte
    :param disk_out: folder of the file
    :param file_name: filename of the file
    :return: boolean if a file contains a null byte
    """

    with open(os.path.join(disk_out, file_name), 'r', encoding='utf-8') as file_csv:
        if '\0' in file_csv.read():
            print("have null byte")


def replacement_file(disk, file_name, old_new_text):
    # download_archive.replacement_file('', 'archive_frequent_users_2018_4_user.csv.gz', [['None', ''],
    # ['\0', '_'], ['\x00', '_']])

    """
    Function to replace some words with some others in a compressed file
    :param disk: disk where the files gz are
    :param file_name: name of the compressed file that has to searched for the replacements
    :param old_new_text: list of lists with the words that have to be replaced with the second one in the list
                         also for no null byte
    :return: decompressed csv files
    """
    user_csv_u = gzip.open(os.path.join(disk, file_name), 'rt', encoding='utf-8', newline='')
    user_csv = user_csv_u.read()

    # do the replace
    for i in range(len(old_new_text)):
        print(old_new_text[i-1])
        user_csv = re.sub(old_new_text[i-1][0], old_new_text[i-1][1], user_csv)

    # write the file
    user_csv_r = gzip.open(os.path.join(disk, "replaced_" + file_name), 'wt', encoding='utf-8', newline='')
    user_csv_r.write(user_csv)


def lowercase_file(folder, file, lower_file):
    # download_archive.lowercase_file('', file, lower_file)

    """
    Function to make all the words in a file as lowercase
    :param folder: folder where the file is
    :param file: filename to be examined
    :param lower_file: filename of the file with the replaced words
    :return:
    """

    with gzip.open(os.path.join(folder, file), 'r') as f:
        lines = [line.lower() for line in f]

    with gzip.open(os.path.join(folder, lower_file), 'w') as lf:
        lf.writelines(lines)

    os.remove(os.path.join(folder, file))
    os.rename(os.path.join(folder, lower_file), os.path.join(folder, file))


def all_lowercase(folder, root_name):
    # download_archive.all_lowercase('', 'replaced_archive_frequent_users_2018')

    """
    Function to make all the words in all the files in a folder as lowercase
    :param folder: name of the folder where the zipped csv files are
    :param root_name: root of the name of the file
    :return: files with all in lowercase
    """

    file_names_list = list(Path(folder).glob(root_name+'*.csv.gz'))
    print(file_names_list)

    for file_name in file_names_list:
        lowercase_file(folder, file_name, "lower_" + str(file_name))


###################################################################################################################
# setting functions


def deduplicate_lowercase(l):
    """
    Helper function that performs two things:
    - Converts everything in the list to lower case
    - Deduplicates the list by converting it into a set and back to a list

    :param l: list
    :return: deduplicated list
    """
    valid = list(filter(None, l))
    lowercase = [e.lower() for e in valid]
    if len(valid) != len(lowercase):
        logging.warning(
            "The input file had {0} empty lines, skipping those. Please verify that it is complete and valid.".format(
                len(lowercase) - len(valid)))
    deduplicated = list(set(lowercase))
    return deduplicated


def deduplicate(l):
    """
    Helper function that performs two things:
    - Converts everything in the list to lower case
    - Deduplicates the list by converting it into a set and back to a list

    :param l: list
    :return: deduplicated list
    """
    valid = list(filter(None, l))
    lowercase = [e.lower() for e in valid]
    if len(valid) != len(lowercase):
        logging.warning(
            "The input file had {0} empty lines, skipping those. Please verify that it is complete and valid.".format(
                len(lowercase) - len(valid)))
    deduplicated = list(set(valid))
    return deduplicated


def readlines_reverse(filename):

    """
    Read file line by line in reverse order
    :param filename: file with the archive
    :return: line by line of the file in a reversed order
    """
    with open(filename, 'r') as gfile:
        gfile.seek(0, os.SEEK_END)
        position = gfile.tell()
        line = ''
        while position >= 0:
            gfile.seek(position)
            try:
                next_char = gfile.read(1).decode('utf-8')
            except AttributeError:
                next_char = gfile.read(1)
            if next_char == "\n":
                yield line[::-1]
                line = ''
            else:
                line += next_char
            position -= 1
        yield line[::-1]


def list_no_retrieved_users(filtered_user_path, archive_name, disk_out, last_user):
    # download.list_no_retrieved_users('unique_frequent_users.csv', 'archive_frequent_users_2018', '', None)

    """
    Check users stored in the csv file to consider the users not already stored
    :param filtered_user_path: csv file with the list of filtered selected users
    :param archive_name: name of the file with the archive
    :param disk_out: disk_out to save json
    :param last_user: plan B if no csv file: Insert user id (as int) to start retrieve data from there
    :return: no_retrieved_users: list of the users whose tweets have not been retrieved yet
    """

    from_beginning = True  # start from the beginning if no users are stored
    no_retrieved_users = []  # list of users that still have to be retrieved
    if last_user is None:
        if os.path.isfile(os.path.join(disk_out, archive_name)):
            print("Users already present")
            user_present = False
            for line in readlines_reverse(os.path.join(disk_out, archive_name)):  # read file line by line reversed
                while line != '' and user_present is False:  # skip last line because it is white
                    line = json.loads(line)  # read line of the json file
                    print('last tweet: ' + str(line))
                    last_user_retrieved = line['user']['id']  # last user found, start from here in unique_users list
                    #  open csv file and transform it into a list, but just the users that aren't stored yet
                    with open(filtered_user_path, 'r') as uu:
                        reader = csv.reader(uu)
                        next(reader, None)  # skip header
                        found_retrieved_user = False
                        for rows in reader:
                            if found_retrieved_user is True:  # from now on consider all the users and add them to list
                                no_retrieved_users.append(rows)
                            if rows[0] == str(last_user_retrieved):  # if the last retrieved user was found
                                user_present = True
                                from_beginning = False
                                found_retrieved_user = True
                                no_retrieved_users.append(rows)  # to be sure add also last retrieved user to list

                        if len(no_retrieved_users) == 0:  # if last retrieved user not found start from the beginning
                            from_beginning = True
                            break

                        print("number of users that still have to be retrieved: " + str(len(no_retrieved_users)))
        else:
            from_beginning = True
    else:
        print("Some users have already been retrieved")
        with open(filtered_user_path, 'r') as uu:
            reader = csv.reader(uu)
            next(reader, None)  # skip header
            found_retrieved_user = False
            for rows in reader:
                if found_retrieved_user is True:  # from now on consider all the users and add them to list
                    no_retrieved_users.append(rows)
                if rows[0] == str(last_user):  # if the last retrieved user was found
                    from_beginning = False
                    found_retrieved_user = True
                    no_retrieved_users.append(rows)  # to be sure add also last retrieved user to list
            print("number of users that still have to be retrieved: " + str(len(no_retrieved_users)))

    if from_beginning is True:
        print("Users no retrieved yet")
        #  open csv file and transform it into the list of no retrieved users
        with open(filtered_user_path, 'r') as uu:
            reader = csv.reader(uu)
            next(reader, None)  # skip header
            #  dictionary of users with retrieval date as value
            no_retrieved_users = []  # list of users that still have to be retrieved
            for rows in reader:
                no_retrieved_users.append(rows)
        print("number of users that still have to be retrieved: " + str(len(no_retrieved_users)))

    return no_retrieved_users


def user_from_tweet(tweet, retrieval_date, frequency_user, nationality_user):
    """
    Function to retrieve the fields of interest for the user from the tweet
    :param tweet: dictionary
    :param retrieval_date: str with retrieval date
    :param frequency_user: n tweets/second
    :param nationality_user: str of a list with the italianity of the criteria of the first page of the user
    :return: user_line: list with all the selected fields of the user
    """

    user_line = [tweet['user']['id'], tweet['user']['screen_name'].replace("\n", " ").replace("\r", " "),
                 tweet['user']["lang"], fields_of_interest.create_time_zone_from_tweet(tweet),
                 tweet["user"]["description"].replace("\n", " ").replace("\r", " "),
                 retrieval_date, frequency_user, nationality_user,
                 fields_of_interest.create_location_description_from_tweet(tweet).replace("\n", " ").replace("\r", " ")]

    return user_line


def sanitize(value):
    # Function to have sane inputs
    return value.replace('\n', ' ').replace('\r', ' ')


def reformat_date(s):
    """
    Function to have the data from string to datetime
    :param s: string of date in wrong format
    :return date in correct format  %Y-%m-%d %H:%M:%S
    """
    return datetime.datetime \
        .strptime(s, '%a %b %d %H:%M:%S +0000 %Y') \
        .strftime('%Y-%m-%d %H:%M:%S')


def open_file(data_folder, filename, mode, ending):
    """
    Function to open the file
    :param data_folder: folder where to save the files gz
    :param filename: name of the file
    :param mode: read/write the archive
    :param ending: gz or csv
    :return: open the csv files with the table for the user
    """
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    if ending == "gz":
        # 't' is for the text mode
        user_csv = gzip.open(os.path.join(data_folder, filename), mode+'t', encoding='utf-8', errors='ignore',
                             newline='')
    elif ending == "csv":
        user_csv = open(os.path.join(data_folder, filename), mode, encoding='utf-8', errors='ignore', newline='')
    else:
        raise Exception("Unknown format of the file")

    return user_csv


def write_tweet_in_json(tweet, f_json):
    """
    Function to write a tweet in a json file
    :param tweet:
    :param f_json:
    :return: write tweet in json
    """

    # save tweets in a json file
    json_str = '{}\n'.format(json.dumps(tweet))
    json_bytes = json_str
    f_json.write(json_bytes)


####################################################################################################################
# useful functions


def download_new_user(unfiltered_user_path, archive_name, min_tweet_date, max_tweet_date, months_back, lang, capital,
                      threshold_lang, disk_out, last_user, chunk_users):
    # download_archive.download_new_user('unique_users.csv', 'archive_01_08_2018', "2018-01-01", "2018-12-31", 1, "it",
    # "rome", 2, '', None, 0, 50)

    """
    # for each user check if it still exists, if the selection criteria are still satisfied in the first page of the
    # archive (last 200 tweets), if the user is still active in the last month, if all the 3 criteria are satisfied,
    # then the user has field selected = True, all the its tweets in the time interval are saved in a json file.
    :param unfiltered_user_path: (str) path of the csv file with the list of unfiltered selected users
    :param archive_name: (str) name of the compressed file with the archive
    :param min_tweet_date: (str) lower limit to save the tweets (to be changed each month to speed up the code)
    :param max_tweet_date: (str) upper limit to save the tweets
    :param months_back: (int) number of monts to go dackwards to consider the user as inactive if no activity (1)
    :param lang: (str) selection parameter ("it")
    :param capital: (str) selection parameter ("rome")
    :param threshold_lang: (int) selection parameter: minimum lever of consistency for the language that is required.
    :param disk_out: (str) path of the disk_out to save json.gz
    :param last_user: (int) plan B if no csv file. Insert user id (as int) to start retrieve data from there
    :param chunk_users: (int) number of users of the chunk files
    :return: json and csv.gz files with the tweets and the tables of the selected users between min_tweet_date and
    max_tweet_date
    """

    unfiltered_users_retrieval_date = list_no_retrieved_users(
        unfiltered_user_path, archive_name + '.json', disk_out, last_user)
    unfiltered_users = [row[0] for row in unfiltered_users_retrieval_date]

    processed_users = 0
    unique_active_users = 0
    min_tweet_date = parser.parse(min_tweet_date)
    max_tweet_date = parser.parse(max_tweet_date)
    n_unfiltered_users = len(unfiltered_users)
    f_json = open(os.path.join(disk_out, archive_name + '.json'), 'a', encoding='utf-8')
    filename = archive_name + '_user.csv'
    data_folder = os.path.join(disk_out, archive_name + '_' + "data")
    user_csv = csv.writer(open_file(data_folder, filename, 'w', 'csv'), delimiter=',')
    data_folder = os.path.join(disk_out, archive_name + '_' + "data")

    for user_str in unfiltered_users_retrieval_date:

        selected_criteria = False  # user not yet selected for criteria
        saved_user = False  # user saved in csv
        processed_users += 1
        user = int(user_str[0])
        retrieval_date = user_str[1]
        print('User: '+str(user))

        # get the whole archive as list between min_tweet_date and max_tweet_date
        obtained_tweets = False
        while obtained_tweets is False:  # get the archive if no connection errors otherwise repeat
            try:
                archive = list(rest.fetch_user_archive(user, 16, min_tweet_date))  # get pages of the archive as list
                obtained_tweets = True
            except requests.exceptions.ConnectionError as errc:
                print(errc)
                obtained_tweets = False
            except ValueError as errc:
                print(errc)
                obtained_tweets = False

        # FILTER
        # if the profile still exists
        if type(archive) == list and len(archive) > 0 and ("errors", "error") not in archive:

            # if the user is still active and more than 1 tweet
            print(len(archive))
            archive_first_page = archive[0]
            n_tweets_first_page = len(archive_first_page)

            if n_tweets_first_page > 1:

                tweet_1 = archive_first_page[0]
                date_first_tweet_date = parser.parse(tweet_1['created_at'])  # as date
                date_first_tweet_str = date_first_tweet_date.strftime("%Y-%m-%d")
                date_first_tweet = datetime.datetime.strptime(date_first_tweet_str, "%Y-%m-%d").date()  # as str
                month_ago = (datetime.datetime.today() - relativedelta(months=months_back)).date()  # as str
                date_last_tweet = parser.parse(archive_first_page[n_tweets_first_page - 1]['created_at'])

                if date_first_tweet >= month_ago:

                    time_interval = date_first_tweet_date - date_last_tweet  # in days
                    if time_interval.total_seconds != 0:
                        frequency_user = n_tweets_first_page/time_interval.total_seconds()

                        # check first page
                        first_full_page = False
                        for page in archive:  # for each page, from the most recent to the oldest
                            n_tweets_pag = len(page)
                            if n_tweets_pag > 0 and "error" not in page:  # if no errors in pages
                                if first_full_page is False:
                                    # consistency
                                    nationality_user = [0, 0, 0, 0]  # percentage of the satisfied criteria in time

                                    #  characteristics of each tweet
                                    for tweet in archive_first_page:

                                        language = tweet["lang"]

                                        if tweet["place"] is not None:
                                            place = tweet["place"]["country_code"].lower()
                                        else:
                                            place = None

                                        if tweet["user"]["lang"] is not None:
                                            interface = tweet["user"]["lang"].lower()
                                        else:
                                            interface = None

                                        if tweet["user"]["time_zone"] is not None:
                                            time_zone = tweet["user"]["time_zone"].lower()
                                        else:
                                            time_zone = None

                                        #  if the national criteria are still valid
                                        if interface == lang or time_zone == capital or language == lang or \
                                                place == lang:

                                            if interface == lang:
                                                nationality_user[0] += 1
                                            if time_zone == capital:
                                                nationality_user[1] += 1
                                            if language == lang:
                                                nationality_user[2] += 1
                                            if place == lang:
                                                nationality_user[3] += 1

                                    if nationality_user[0] > 0 or nationality_user[1] > 0 or \
                                            nationality_user[2] > threshold_lang or nationality_user[3] > 0:
                                        selected_criteria = True  # user selected: criteria satisfied
                                    # save the array in str
                                    nationality_user = str([l/n_tweets_first_page for l in nationality_user])
                                    retrieval_date = datetime.datetime.strptime(retrieval_date, "%Y-%m-%d")
                                    first_full_page = True

                                # save user in csv, if criteria satisfied
                                if selected_criteria is True:
                                    #  save user in the csv
                                    if saved_user is False:
                                        unique_active_users += 1
                                        user_l = user_from_tweet(tweet_1, retrieval_date, frequency_user,
                                                                 nationality_user)
                                        user_csv.writerow(user_l)
                                        saved_user = True

                                    # save also its tweets
                                    # if the page has tweets in the desired time interval (no time zone)
                                    date_first_tweet = parser.parse(page[0]['created_at']).replace(tzinfo=None)
                                    date_last_tweet = parser.parse(page[n_tweets_pag-1]['created_at']).replace(
                                        tzinfo=None)
                                    # pages with some tweets in the interval
                                    if date_first_tweet >= min_tweet_date and date_last_tweet <= max_tweet_date:
                                        # if all the tweets are in the interval
                                        if date_last_tweet >= min_tweet_date and date_first_tweet <= max_tweet_date:
                                            # no extra checks on the tweets and save tweets of the page in json
                                            for tweet in page:

                                                # save tweets in a json file
                                                write_tweet_in_json(tweet, f_json)
                                        else:
                                            for tweet in page:  # check 1 by one and save tweets of the page in json
                                                date_tweet = parser.parse(tweet['created_at']).replace(tzinfo=None)
                                                if min_tweet_date <= date_tweet <= max_tweet_date:

                                                    # save tweets in a json file and
                                                    write_tweet_in_json(tweet, f_json)

        # timer to see what I am doing
        if processed_users % 10 == 0:
            print("---------------------------------------------------------------------------------")
            print("user:" + str(user))
            print("percentage of processed users:" + str(processed_users / n_unfiltered_users))
            print("processed users:" + str(processed_users))

        # do chunk backup copies each tot users: split
        if processed_users % chunk_users == 0:
            user_csv.close()  # close file
            os.rename(filename, 'backup_'+filename)  # do a backup copy

            # open file and start writing on it again
            user_csv = csv.writer(open_file(data_folder, filename, 'w', 'csv'), delimiter=',')

    user_csv.close()
    f_json.close()


def download_archive_json(filtered_user_path, archive_name, min_tweet_date, max_tweet_date, disk_out, last_user):
    # download_archive.download_archive_json('unique_frequent_users.csv', 'archive_frequent_users_2018', "2018-01-01",
    # "2018-12-31", '', None)

    """
    Function to just download archive as json
    :param filtered_user_path: (str) path of the csv file with the list of filtered selected users
    :param archive_name: (str) name of the compressed file with the archive
    :param min_tweet_date: (str) lower limit to save the tweets (to be changed each month to speed up the code)
    :param max_tweet_date: (str) upper limit to save the tweets
    :param disk_out: (str) path of the disk_out to save json
    :param last_user: (int) plan B if no json file. Insert user id to start retrieve data from there
    :return: json with the tweets between min_tweet_date and max_tweet_date
    """

    no_retrieved_users = [row[0] for row in
                          list_no_retrieved_users(filtered_user_path, archive_name + '.json', disk_out, last_user)]

    processed_users, too_frequent_users = 0, 0
    user_archive_list = None
    min_tweet_date = parser.parse(min_tweet_date)
    max_tweet_date = parser.parse(max_tweet_date)
    tot_users = len(no_retrieved_users)

    with open(os.path.join(disk_out, "too_frequent_users.csv"), "a") as tf:
        f_json = gzip.open(os.path.join(disk_out, archive_name + '.json.gz'), 'at', encoding='utf-8')
        for user_str in no_retrieved_users:

            processed_users += 1
            user = int(user_str)
            print('User: '+str(user_str))

            # get the whole archive as list between min_tweet_date and max_tweet_date
            obtained_tweets = False
            while obtained_tweets is False:  # get the archive if no connection errors otherwhise repeat
                try:
                    user_archive_list = list(rest.fetch_user_archive(user, 16, min_tweet_date))
                    obtained_tweets = True
                except requests.exceptions.ConnectionError as errc:
                    print(errc)
                    obtained_tweets = False
                except ValueError as errc:
                    print(errc)
                    obtained_tweets = False

            len_archive = len(user_archive_list)

            # FILTER
            # if the profile still exists (sanity check)
            if type(user_archive_list) == list and len_archive > 0 and ("errors", "error") not in user_archive_list:
                last_page = user_archive_list[len_archive - 1]
                if len(last_page) > 0:  # date_last_tweet as date
                    date_last_tweet = parser.parse(last_page[len(last_page) - 1]['created_at']).replace(tzinfo=None)
                    if len(user_archive_list) >= 16:  # too frequent users are written in a file
                        tf.write("{0},{1}\n".format(user_str, date_last_tweet))
                        too_frequent_users += 1

                for page in user_archive_list:  # for each page, from the most recent to the oldest
                    n_tweets_pag = len(page)
                    if n_tweets_pag > 0 and "error" not in page:  # if no errors in pages
                        date_first_tweet = parser.parse(page[0]['created_at']).replace(tzinfo=None)
                        date_last_tweet = parser.parse(page[n_tweets_pag-1]['created_at']).replace(tzinfo=None)
                        # if the page has tweets in the desired time interval (no time zone)
                        # pages with some tweets in the interval
                        if date_first_tweet >= min_tweet_date and date_last_tweet <= max_tweet_date:
                            # if all the tweets are in the interval
                            if date_last_tweet >= min_tweet_date and date_first_tweet <= max_tweet_date:
                                # no extra checks on the tweets and save tweets of the page in json
                                for tweet in page:
                                    # save tweets in a json file
                                    json_str = '{}\n'.format(json.dumps(tweet))
                                    f_json.write(json_str)
                            else:
                                for tweet in page:  # check 1 by one and save tweets of the page in json
                                    date_tweet = parser.parse(tweet['created_at']).replace(tzinfo=None)
                                    if min_tweet_date <= date_tweet <= max_tweet_date:
                                        # save tweets in a json file
                                        json_str = '{}\n'.format(json.dumps(tweet))
                                        f_json.write(json_str)

            # timer to see what I am doing
            if processed_users % 10 == 0:
                print("---------------------------------------------------------------------------------")
                print("user:" + user_str)
                print("percentage of processed users:" + str(processed_users / tot_users))
                print("processed users:" + str(processed_users))
                print("too frequent users:" + str(too_frequent_users))


def rename_chunks(path_folder_sorted_merged):

    """
    Function to rename chunks according the time interval: the first and last id of the tweets contained in the chunk
    :param path_folder_sorted_merged: (str) path of the folder containing all the merged chunks to be renamed
    :return: renamed chunks according to the first and last id of the tweets contained in the chunk
    """

    print(os.listdir(path_folder_sorted_merged))
    for file in os.listdir(path_folder_sorted_merged):
        print(file)
        chunk = gzip.open(os.path.join(path_folder_sorted_merged, file), 'rt', encoding="utf-8")
        reader_chunk = list(csv.reader(x.replace('\0', '') for x in chunk))
        try:  # first line if no index error
            first_date = reader_chunk[0][3]  # date in tot seconds of the first tweet
            chunk.close()
        except IndexError:  # white rows in file, delete blank lines
            chunk.close()
            os.remove(os.path.join(path_folder_sorted_merged, file))
            with gzip.open(os.path.join(path_folder_sorted_merged, file), 'wt', encoding="utf-8") as out:
                writer = csv.writer(out)
                for line in reader_chunk:
                    if line:  # exclude white lines
                        writer.writerow(line)
            chunk = gzip.open(os.path.join(path_folder_sorted_merged, file), 'rt', encoding="utf-8")
            reader_chunk = list(csv.reader(chunk))
            chunk.close()
            first_date = reader_chunk[0][3]
        last_date = None
        for index in range(-1, -len(reader_chunk), -1):  # read backward
            print(index)
            try:
                last_date = reader_chunk[index][3]  # date in tot seconds of the last tweet
                print(last_date)
                break
            except IndexError:  # if blank line go on till find last line not empty
                print('IndexError')
                pass
        reader_chunk = None  # clear memory for next chunk

        # rename the chunk with last/first date with tot seconds
        new_file_name = "tweet_"+str(first_date)+"-"+str(last_date)+".csv.gz"
        os.renames(os.path.join(path_folder_sorted_merged, file),
                   os.path.join(path_folder_sorted_merged, new_file_name))
        print(new_file_name)
