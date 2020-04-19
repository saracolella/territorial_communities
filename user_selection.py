import os
import bz2
import json
import re
from pathlib import Path
from dateutil import parser
import tarfile
from operator import itemgetter
from dateutil.relativedelta import relativedelta
import datetime
from collections import Counter
import fields_of_interest
import rest

# Select users according to activity and selection criteria.
# untar: Untar the files of the Twitter "Spritzer" archive
# unzip: Unzip bz2 just the tweets related to a county for each month and put them in json in folder "json_light_users"
# select_user: Select a list of unique users from the "light" json files of the internet archive
# save_user_archive_list: Obtain a list of users that in a cvs, filter them, if they exist, according to their activity
#                         and the selection criteria


###################################################################################################################
# setting functions


def unzip_bz2(disk_in, disk_out, month, year, capital, lang):

    # unzip2_bz2("D:", "F:", "04", "2016", "rome", "it")
    # all inputs in small letters, in the code we put the right format
    # lang in small letters, capital with the first one capital and place with all capital letters

    compiled_patt_lang = b"\"lang\":\"" + bytes(lang, encoding="UTF-8") + b"\""
    compiled_patt_country_code = b"\"country_code\":\"" + bytes(lang.upper(), encoding="UTF-8") + b"\""
    compiled_patt_time_zone = b"\"time_zone\":\"" + bytes(capital.title(), encoding="UTF-8") + b"\""
    compiled_pattern = re.compile(compiled_patt_lang+b"|"+compiled_patt_country_code + b"|"+compiled_patt_time_zone)

    if os.path.exists(os.path.join(disk_in, year, month)):
        with open(os.path.join(disk_out, month+"-"+year+"_light_users.json"), 'wb') as out:  # open the file
            path_list = Path(os.path.join(disk_in, year, month)).glob('**/*.json.bz2')
            for file_path in path_list:
                if file_path == '**/00/59.json.bz2':
                    print(file_path)
                with bz2.BZ2File(file_path) as zipfile:  # open the file
                    for line in zipfile.readlines():  # get the decompressed line
                        # filter
                        if re.search(compiled_pattern, line):
                            out.write(line + b"\n")  # write the light uncompressed file

####################################################################################################################
# useful functions


def untar(disk_in, disk_out):
    """
    Function to untar all the files in a certain location, but not the ones in the subfolders, for that add: **/ before
    :param disk_in: (str) path of the folder/disk of tar files "F:"
    :param disk_out: (str) path of disk_out of where to save the untarred files "D:"
    :return: untar all the files in a certain location
    """

    pathlist = Path(disk_in).glob('*.tar')
    for filepath in pathlist:
        print(filepath)
        tar = tarfile.open(filepath, "r")
        tar.extractall(disk_out)
        tar.close()
        os.remove(filepath)


def unzip(disk_in, disk_out, capital, lang):
    """
    Function to unzip just the tweets related to a county for each month and put them in a json
    :param disk_in: (str) path of the folder/disk of the zipped bz2 files: "D:"
    :param disk_out: (str) path of disk_out of where to save the unzipped files "F:"
    :param capital: (str) capital of the nation of analysis "rome"
    :param lang: (str) language of the nation of analysis "it"
    :return: unzip just the tweets related to a county for each month and put them on an other path in a json
             in the folder "json_light_users"
    """

    # unzip just the tweets related to a county for each month and put them on an other path

    years_path = [[f.path, f.name] for f in os.scandir(disk_in) if f.is_dir()]
    # years_path = next(os.walk(disk_in+'/.'))[1]
    print(years_path)
    if not os.path.exists(os.path.join(disk_out, "json_light_users")):
        os.makedirs(os.path.join(disk_out, "json_light_users"))
    for y in years_path:
        months_path = [[f.path, f.name] for f in os.scandir(y[0]) if f.is_dir()]
        print(months_path)
        if not os.path.exists(os.path.join(disk_out, "json_light_users", y[1])):
            os.makedirs(os.path.join(disk_out, "json_light_users", y[1]))
        for m in months_path:
            print(m)
            if not os.path.exists(os.path.join(disk_out, "json_light_users", y[1], m[1]+"-"+y[1]+"_light_users.json")):
                unzip_bz2(disk_in, os.path.join(disk_out, "json_light_users", y[1]), m[1], y[1], capital, lang)


def select_user(path_folder_in, disk_out, lang, capital):
    """
    Function to select a list of unique users from the "light" json files of the internet archive if they are from a
    certain nationality: user["interface"]/user["time_zone"]/tweet["language"]/tweet["place"]
    :param path_folder_in: (str) path of the folder with the json with the tweets related to country "json_light_users"
    :param disk_out: (str) path of the disk of where we want to save the list of unique users
    :param lang: (str) language of the nation of analysis "it"
    :param capital: (str) capital of the nation of analysis "rome"
    :return: csv with unique users with a certain nationality: user "id" and the date of the tweet
    """

    selected_users = {}  # set dictionary
    years_path = [[f.path, f.name] for f in os.scandir(path_folder_in) if f.is_dir()]
    years_path = sorted(years_path, key=itemgetter(1), reverse=True)
    with open(os.path.join(disk_out, 'unique_users.csv'), "w") as u:

        u.write("user, retrival date\n")
        for y in years_path:
            year = y[1]
            # all the json files corresponding to the months for the folder of the year
            month_paths = [[os.path.join(y[0], f), f] for f in os.listdir(y[0]) if f.endswith('.json')]
            month_paths = sorted(month_paths, key=itemgetter(1), reverse=True)
            for m in month_paths:
                month = m[1][:2]
                u_month = 0
                for line in open(m[0], "rb"):
                    try:
                        line = json.loads(line.decode('utf-8'))

                        tweet = fields_of_interest.create_tweet_from_line(line)
                        user = fields_of_interest.create_user_from_line(line)

                        # users
                        if user["interface"] == lang or user["time_zone"] == capital or tweet["language"] == lang or \
                           tweet["place"] == lang:

                            # unique users
                            if user["user_id"] not in selected_users:
                                u_month += 1
                                # cvs with just ids of unique users
                                selected_users[user["user_id"]] = tweet["date"]

                                u.write("{0},{1}\n".format(user["user_id"], tweet["date"]))

                    except ValueError:
                        pass
                print("new users {0}, {1}".format(month+"/"+year, u_month))


def save_user_archive_list(unfiltered_user_path, months_back, lang, capital):
    """
    Function to fetch tweets, at max 200x200 for a list of users that in a cvs, filter them, if they exist, according to
    their activity and the selection criteria: user["interface"]/user["time_zone"]/tweet["language"]/tweet["place"]
    and then save them in a cvs: 'filtered_users.csv' and some other analysis csv
    :param unfiltered_user_path:(str) path of the file that contains the list of unique users "unique_users.cvs"
    :param months_back: (int) if the user has not tweeted in months_back months back in time then he is not active
    :param lang: (str) language of the nation of analysis "it"
    :param capital: (str) capital of the nation of analysis "rome"
    :return: 1) 'filtered_users.csv' with ids of the users that satisfy the activity and selection criteria
             2) 'filtered_time_users.csv' with ids of the selected users with the retrieval date
             3) 'unique_active_users_time.csv' with number of saved
             4) 'nationality_users.csv' with parameters of selection:
                 n_tweets, user_interface, user_time_zone, tweet_language, tweet_place
             5) 'perc_overlap.csv'/'abs_overlap.csv' impact of each selection criteria
             6) 'frequency_users.csv' frequency of the users
    """

    with open('filtered_users.csv', "w") as f, open('filtered_time_users.csv', "w") as ft, \
            open('unique_active_users_time.csv', "w") as t, open('nationality_users.csv', "w") as n, \
            open('abs_overlap.csv', "w") as a, open('perc_overlap.csv', "w") as p, \
            open('frequency_users.csv', "w") as freq:

        dict_retrival_date = {}
        unique_active_users = []
        frequency = []
        perc_users = {}
        u_interface = ['user_interface', 0, 0, 0, 0]  # user_interface, &user_time_zone, &tweet_language, &tweet_place
        u_time_zone = ['user_time_zone', 0, 0, 0, 0]  # &user_interface, user_time_zone, &tweet_language, &tweet_place
        u_language = ['tweet_language', 0, 0, 0, 0]  # &user_interface, &user_time_zone, tweet_language, &tweet_place
        u_place = ['tweet_place', 0, 0, 0, 0]  # &user_interface, &user_time_zone, &tweet_language, tweet_place
        n.write("user_id, n_tweets, user_interface, user_time_zone, tweet_language, tweet_place\n")
        freq.write("days interval, users' count\n")

        for user, retrival_date in unfiltered_user_path:

            perc_users[user] = [0, 0, 0, 0, 0]
            archive_first_page = rest.fetch_user_archive(user, 1, 200)  # get the first page of the archive
            n_tweets_user = 0

            # filter
            for tweets in archive_first_page:

                # if the profile still exists
                if "errors" not in archive_first_page:

                    date_first_tweet = parser.parse(tweets[0]['created_at']).strftime("%Y-%m-%d")
                    date_first_tweet_str = datetime.datetime.strptime(date_first_tweet, "%Y-%m-%d").date()
                    month_ago = (datetime.datetime.today() - relativedelta(months=months_back)).date()

                    for tweet in tweets:
                        # if the user is still active
                        if date_first_tweet_str >= month_ago:
                            n_tweets_user += 1

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

                            # analyse all their tweets
                            if interface == lang:
                                perc_users[user][0] += 1
                            if time_zone == capital:
                                perc_users[user][1] += 1
                            if language == lang:
                                perc_users[user][2] += 1
                            if place == lang:
                                perc_users[user][3] += 1

                            # if the national criteria are still valid
                            if interface == lang or time_zone == capital or language == lang or place == lang:

                                # selection phase: unique users
                                if user not in unique_active_users:
                                    f.write("{0}\n".format(user))
                                    ft.write("{0},{1}\n".format(user, retrival_date))

                                    if retrival_date in dict_retrival_date:
                                        dict_retrival_date[retrival_date] += 1
                                    else:
                                        dict_retrival_date[retrival_date] = 1

                                    if interface == lang:
                                        u_interface[1] += 1
                                        if time_zone == capital:
                                            u_interface[2] += 1
                                        if language == lang:
                                            u_interface[3] += 1
                                        if place == lang:
                                            u_interface[4] += 1
                                    if time_zone == capital:
                                        u_time_zone[2] += 1
                                        if interface == lang:
                                            u_time_zone[1] += 1
                                        if language == lang:
                                            u_time_zone[3] += 1
                                        if place == lang:
                                            u_time_zone[4] += 1
                                    if language == lang:
                                        u_language[3] += 1
                                        if interface == lang:
                                            u_language[1] += 1
                                        if time_zone == capital:
                                            u_language[2] += 1
                                        if place == lang:
                                            u_language[4] += 1
                                    if place == lang:
                                        u_place[4] += 1
                                        if interface == lang:
                                            u_place[1] += 1
                                        if time_zone == capital:
                                            u_place[2] += 1
                                        if language == lang:
                                            u_place[3] += 1

                                    unique_active_users.append(user)
                            else:
                                break
                    # consider all the tweets for the nationality on the use, consistency, but print just the selected
                    if user in unique_active_users:
                        date_first_tweet = parser.parse(tweets[0]['created_at'])
                        date_last_tweet = parser.parse(tweets[199]['created_at'])
                        time_interval = date_first_tweet - date_last_tweet  # in days
                        frequency.append(time_interval.days)

                        perc_users[user] = [l / n_tweets_user for l in perc_users[user]]
                        perc_users[user][4] = n_tweets_user
                        n.write("{0},{1},{2},{3},{4},{5}\n".format(user, perc_users[user][4], perc_users[user][0],
                                                                   perc_users[user][1], perc_users[user][2],
                                                                   perc_users[user][3]))

        print(dict_retrival_date)

        u_tot = 0
        for key in dict_retrival_date:
            u_tot += dict_retrival_date[key]
            t.write("{0},{1}\n".format(key, dict_retrival_date[key]))

        for item in Counter(frequency).items():
            freq.write("{0},{1}\n".format(item[0], item[1]))

        # summary: overlap tables
        a.write("{0}, user_interface, user_time_zone, tweet_language, tweet_place\n".format(u_tot))
        a.write("{0},{1},{2},{3},{4}\n".format(u_interface[0], u_interface[1], u_interface[2], u_interface[3],
                                               u_interface[4]))
        a.write("{0},{1},{2},{3},{4}\n".format(u_time_zone[0], u_time_zone[1], u_time_zone[2], u_time_zone[3],
                                               u_time_zone[4]))
        a.write("{0},{1},{2},{3},{4}\n".format(u_language[0], u_language[1], u_language[2], u_language[3],
                                               u_language[4]))
        a.write("{0},{1},{2},{3},{4}\n".format(u_place[0], u_place[1], u_place[2], u_place[3], u_place[4]))
        p.write(" , user_interface, user_time_zone, tweet_language, tweet_place\n")
        if u_interface[1] != 0:
            p.write("{0},{1},{2},{3},{4}\n".format(u_interface[0], u_interface[1] / u_tot,
                                                   u_interface[2] / u_interface[1], u_interface[3] / u_interface[1],
                                                   u_interface[4] / u_interface[1]))
        else:
            p.write("{0},{1},{2},{3},{4}\n".format(u_interface[0], u_interface[1] / u_tot, None, None, None))
        if u_time_zone[2] != 0:
            p.write("{0},{1},{2},{3},{4}\n".format(u_time_zone[0], u_time_zone[1] / u_time_zone[2],
                                                   u_time_zone[2] / u_tot, u_time_zone[3] / u_time_zone[2],
                                                   u_time_zone[4] / u_time_zone[2]))
        else:
            p.write("{0},{1},{2},{3},{4}\n".format(u_time_zone[0], None, u_time_zone[2] / u_tot, None, None))
        if u_language[3] != 0:
            p.write("{0},{1},{2},{3},{4}\n".format(u_language[0], u_language[1] / u_language[3],
                                                   u_language[2] / u_language[3], u_language[3] / u_tot,
                                                   u_language[4] / u_language[3]))
        else:
            p.write("{0},{1},{2},{3},{4}\n".format(u_language[0], None, None, u_language[3] / u_tot, None))
        if u_place[4] != 0:
            p.write("{0},{1},{2},{3},{4}\n".format(u_place[0], u_place[1] / u_place[4], u_place[2] / u_place[4],
                                                   u_place[3] / u_place[4], u_place[4] / u_tot))
        else:
            p.write("{0},{1},{2},{3},{4}\n".format(u_place[0], None, None, None, u_place[4] / u_tot))
