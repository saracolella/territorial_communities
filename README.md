# territorial_communities
Sociology has identified ”territorial communities” as important elements in people’s social identity (Anderson, 1991). What ”glues” these territorial communities together are for example language, shared cultural heritage, or common concerns about the same events. Spatial proximity and direct interaction can but not necessarily have to be constitution elements of territorial communities. For example, children of immigrants might be part of territorial communities by sharing language and cultural heritage without actually living in the old country or by interacting with people living there (Fortier, 1998). Existing community detection methods falls short of identifying territorial communities. Their reliance on geographical locations or direct interaction patterns only map parts of territorial communities while systematically missing the members not covered by these selection criteria. We propose a community detection approach on the social media service Twitter that for the first time accounts for the complete breadth of the sociological concept of territorial communities (Newby and Bell, 1974). We show that the existing techniques, focusing on direct interaction ties or spatial proximity, considerably underestimate the size of territorial communities. In contrast, our approach account for a broader concept of territorial community by taking into account Twitter users invisible to geographical or interaction based selection present in the existing literature, but that belong to a shared cultural space, such as emigrants and children of immigrants. These users express their connection with the a territorial community on the platform through several signals, such as the language of their posts, and through a self-directed public performance of identity. We implemented our approach for the Italian community and detected other user categories beside the ones characterized by spatial proximity (the locals, the majority of total user selection). Around 9% of the community detection belonged to categories that would have not been selected with the existing methods, the largest was composed by the Italians abroad (around 5% of the community detection) followed by local football team supporters, foreign companies who have a business relationship with the countries as well as children of emigrants and immigrants. Furthermore, our methodology shows the value of theory-driven operationalization. As we decide on which signals to include in our community detection approach based on concepts developed in sociological theory our approach illustrates the potential and challenges of doing data science following well-established sociological theory developed in a context diverging radically from the environment of digital communication. 
The draft of the journal paper on the matter is available in this folder as well as the relative python code.

########################################################################################################################################

Instructions to select a list of Twitter users that are part of the territorial community of a certain nation, obtain their archive and analyze their behaviour.

########################################################################################################################################Select users

PYTHON

File user_selection.py

0.1) Download the Twitter "Spritzer" archive from the internet archive : "https://archive.org/details/twitterstream"

0.2) Untar the files of the Twitter "Spritzer" archive. untar

0.3) Unzip bz2 just the tweets related to a county for each month and put them on an other path in a json in the folder "json_light_users". unzip

0.4) Select a list of unique users from the "light" json files of the internet archive. select_user

0.5) Obtain a list of users that in a cvs, filter them, if they exist, according to their activity and the selection criteria. save_user_archive_list

File download_archive_json.py

0.6) Check if the criteria of "Italianess" and of activity (tweeted in the last month) are still satisfied. Expecially if the consistency in the use of the languge is over the threshold.
In that case save the users profile in a csv and the archive in a json. download_new_user

##################################################################################################################################################################################################
Create archive

PYTHON

File dataset_management.py

1.1) Once that the user selection is complete, at certain time intervals save the archive again without doing any check. download_archive_json

BASH

File jq-chunker_csv.sh

1.2) Obtain from the archive in json format just the fields of interest of each tweet of the archive and save them in zipped csv files dividing them in chunks that can fit into memory.

1.3) Join in the folder "merged_sorted_chunks" also the previous zipped csv files with the previous archive.

File jq-chunker_merger_sorted.sh

1.4) Merge and sort the chunks of all the zipped csv files such to have an ordered, unique and complete archive with chunks that fit into memory.

PYTHON

File download_archive_json.py

1.5) Rename chunks according the time interval: the first and last id of the tweets contained in the chunk. rename_chunks

##################################################################################################################################################################################################
Analyze users behaviour

PYTHON

File dataset_management.py

2.1) Analyze users behaviour: tweets and retweets and tw and retweets per user per hour, per day of the week and per month. dataset_descriptive

