#!/usr/bin/env bash
shopt -s nullglob
set -o nounset -o errexit  # if error break suddenly

mkdir -p merged_sorted_chunks  # make directory if it doesn't exists

# fiels of the tweets I am interested in:
# id_str / user.id_str / .text / .created_at / tweet_localization / .lang / tags (lowcase) / urls / retweet_id_str
# id_str because if too long integers then they get rounded
cols=(
	.id_str
	.user.id_str
	'((.full_text // .text) | @json)' # might have newlines inside
	'(.created_at | strptime("%a %b %d %H:%M:%S +0000 %Y") | mktime)'
	.lang
	.place.country_code
	'([.entities.hashtags[]? | .text | ascii_downcase] | unique | join(":"))'
	'([.entities.urls[]? | .expanded_url] | unique | join(":"))'
	.retweeted_status.id_str
)

printf -v filter '%s,' "${cols[@]}"
filter="[${filter%,}] | @csv"
echo "$filter"

function mysort {
  local f="merged_sorted_chunks/chunk_${FILE}.csv.gz"
  echo "$f" >&2
  gzip > "$f"
}
export -f mysort

# arguments: 1)n_lines for each chunk 2) paths of the files I have to merge
n_lines="$1"; shift  # count the arguments from the second for the paths

# check if the argument 1 is a number
if [[ ! "${n_lines##*[^0-9]*}" ]]; then
	echo 'n_lines is not a number'
	exit 1
fi

# print the arguments
echo "n_lines: $n_lines" >&2
echo "files names: $@" >&2

for file in "$@"; do  # unzip and do not stop if a file is corrupted 
	if ! gunzip --stdout "$file"; then
		echo 'Corrupted file: ' "$file" >&2
	else
		echo 'Complete file: ' "$file" >&2
	fi |
	jq --raw-output "$filter" |  # filder the fiels of the tweet
	pv -l
done |# split the file such that the chunks stay in memory: n_lines=5000000
split -d --suffix-length=4 \
--lines="$n_lines" --filter='mysort' '-' ''


# DESCRIPTION:
# Obtain from the archive in json format just the fields of interest of each tweet of the archive and save them in csv files dividing them in chunks 

# On BASH run:
# to convert file to Unix format
# dos2unix jq-chunker.sh
# to see which files we are chunking:
# echo *.json.gz
# to make executable this file:
# chmod +x jq-chunker.sh
# # to chunk all the json files in the folder
# ./jq-chunker.sh 5000000 *.json.gz
