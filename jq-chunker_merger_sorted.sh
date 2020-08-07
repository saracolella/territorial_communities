#!/usr/bin/env bash
shopt -s nullglob
set -o nounset -o errexit  # if error break suddenly

mkdir -p merged_sorted_chunks_sorted  # make directory if it doesn't exists

printf -v filter '%s,' "${cols[@]}"
filter="[${filter%,}] | @csv"
echo "$filter"

function mysort {
  local f="merged_sorted_chunks_sorted/chunk_${FILE}.csv.gz"
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

gunzip --stdout "$@"| # unzip
sort -u -t, -V --key=1 --parallel=2 | # sort and merge without duplicates
# split the merged file such that the chunks stay in memory: n_lines=5000000
split -d --suffix-length=4 \
--lines="$n_lines" --filter='mysort' '-' ''


# DESCRIPTION:
# Merge and sort the chunks of all the zipped csv files such to have an ordered, unique and complete archive with chunks that fit into memory 

# On BASH run:
# to convert file to Unix format
# dos2unix jq-chunker_merger_sorted.sh
# to see which files we are merging, sorting and chunking:
# echo /*.csv.gz
# to make executable this file:
# chmod +x jq-chunker_merger_sorted.sh
# # to sort, merge and chunk all the ziipes csv files in the folder
# ./jq-chunker_merger_sorted.sh 5000000 *.csv.gz
