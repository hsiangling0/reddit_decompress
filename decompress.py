import os, re
import zstandard as zstd
import csv, json, datetime
from typing import Iterable
import pathlib
import sys

# FilePath = "submissions/"
submission = ['post_id', 'subreddit', 'post_title', 'post_content', 'post_score', 'post_create']
comment = ['post_id', 'comment_content', 'comment_score', 'comment_create']
target_field = [[
    'politics', 'PoliticalDiscussion', 'unpopularopinion', 'Conservative', 'PoliticalHumor'
], [
    'nba',
    'sports',
    'nfl',
    'PremierLeague',
    'formula1',
], ['Economics', 'AskEconomics', 'inflation', 'economicCollapse', 'badeconomics']]


def contextClear(s):
    newS = ""
    url_pattern = re.compile(r'(\[\S+\])?(\(?https?://\S+|www\.\S+\)?)|(/?r/\S+)|[\n]+|&\S+;',
                             flags=re.MULTILINE)

    if s not in {'', '[deleted]', '[removed]'}:
        newS = url_pattern.sub('', s)
    if re.search('[a-zA-Z]', newS):
        return newS
    else:
        return ""


def decode(reader, chunk_size, max_window_size, prev=None, byte=0):
    chunk = reader.read(chunk_size)
    byte += chunk_size
    if prev is not None:
        chunk = prev + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if byte > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {byte:,} bytes")
        return decode(reader, chunk_size, max_window_size, chunk, byte)


def decompress(file):
    with open(file, 'rb') as f:
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        buffer = ''
        with dctx.stream_reader(f) as reader:
            while True:
                chunk = decode(reader, 2**27, (2**29) * 2)
                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")
                for line in lines[:-1]:
                    yield line
                    buffer = lines[-1]
            reader.close()


def DataClean(file):
    if os.path.isfile(file) and file.endswith('zst'):
        is_submission = False
        if 'RS' in file:
            attr = submission
            is_submission = True
        else:
            attr = comment
        path, filename = os.path.split(file)
        dir = os.path.join(path, 'decompress')
        # os.mkdir(dir)
        pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
        politics = dir + '/' + filename[:-4] + '-politics.csv'
        sports = dir + '/' + filename[:-4] + '-sports.csv'
        economics = dir + '/' + filename[:-4] + '-economics.csv'

        with open(politics, 'w', newline='') as p, open(sports, 'w',
                                                        newline='') as s, open(economics,
                                                                               'w',
                                                                               newline='') as e:
            writer_p = csv.writer(p)
            writer_s = csv.writer(s)
            writer_e = csv.writer(e)
            writer_p.writerow(attr)
            writer_s.writerow(attr)
            writer_e.writerow(attr)
            try:
                for lines in decompress(file):
                    item = json.loads(lines)
                    output = []
                    if item['subreddit'] in target_field[0]:
                        writer = writer_p
                    elif item['subreddit'] in target_field[1]:
                        writer = writer_s
                    elif item['subreddit'] in target_field[2]:
                        writer = writer_e
                    else:
                        continue
                    s = item['selftext'] if is_submission else item['body']
                    context = contextClear(s)
                    if context == "":
                        continue

                    for element in attr:
                        if element.endswith('content'):
                            value = context
                        elif element.endswith('create'):
                            value = datetime.datetime.fromtimestamp(int(
                                item['created_utc'])).strftime("%Y-%m-%d")
                        # elif element == 'post_url':
                        #     value = f"https://www.reddit.com{item['permalink']}"
                        elif element == 'post_id':
                            if is_submission:
                                value = item['id']
                            else:
                                value = item['link_id'][3:]
                        elif element.endswith('score'):
                            value = item['score']
                        # elif element == 'comment_id':
                        #     value = item['id']
                        elif element == 'post_title':
                            value = contextClear(item['title'])
                        else:
                            value = item[element]
                        output.append(str(value))
                    writer.writerow(output)

            except KeyError as e:
                print(e)


def Dir_File(dir):
    fileIterator: Iterable[str]
    fileIterator = os.listdir(dir)
    fileIterator = (os.path.join(dir, file) for file in fileIterator)

    for i, file in enumerate(fileIterator):
        DataClean(file)


def main():
    for FilePath in sys.argv[1:]:
        if os.path.isdir(FilePath):
            Dir_File(FilePath)
        else:
            DataClean(FilePath)


if __name__ == "__main__":
    main()
