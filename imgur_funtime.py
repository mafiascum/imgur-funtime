import mariadb
import os
import re
import json
import sys

imgur_regex_str = "https?://i\\.imgur\\.com/[A-Za-z0-9]+\\.[A-Za-z]{3,4}"
imgur_regex = re.compile(imgur_regex_str, re.IGNORECASE)


def collect_images(cursor, table, id_column, text_column):
    with open(f"connections/{table}.json", "w+") as out_file:
        output = {}
        after_id = 0
        more = True
        while more:
            print(f"after_id is now {after_id}")
            cursor.execute(f"select {id_column}, {text_column} from {table} where {text_column} RLIKE '{imgur_regex_str}' and {id_column} > {after_id} order by {id_column} limit 1000")
            results = cursor.fetchall()
            result_len = len(results)
            print(f"found {result_len} rows")
            if result_len == 0:
                more = False
            for (p_id, text) in results:
                after_id = p_id
                matches = re.findall(imgur_regex, text)
                for match in matches:
                    if p_id not in output:
                        output[p_id] = {}
                    output[p_id][match] = True
        out_file.seek(0)
        json.dump(output, out_file)
        out_file.truncate()


def collect():
    conn = mariadb.connect(
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        host=os.environ['DB_HOST'],
        port=3306,
        database=os.environ['DB_NAME']
    )
    cursor = conn.cursor()
    collect_images(cursor, "phpbb_posts", "post_id", "post_text")
    collect_images(cursor, "phpbb_privmsgs", "msg_id", "message_text")


if __name__ == "__main__":
    if sys.argv[1] == 'collect':
        collect()
