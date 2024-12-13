import psycopg2
import subprocess


def upload(POST, post_path, comment_path, cursor):
    sql = '''CREATE TABLE {0}(private_id SERIAL UNIQUE, post_id VARCHAR(8) NOT NULL,
    subreddit VARCHAR(25), post_title TEXT, post_content TEXT,post_score INT,
    post_create DATE, comment_content TEXT [],comment_score INT[],comment_create DATE []);'''.format(
        POST)
    cursor.execute(sql)

    sql = '''
    CREATE TABLE COMMAND(post_id VARCHAR(8) NOT NULL,
    comment_content TEXT,comment_score INT, comment_create DATE);
    '''
    cursor.execute(sql)
    copy_command = f"""
    psql -U postgres -h 35.222.3.25 -d reddit-data -c "\\copy {POST}(post_id, subreddit, post_title, post_content, post_score, post_create) FROM '{post_path}' DELIMITER ',' CSV HEADER"
    """
    subprocess.run(copy_command, shell=True, check=True)
    copy_command = f"""
    psql -U postgres -h 35.222.3.25 -d reddit-data -c "\\copy COMMAND(post_id, comment_content, comment_score, comment_create) FROM '{comment_path}' DELIMITER ',' CSV HEADER"
    """
    subprocess.run(copy_command, shell=True, check=True)

    sql3 = '''UPDATE {0}
    SET comment_content = subquery.content,
    comment_score = subquery.score,
    comment_create=subquery.create
    FROM (
        SELECT
            p.post_id,
            ARRAY_AGG(c.comment_content) AS content,
            ARRAY_AGG(c.comment_score) AS score,
            ARRAY_AGG(c.comment_create) AS create
        FROM
            {0} p
        LEFT JOIN
            COMMAND c
        ON
            p.post_id = c.post_id
        GROUP BY
            p.post_id
    ) AS subquery
    WHERE {0}.post_id = subquery.post_id;
    '''.format(POST)
    cursor.execute(sql3)

    sql4 = '''select * from {0};'''.format(POST)
    cursor.execute(sql4)

    sql5 = '''drop table command'''
    cursor.execute(sql5)


def main():
    # 'sports', 'economics', 'politics'
    database = input("Please enter database info\nDatabase: ")
    user = input("User: ")
    pwd = input("Password: ")
    host = input("Host: ")
    port = input("Post: ")
    POST = input('Please enter your target TABLE name: ')
    post_path = input('Please enter your local submissions csv file address: ')
    comment_path = input('Please enter your local comments csv file address: ')
    conn = psycopg2.connect(database=database, user=user, password=pwd, host=host, port=port)
    try:
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error connecting to the remote database: {e}")
        exit()
    upload(POST, post_path, comment_path, cursor)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
