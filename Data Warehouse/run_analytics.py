import configparser
import psycopg2
from sql_queries import analytical_queries


def run_analytical_queries(cur):
    for query in analytical_queries:
        print(query)
        row = cur.execute(query)
        print(cur.fetchall()[0][0])


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Connecting to database...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    run_analytical_queries(cur)

    conn.close()


if __name__ == "__main__":
    main()