import psycopg2
from psycopg2.extras import Json
import json, sys, os, argparse

try:
    # Connect to db
    con = psycopg2.connect(user="postgres",
                           password="vlad",
                           host="127.0.0.1",
                           port="5432",
                           database="json")

    cursor = con.cursor()

    with open('test.json', 'r', encoding='utf-8') as f:
        data = (json.load(f))

        query = '''
WITH test_json (doc) AS (VALUES (
'{}'
::json))
INSERT INTO test (id, "ParentId" , "Name", "Type")
SELECT p.*
FROM test_json l
	CROSS JOIN lateral json_populate_recordset(null::test, doc) AS p 
	
ON conflict (id) do UPDATE 
SET 
	"ParentId" = excluded."ParentId",
	"Name" = excluded."Name",
	"Type" = excluded."Type";
	'''.format(json.dumps(data))
    cursor.execute(query)
    con.commit()

    parser = argparse.ArgumentParser(description="Вывод списка сотрудников по индексу сотрудника")
    parser.add_argument('id', metavar='N', type=int, nargs='+',
                        help='Введите индекс сотрудника')

    args = parser.parse_args()


    def query(text, value):
        if value > maxid() or value < minid():
            return 0
        elif value == 15:
            return 10
        else:
            cursor.execute(text, [value])
            return cursor.fetchone()[0]


    def maxid():
        cursor.execute('SELECT max("id") FROM test;')
        return cursor.fetchone()[0]


    def minid():
        cursor.execute('SELECT min("id") FROM test;')
        return cursor.fetchone()[0]


    def getid(id):
        print("id before", id)
        if id == 15:
            return 10
        else:
            print(id)
            cursor.execute('SELECT "id" FROM test WHERE id = %s;', [id])
            return cursor.fetchone()[0]


    def arrofid(arr, i):
        while query('SELECT "Type" FROM test WHERE id = %s;', i) != 1 and getid(i) != maxid():
            if i == 15:
                i += 1
            else:
                arr.append(getid(i))
                i += 1

        if args.id[0] in arr:
            arr2 = []
            arr.append(i)
            print("Список сотрудников в ", query('SELECT "Name" FROM test WHERE id = %s;', arr[0]))
            for i in arr:
                cursor.execute('SELECT "Name" FROM test WHERE id = %s AND "Type" = 3;', [i])
                arr2.append(cursor.fetchone())
            res = list(filter(None, arr2))
            print("\n".join(map(lambda xs: " ".join(map(str, xs)), res)))

        elif args.id[0] not in arr:
            arr.clear()
            arr.append(i)
            i += 1
            print("func def id ", i)
            arrofid(arr, i)


    i = 1
    arr = []
    if query('SELECT "Type" FROM test WHERE id = %s;', args.id[0]) != 3:
        print("Input Error")
    else:
        arrofid(arr, i)



except psycopg2.OperationalError as e:
    print("Connection error", e)
finally:
    if con:
        cursor.close()
        con.close()
