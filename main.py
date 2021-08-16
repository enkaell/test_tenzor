import psycopg2
from psycopg2.extras import Json
import json, sys, os, argparse

#Подключение к БД
try:
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
    #Создание позиционных аргументов командной строки
    parser = argparse.ArgumentParser(description="Вывод списка сотрудников по индексу сотрудника")
    parser.add_argument('id', metavar='N', type=int, nargs='+',
                        help='Введите индекс сотрудника')

    args = parser.parse_args()

    #Функция для запросов в бд
    #text - str sql - запрос
    #value - int значение для выборки
    def query(text, value):
        cursor.execute(text, [value])
        return cursor.fetchone()[0]


    #Функция для поиска максимального значения идентификатора в базе
    def maxid():
        cursor.execute('SELECT max("id") FROM test;')
        return cursor.fetchone()[0]


    #Функция для поиска минимального значения идентификатора в базе
    def minid():
        cursor.execute('SELECT min("id") FROM test;')
        return cursor.fetchone()[0]


    #Функция для поиска и вывода имен по идентификатору
    #arr - list список который будет заполняться идентификаторами
    #searchid - int идентифкатор по которому будет осуществляться поиск
    def arrofid(arr, searchid):
        while query('SELECT "Type" FROM test WHERE id = %s;', searchid) != 1:
            searchid -= 1
            if searchid == 15:
                searchid -= 1

        top = searchid
        searchid += 1
        while searchid != maxid()+1 and query('SELECT "Type" FROM test WHERE id = %s;', searchid) != 1:
            arr.append(searchid)
            searchid += 1
            if searchid == 15:
                searchid += 1

        print("Список сотрудников в ", query('SELECT "Name" FROM test WHERE id = %s-1;', arr[0]))

        for i in arr:
            if query('SELECT "Type" FROM test WHERE  "id" = %s;', i) == 3:
                print(query('SELECT "Name" FROM test WHERE id = %s;', i))


    arr = []
    if args.id[0] > maxid() or args.id[0] == 15 or  args.id[0]< minid() or query('SELECT "Type" FROM test WHERE id = %s;', args.id[0]) != 3:
        print("Input Error")
    else:
        arrofid(arr, args.id[0])



except psycopg2.OperationalError as e:
    print("Connection error", e)
finally:
    if con:
        cursor.close()
        con.close()
