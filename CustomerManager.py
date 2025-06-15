import psycopg2

def create_db(conn):
    with conn.cursor() as cursor:
        cursor.execute('DROP SCHEMA IF EXISTS customers CASCADE')
        cursor.execute('CREATE SCHEMA IF NOT EXISTS customers;')
        cursor.execute('CREATE TABLE IF NOT EXISTS customers.customers '
                       '(customer_id SERIAL NOT NULL PRIMARY KEY,'
                       'name VARCHAR(255) NOT NULL,'
                       'surname VARCHAR(255) NOT NULL,'
                       'email VARCHAR(255) NOT NULL);')
        cursor.execute('CREATE TABLE IF NOT EXISTS customers.customers_phones '
                       '(phone_id SERIAL PRIMARY KEY,'
                       'customer_id INTEGER NOT NULL REFERENCES customers.customers(customer_id),'
                       'phone VARCHAR(255) NOT NULL UNIQUE);')
        conn.commit()

def add_client(conn, name, surname, email, phones=None):
    with conn.cursor() as cursor:
        cursor.execute('INSERT INTO customers.customers (name, surname, email) VALUES (%s, %s, %s) RETURNING customer_id;',
                       (name, surname, email))
        customer_id = cursor.fetchone()[0]
        if phones is not None:
            queries = (f"INSERT INTO customers.customers_phones (customer_id, phone) VALUES('{customer_id}','{phone}');"
                       for phone in phones)
            for q in queries:
                cursor.execute(q)

def add_phone(conn, customer_id, phone):
    with conn.cursor() as cursor:
        cursor.execute(f"INSERT INTO customers.customers_phones (customer_id, phone) VALUES('{customer_id}','{phone}') ")

def change_customer(conn, customer_id, name=None, surname=None, email=None, phones=None):
    with conn.cursor() as cursor:
        cursor.execute(f"UPDATE customers.customers SET name = '{name}', "
                       f"surname = '{surname}', email = '{email}' WHERE customer_id = {customer_id};")
        if phones is not None:
            queries = (f"INSERT INTO customers.customers_phones (customer_id, phone) VALUES('{customer_id}','{phone}');"
                       for phone in phones)
            for q in queries:
                cursor.execute(q)

def delete_phone(conn, customer_id, phone):
    with conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM customers.customers_phones WHERE customer_id = {customer_id} AND phone = '{phone}'", (customer_id, phone))

def delete_customer(conn, customer_id):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM customers.customers_phones WHERE customer_id = {customer_id} RETURNING phone_id")
        phone_ids = cursor.fetchall()
        print(phone_ids)
        # cursor.execute("DELETE FROM customers.customers WHERE customer_id = %s", customer_id)

def find_customer(conn, name=None, surname=None, email=None, phone=None):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM customers.customers c "
                       f"INNER JOIN customers.customers_phones cp ON c.customer_id = cp.customer_id "
                       f"WHERE c.name = '{name}' AND c.surname = '{surname}' AND c.email = '{email}' AND cp.phone = '{phone}';")
        print(cursor.fetchall())



if __name__ == "__main__":
    with psycopg2.connect('dbname=postgres user=postgres password=Test!123') as conn:
        create_db(conn)
        add_client(conn, 'test', 'testtest', 'test@test.test')
        add_client(conn, 'test', 'testtest', 'test@test.test', ['+1(111)1111111','+2(222)2222222'])
        add_client(conn, 'test3', 'testtest3', 'test3@test.test', ['+3(333)3333333'])
        add_client(conn, 'test4', 'testtest4', 'test4@test.test', ['+4(444)4444444'])
        add_phone(conn, 4, '+5(444)4444444')
        # change_customer(conn, 2, 'test5', 'testtest5', 'test5@test.test', '+6(666)6666666')
        delete_phone(conn, 1, '+2(222)2222222')
        find_customer(conn, 'test', 'testtest', 'test@test.test')

        conn.commit()

    conn.close()