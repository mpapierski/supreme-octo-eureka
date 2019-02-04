import sqlite3


def db_connect():
    """Connects to the database"""
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS data(key PRIMARY KEY, value)''')
    conn.commit()
    return conn


DB = db_connect()


def db_write(key, value):
    """Writes a value to a key"""
    if db_read(key) is not None:
        DB.execute('''UPDATE data SET value = ? WHERE key = ?''', (value, key))
        print('Update', key, 'with', value)
    else:
        DB.execute('''INSERT INTO data(key, value) VALUES (?, ?)''', (key, value))
        print('New key', key, 'value', value)


def db_read(key):
    """Reads a value saved by key"""
    cur = DB.cursor()
    cur.execute('''SELECT value FROM data WHERE key = ?''', (key, ))
    row = cur.fetchone()
    if row is None:
        return None
    (value, ) = row
    return value
