import sqlite3

# Connect to the SQLite database or create a new one
conn = sqlite3.connect('CREDENTIALS.db')

# Create a cursor object
cursor = conn.cursor()

# Create a table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT,
        pass TEXT
    )
''')

# Insert data
cursor.execute("INSERT INTO users (username, pass) VALUES (?, ?)", ('sahar', '123'))

# Commit changes
conn.commit()

# Select and print data
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()
for row in rows:
    print(row)

# Close the database connection
conn.close()
