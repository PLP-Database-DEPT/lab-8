import pymysql

def create_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='', # Add your password
        database='photos'
    )

def create_table():
    query = """
    CREATE TABLE IF NOT EXISTS images (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        data LONGBLOB NOT NULL
    );
    """
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

def insert_blob(file_path, name):
    with open(file_path, 'rb') as file:
        binary_data = file.read()

    conn = create_connection()
    cursor = conn.cursor()
    query = "INSERT INTO images (name, data) VALUES (%s, %s)"
    cursor.execute(query, (name, binary_data))
    conn.commit()
    print(f"{name} saved to database.")
    cursor.close()
    conn.close()

def retrieve_blob(image_id, output_path):
    conn = create_connection()
    cursor = conn.cursor()
    query = "SELECT name, data FROM images WHERE id = %s"
    cursor.execute(query, (image_id,))
    result = cursor.fetchone()

    if result:
        name, binary_data = result
        with open(output_path, 'wb') as file:
            file.write(binary_data)
        print(f"{name} retrieved and saved to {output_path}")
    else:
        print("No data found.")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_table()
    insert_blob('sample.jpg', 'sample1')
    retrieve_blob(1, 'output/retrieved.jpg')
