import mysql.connector

# ----------------------------
# Database Shard Configurations
# ----------------------------
db_shards = {
    "Asia": {
        "host": " 172.31.31.46",  # DB1 private IP
        "user": "root",
        "password": "",
        "database": "userdb"
    },
    "Europe/USA": {
        "host": " 172.31.25.136",  # DB2 private IP
        "user": "root",
        "password": "",
        "database": "userdb"
    }
}

# ----------------------------
# Shard Selection Function
# ----------------------------
def get_shard_by_location(country):
    asia_countries = ["India", "China", "Japan", "Singapore"]
    usa_europe_countries = ["USA", "UK", "Germany", "France"]

    if country in asia_countries:
        return "Asia"
    elif country in usa_europe_countries:
        return "Europe/USA"
    else:
        # Default shard
        return "Asia"

# ----------------------------
# Insert User
# ----------------------------
def insert_user(name, country):
    shard_key = get_shard_by_location(country)
    db_config = db_shards[shard_key]

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (name, country) VALUES (%s, %s)", (name, country))
    conn.commit()
    print(f"Inserted: {name} ({country}) in Shard {shard_key}")
    cursor.close()
    conn.close()

# ----------------------------
# Fetch Users from All Shards
# ----------------------------
def get_users():
    for shard_key, db_config in db_shards.items():
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        print(f"\nUsers in Shard {shard_key}:")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
        conn.close()

# ----------------------------
# Main Testing
# ----------------------------
if __name__ == "__main__":
    # Insert some users
    insert_user("Rahul", "India")
    insert_user("Alex", "USA")
    insert_user("Yuki", "Japan")
    insert_user("John", "UK")
    insert_user("Zara", "UAE")  # goes to default shard

    # Fetch users
    get_users()
