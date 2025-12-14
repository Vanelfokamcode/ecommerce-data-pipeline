import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

print("="*80)
print("DAY 8: POSTGRESQL FIRST CONNECTION TEST")
print("="*80)

# ==============================================================================
# CONFIGURATION - CHANGE YOUR PASSWORD HERE
# ==============================================================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',  # Connect to default database first
    'user': 'postgres',
    'password': '1234'  
}

# ==============================================================================
# STEP 1: TEST CONNECTION TO POSTGRESQL
# ==============================================================================
print("\n[STEP 1] Connecting to PostgreSQL...")
print(f"  Host: {DB_CONFIG['host']}")
print(f"  Port: {DB_CONFIG['port']}")
print(f"  Database: {DB_CONFIG['database']}")
print(f"  User: {DB_CONFIG['user']}")

try:
    # Connect to default postgres database
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connection successful!")
    
    # Get PostgreSQL version
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]
    print(f"\n✅ PostgreSQL version:")
    print(f"  {version.split(',')[0]}")  # Print first part of version string
    
    cursor.close()
    conn.close()
    print("✅ Connection closed properly")
    
except psycopg2.Error as e:
    print(f"❌ Connection failed!")
    print(f"Error: {e}")
    print("\nTroubleshooting:")
    print("  1. Check your password is correct")
    print("  2. Make sure PostgreSQL service is running")
    print("  3. Verify port 5432 is not blocked")
    exit(1)

# ==============================================================================
# STEP 2: CREATE SHOPIFY_DB DATABASE
# ==============================================================================
print("\n" + "="*80)
print("[STEP 2] Creating 'shopify_db' database...")
print("="*80)

try:
    # Connect with autocommit to create database
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'shopify_db'")
    exists = cursor.fetchone()
    
    if exists:
        print("⚠️  Database 'shopify_db' already exists, dropping it first...")
        cursor.execute("DROP DATABASE shopify_db")
        print("✅ Old database dropped")
    
    # Create new database
    cursor.execute("CREATE DATABASE shopify_db")
    print("✅ Database 'shopify_db' created successfully!")
    
    cursor.close()
    conn.close()
    
except psycopg2.Error as e:
    print(f"❌ Failed to create database!")
    print(f"Error: {e}")
    exit(1)

# ==============================================================================
# STEP 3: CONNECT TO NEW DATABASE AND CREATE TABLE
# ==============================================================================
print("\n" + "="*80)
print("[STEP 3] Connecting to 'shopify_db' and creating test table...")
print("="*80)

try:
    # Update config to connect to new database
    DB_CONFIG['database'] = 'shopify_db'
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("✅ Connected to 'shopify_db'")
    
    # Create test_products table
    create_table_query = """
    CREATE TABLE test_products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    print("✅ Table 'test_products' created with columns:")
    print("   - id (auto-incrementing integer)")
    print("   - name (text, up to 255 characters)")
    print("   - price (decimal with 2 decimal places)")
    print("   - created_at (timestamp)")
    
except psycopg2.Error as e:
    print(f"❌ Failed to create table!")
    print(f"Error: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ==============================================================================
# STEP 4: INSERT TEST DATA
# ==============================================================================
print("\n" + "="*80)
print("[STEP 4] Inserting test products...")
print("="*80)

try:
    # Insert 3 test products
    test_products = [
        ('Laptop', 999.99),
        ('Wireless Mouse', 29.99),
        ('Mechanical Keyboard', 79.99)
    ]
    
    insert_query = """
    INSERT INTO test_products (name, price)
    VALUES (%s, %s)
    """
    
    for product_name, product_price in test_products:
        cursor.execute(insert_query, (product_name, product_price))
        print(f"  ✓ Inserted: {product_name} - ${product_price}")
    
    conn.commit()
    print("✅ All test products inserted successfully!")
    
except psycopg2.Error as e:
    print(f"❌ Failed to insert data!")
    print(f"Error: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ==============================================================================
# STEP 5: QUERY AND DISPLAY DATA
# ==============================================================================
print("\n" + "="*80)
print("[STEP 5] Querying database...")
print("="*80)

try:
    # Query all products
    cursor.execute("SELECT id, name, price, created_at FROM test_products ORDER BY id")
    products = cursor.fetchall()
    
    print(f"\n📊 Found {len(products)} products in database:\n")
    print(f"{'ID':<5} {'Product Name':<25} {'Price':<12} {'Created At'}")
    print("-" * 80)
    
    for product in products:
        product_id, name, price, created_at = product
        print(f"{product_id:<5} {name:<25} ${price:<11.2f} {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n✅ Query successful!")
    
except psycopg2.Error as e:
    print(f"❌ Failed to query data!")
    print(f"Error: {e}")

# ==============================================================================
# STEP 6: ADDITIONAL TEST QUERIES
# ==============================================================================
print("\n" + "="*80)
print("[STEP 6] Running additional test queries...")
print("="*80)

try:
    # Test 1: Count products
    cursor.execute("SELECT COUNT(*) FROM test_products")
    count = cursor.fetchone()[0]
    print(f"✓ Total products: {count}")
    
    # Test 2: Get most expensive product
    cursor.execute("SELECT name, price FROM test_products ORDER BY price DESC LIMIT 1")
    most_expensive = cursor.fetchone()
    print(f"✓ Most expensive: {most_expensive[0]} (${most_expensive[1]})")
    
    # Test 3: Get average price
    cursor.execute("SELECT AVG(price) FROM test_products")
    avg_price = cursor.fetchone()[0]
    print(f"✓ Average price: ${avg_price:.2f}")
    
    # Test 4: Get products over $50
    cursor.execute("SELECT name, price FROM test_products WHERE price > 50")
    expensive_products = cursor.fetchall()
    print(f"✓ Products over $50: {len(expensive_products)}")
    for name, price in expensive_products:
        print(f"    - {name}: ${price}")
    
    print("\n✅ All test queries successful!")
    
except psycopg2.Error as e:
    print(f"❌ Query failed!")
    print(f"Error: {e}")

# ==============================================================================
# CLEANUP AND CLOSE
# ==============================================================================
print("\n" + "="*80)
print("[CLEANUP] Closing connections...")
print("="*80)

try:
    cursor.close()
    conn.close()
    print("✅ Database connections closed properly")
    
except Exception as e:
    print(f"⚠️  Error during cleanup: {e}")

# ==============================================================================
# FINAL SUMMARY
# ==============================================================================
print("\n" + "="*80)
print("🎉 DAY 8 COMPLETE!")
print("="*80)

print("\n✅ What we accomplished:")
print("  1. ✓ Connected to PostgreSQL")
print("  2. ✓ Created 'shopify_db' database")
print("  3. ✓ Created 'test_products' table")
print("  4. ✓ Inserted 3 test products")
print("  5. ✓ Queried data successfully")
print("  6. ✓ Ran aggregate queries (COUNT, AVG, MAX)")
print("  7. ✓ Closed connections properly")

print("\n📚 What you learned:")
print("  • How to connect Python to PostgreSQL")
print("  • How to create databases and tables")
print("  • How to INSERT data")
print("  • How to SELECT and query data")
print("  • Basic SQL operations (COUNT, AVG, WHERE, ORDER BY)")

print("\n🚀 Next Steps (Day 9):")
print("  • Load your 5,869 Shopify products into PostgreSQL")
print("  • Design proper table structure")
print("  • Create relationships between tables")

print("\n" + "="*80)
print("Database Setup Complete! Ready for Day 9! 🎉")
print("="*80)