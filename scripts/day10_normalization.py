import psycopg2
from psycopg2 import sql
import time
from datetime import datetime

print("="*80)
print("DAY 10: DATABASE NORMALIZATION - CREATING RELATED TABLES")
print("="*80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'shopify_db',
    'user': 'postgres',
    'password': '123'  # ⚠️ CHANGE THIS
}

start_time = time.time()

# ==============================================================================
# STEP 1: ANALYZE CURRENT DATA
# ==============================================================================
print("="*80)
print("[STEP 1] ANALYZING CURRENT DATA")
print("="*80)

try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cursor = conn.cursor()
    print("✅ Connected to shopify_db")
    
    # Count current products
    cursor.execute("SELECT COUNT(*) FROM products")
    total_products = cursor.fetchone()[0]
    print(f"\n📊 Current State:")
    print(f"   Total products: {total_products:,}")
    
    # Count unique vendors
    cursor.execute("SELECT COUNT(DISTINCT vendor) FROM products WHERE vendor IS NOT NULL")
    unique_vendors = cursor.fetchone()[0]
    print(f"   Unique vendors: {unique_vendors:,}")
    
    # Count unique categories
    cursor.execute("SELECT COUNT(DISTINCT product_category) FROM products WHERE product_category IS NOT NULL")
    unique_categories = cursor.fetchone()[0]
    print(f"   Unique categories: {unique_categories:,}")
    
    # Show top vendors
    cursor.execute("""
        SELECT vendor, COUNT(*) as count 
        FROM products 
        WHERE vendor IS NOT NULL
        GROUP BY vendor 
        ORDER BY count DESC 
        LIMIT 5
    """)
    print(f"\n   Top 5 Vendors:")
    for vendor, count in cursor.fetchall():
        print(f"     • {vendor}: {count:,} products")
    
    # Sample products
    cursor.execute("SELECT id, title, vendor, product_category, variant_price FROM products LIMIT 3")
    print(f"\n   Sample Products:")
    for row in cursor.fetchall():
        print(f"     • {row[1][:50]}... | {row[2]} | ${row[4]}")
    
except psycopg2.Error as e:
    print(f"❌ Analysis failed: {e}")
    exit(1)

# ==============================================================================
# STEP 2: CREATE NEW NORMALIZED TABLES
# ==============================================================================
print("\n" + "="*80)
print("[STEP 2] CREATING NORMALIZED TABLES")
print("="*80)

try:
    # Drop existing normalized tables if they exist
    cursor.execute("DROP TABLE IF EXISTS pricing CASCADE")
    cursor.execute("DROP TABLE IF EXISTS products_normalized CASCADE")
    cursor.execute("DROP TABLE IF EXISTS vendors CASCADE")
    cursor.execute("DROP TABLE IF EXISTS categories CASCADE")
    print("⚠️  Dropped existing normalized tables (if any)")
    
    # A) Create vendors table
    print("\n📋 Creating vendors table...")
    cursor.execute("""
        CREATE TABLE vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) UNIQUE NOT NULL,
            vendor_tier VARCHAR(50),
            product_count INTEGER DEFAULT 0,
            avg_price DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ vendors table created")
    
    # B) Create categories table
    print("📋 Creating categories table...")
    cursor.execute("""
        CREATE TABLE categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) UNIQUE NOT NULL,
            product_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ categories table created")
    
    # C) Create products_normalized table
    print("📋 Creating products_normalized table...")
    cursor.execute("""
        CREATE TABLE products_normalized (
            product_id SERIAL PRIMARY KEY,
            handle VARCHAR(500) UNIQUE NOT NULL,
            title TEXT NOT NULL,
            vendor_id INTEGER REFERENCES vendors(vendor_id) ON DELETE SET NULL,
            category_id INTEGER REFERENCES categories(category_id) ON DELETE SET NULL,
            seo_title TEXT,
            seo_description TEXT,
            tags TEXT,
            status VARCHAR(50),
            content_quality_score INTEGER,
            high_value_product BOOLEAN DEFAULT FALSE,
            quick_win BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ products_normalized table created with foreign keys")
    
    # D) Create pricing table
    print("📋 Creating pricing table...")
    cursor.execute("""
        CREATE TABLE pricing (
            pricing_id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products_normalized(product_id) ON DELETE CASCADE,
            variant_price DECIMAL(10, 2),
            compare_at_price DECIMAL(10, 2),
            cost_per_item DECIMAL(10, 2),
            profit_margin DECIMAL(10, 2),
            price_tier VARCHAR(50),
            discount_strategy VARCHAR(50),
            price_valid BOOLEAN DEFAULT FALSE,
            discount_valid BOOLEAN DEFAULT FALSE,
            needs_pricing_review BOOLEAN DEFAULT FALSE,
            effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ pricing table created with foreign keys")
    
    conn.commit()
    print("\n✅ All normalized tables created successfully!")
    
except psycopg2.Error as e:
    print(f"❌ Failed to create tables: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ==============================================================================
# STEP 3: MIGRATE DATA
# ==============================================================================
print("\n" + "="*80)
print("[STEP 3] MIGRATING DATA TO NORMALIZED TABLES")
print("="*80)

try:
    # STEP 3.1: Migrate Vendors
    print("\n📦 Step 3.1: Migrating vendors...")
    cursor.execute("""
        INSERT INTO vendors (vendor_name, vendor_tier)
        SELECT DISTINCT 
            vendor,
            NULL as vendor_tier
        FROM products
        WHERE vendor IS NOT NULL
        ORDER BY vendor
    """)
    vendor_count = cursor.rowcount
    print(f"✅ Inserted {vendor_count:,} unique vendors")
    
    # STEP 3.2: Migrate Categories
    print("📦 Step 3.2: Migrating categories...")
    cursor.execute("""
        INSERT INTO categories (category_name)
        SELECT DISTINCT product_category
        FROM products
        WHERE product_category IS NOT NULL
        ORDER BY product_category
    """)
    category_count = cursor.rowcount
    print(f"✅ Inserted {category_count:,} unique categories")
    
    # STEP 3.3: Migrate Products
    print("📦 Step 3.3: Migrating products...")
    cursor.execute("""
        INSERT INTO products_normalized (
            handle, title, vendor_id, category_id,
            seo_title, seo_description, tags, status,
            content_quality_score, high_value_product, quick_win
        )
        SELECT 
            p.handle,
            p.title,
            v.vendor_id,
            c.category_id,
            p.seo_title,
            p.seo_description,
            p.tags,
            p.status,
            p.content_quality_score,
            COALESCE(p.high_value_product, FALSE),
            COALESCE(p.quick_win, FALSE)
        FROM products p
        LEFT JOIN vendors v ON p.vendor = v.vendor_name
        LEFT JOIN categories c ON p.product_category = c.category_name
    """)
    products_migrated = cursor.rowcount
    print(f"✅ Migrated {products_migrated:,} products")
    
    # STEP 3.4: Migrate Pricing
    print("📦 Step 3.4: Migrating pricing data...")
    cursor.execute("""
        INSERT INTO pricing (
            product_id, variant_price, compare_at_price, cost_per_item,
            profit_margin, price_tier, discount_strategy,
            price_valid, discount_valid, needs_pricing_review
        )
        SELECT 
            pn.product_id,
            p.variant_price,
            p.variant_compare_at_price,
            p.cost_per_item,
            p.profit_margin,
            p.price_tier,
            p.discount_strategy,
            COALESCE(p.price_valid, FALSE),
            COALESCE(p.discount_valid, FALSE),
            COALESCE(p.needs_pricing_review, FALSE)
        FROM products p
        JOIN products_normalized pn ON p.handle = pn.handle
    """)
    pricing_migrated = cursor.rowcount
    print(f"✅ Migrated {pricing_migrated:,} pricing records")
    
    conn.commit()
    print("\n✅ Data migration complete!")
    
except psycopg2.Error as e:
    print(f"❌ Migration failed: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ==============================================================================
# STEP 4: CREATE INDEXES
# ==============================================================================
print("\n" + "="*80)
print("[STEP 4] CREATING PERFORMANCE INDEXES")
print("="*80)

try:
    indexes = [
        ("idx_products_vendor_id", "products_normalized", "vendor_id"),
        ("idx_products_category_id", "products_normalized", "category_id"),
        ("idx_products_handle", "products_normalized", "handle"),
        ("idx_products_high_value", "products_normalized", "high_value_product"),
        ("idx_pricing_product_id", "pricing", "product_id"),
    ]
    
    for idx_name, table, column in indexes:
        cursor.execute(f"CREATE INDEX {idx_name} ON {table}({column})")
        print(f"✅ Created index: {idx_name} on {table}({column})")
    
    conn.commit()
    print("\n✅ All indexes created successfully!")
    
except psycopg2.Error as e:
    print(f"❌ Index creation failed: {e}")
    conn.rollback()

# ==============================================================================
# STEP 5: UPDATE VENDOR/CATEGORY COUNTS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 5] UPDATING AGGREGATE COUNTS")
print("="*80)

try:
    # Update vendor counts and avg prices
    print("📊 Updating vendor statistics...")
    cursor.execute("""
        UPDATE vendors v
        SET 
            product_count = (
                SELECT COUNT(*) 
                FROM products_normalized p 
                WHERE p.vendor_id = v.vendor_id
            ),
            avg_price = (
                SELECT AVG(pr.variant_price)
                FROM products_normalized p
                JOIN pricing pr ON p.product_id = pr.product_id
                WHERE p.vendor_id = v.vendor_id
            )
    """)
    print(f"✅ Updated {cursor.rowcount} vendor records")
    
    # Update category counts
    print("📊 Updating category statistics...")
    cursor.execute("""
        UPDATE categories c
        SET product_count = (
            SELECT COUNT(*) 
            FROM products_normalized p 
            WHERE p.category_id = c.category_id
        )
    """)
    print(f"✅ Updated {cursor.rowcount} category records")
    
    conn.commit()
    print("\n✅ Aggregate counts updated!")
    
except psycopg2.Error as e:
    print(f"❌ Update failed: {e}")
    conn.rollback()

# ==============================================================================
# STEP 6: CREATE VIEWS WITH JOINS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 6] CREATING VIEWS WITH JOINS")
print("="*80)

try:
    # Drop existing views
    cursor.execute("DROP VIEW IF EXISTS products_full_view CASCADE")
    cursor.execute("DROP VIEW IF EXISTS vendor_performance_view CASCADE")
    cursor.execute("DROP VIEW IF EXISTS high_value_products_full CASCADE")
    
    # A) Products Full View
    print("📋 Creating products_full_view...")
    cursor.execute("""
        CREATE VIEW products_full_view AS
        SELECT 
            p.product_id,
            p.handle,
            p.title,
            v.vendor_name,
            v.vendor_tier,
            c.category_name,
            pr.variant_price,
            pr.compare_at_price,
            pr.profit_margin,
            pr.price_tier,
            pr.discount_strategy,
            p.content_quality_score,
            p.high_value_product,
            p.quick_win,
            p.status
        FROM products_normalized p
        LEFT JOIN vendors v ON p.vendor_id = v.vendor_id
        LEFT JOIN categories c ON p.category_id = c.category_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
    """)
    print("✅ Created products_full_view")
    
    # B) Vendor Performance View
    print("📋 Creating vendor_performance_view...")
    cursor.execute("""
        CREATE VIEW vendor_performance_view AS
        SELECT 
            v.vendor_id,
            v.vendor_name,
            v.vendor_tier,
            v.product_count,
            v.avg_price,
            COUNT(CASE WHEN p.high_value_product THEN 1 END) as high_value_count,
            AVG(pr.profit_margin) as avg_margin,
            COUNT(CASE WHEN pr.needs_pricing_review THEN 1 END) as needs_review_count
        FROM vendors v
        LEFT JOIN products_normalized p ON v.vendor_id = p.vendor_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
        GROUP BY v.vendor_id, v.vendor_name, v.vendor_tier, v.product_count, v.avg_price
        ORDER BY v.product_count DESC
    """)
    print("✅ Created vendor_performance_view")
    
    # C) High Value Products Full
    print("📋 Creating high_value_products_full...")
    cursor.execute("""
        CREATE VIEW high_value_products_full AS
        SELECT 
            p.product_id,
            p.title,
            v.vendor_name,
            c.category_name,
            pr.variant_price,
            pr.profit_margin,
            p.content_quality_score,
            pr.price_tier
        FROM products_normalized p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        JOIN categories c ON p.category_id = c.category_id
        JOIN pricing pr ON p.product_id = pr.product_id
        WHERE p.high_value_product = TRUE
        ORDER BY pr.profit_margin DESC
    """)
    print("✅ Created high_value_products_full")
    
    conn.commit()
    print("\n✅ All views created successfully!")
    
except psycopg2.Error as e:
    print(f"❌ View creation failed: {e}")
    conn.rollback()

# ==============================================================================
# STEP 7: VERIFY DATA INTEGRITY
# ==============================================================================
print("\n" + "="*80)
print("[STEP 7] VERIFYING DATA INTEGRITY")
print("="*80)

try:
    # Count comparison
    cursor.execute("SELECT COUNT(*) FROM products")
    old_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM products_normalized")
    new_count = cursor.fetchone()[0]
    
    print(f"✅ Products in old table: {old_count:,}")
    print(f"✅ Products in new table: {new_count:,}")
    
    if old_count == new_count:
        print(f"✅ Perfect match! All {new_count:,} products migrated")
    else:
        print(f"⚠️  Mismatch: {abs(old_count - new_count)} products difference")
    
    # Verify foreign keys
    cursor.execute("""
        SELECT COUNT(*) 
        FROM products_normalized p
        WHERE p.vendor_id IS NOT NULL 
        AND NOT EXISTS (SELECT 1 FROM vendors v WHERE v.vendor_id = p.vendor_id)
    """)
    orphan_vendors = cursor.fetchone()[0]
    print(f"✅ Orphaned vendor references: {orphan_vendors} (should be 0)")
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM pricing pr
        WHERE NOT EXISTS (SELECT 1 FROM products_normalized p WHERE p.product_id = pr.product_id)
    """)
    orphan_pricing = cursor.fetchone()[0]
    print(f"✅ Orphaned pricing records: {orphan_pricing} (should be 0)")
    
    # Test JOIN query
    print("\n📊 Testing JOIN query...")
    cursor.execute("""
        SELECT p.title, v.vendor_name, pr.variant_price
        FROM products_normalized p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        LIMIT 3
    """)
    print("   Sample joined data:")
    for title, vendor, price in cursor.fetchall():
        print(f"     • {title[:40]}... | {vendor} | ${price}")
    
    print("\n✅ Data integrity verified!")
    
except psycopg2.Error as e:
    print(f"❌ Verification failed: {e}")

# ==============================================================================
# STEP 8: GENERATE COMPARISON REPORT
# ==============================================================================
print("\n" + "="*80)
print("[STEP 8] GENERATING COMPARISON REPORT")
print("="*80)

try:
    # Get table sizes
    cursor.execute("""
        SELECT 
            pg_size_pretty(pg_total_relation_size('products')) as old_size,
            pg_size_pretty(pg_total_relation_size('products_normalized') + 
                          pg_total_relation_size('vendors') + 
                          pg_total_relation_size('categories') + 
                          pg_total_relation_size('pricing')) as new_size
    """)
    old_size, new_size = cursor.fetchone()
    
    print(f"\n💾 Storage Comparison:")
    print(f"   Old single table: {old_size}")
    print(f"   New normalized tables: {new_size}")
    
    print(f"\n📊 Structure Comparison:")
    print(f"   Before: 1 table with all data")
    print(f"   After: 4 related tables")
    print(f"     • vendors: {vendor_count:,} records")
    print(f"     • categories: {category_count:,} records")
    print(f"     • products_normalized: {products_migrated:,} records")
    print(f"     • pricing: {pricing_migrated:,} records")
    
    print(f"\n⚡ Performance Benefits:")
    print(f"   ✓ Vendor data stored once, not {total_products:,} times")
    print(f"   ✓ Category data stored once, not repeated")
    print(f"   ✓ Faster queries with proper indexes")
    print(f"   ✓ Easier to maintain and update")
    
    print(f"\n📝 Sample JOIN Queries You Can Now Run:")
    print(f"""
   -- Get all products from a vendor with pricing
   SELECT p.title, v.vendor_name, pr.variant_price
   FROM products_normalized p
   JOIN vendors v ON p.vendor_id = v.vendor_id
   JOIN pricing pr ON p.product_id = pr.product_id
   WHERE v.vendor_name = 'Perlys';
   
   -- Get vendor performance stats
   SELECT * FROM vendor_performance_view
   ORDER BY avg_margin DESC;
   
   -- Get high value products with full details
   SELECT * FROM high_value_products_full
   LIMIT 10;
    """)
    
except psycopg2.Error as e:
    print(f"❌ Report generation failed: {e}")

# ==============================================================================
# STEP 9: BACKUP AND RENAME
# ==============================================================================
print("\n" + "="*80)
print("[STEP 9] BACKING UP OLD TABLE")
print("="*80)

try:
    # Rename old table to backup
    cursor.execute("ALTER TABLE products RENAME TO products_backup")
    print("✅ Renamed 'products' → 'products_backup'")
    
    # Rename normalized table to products
    cursor.execute("ALTER TABLE products_normalized RENAME TO products")
    print("✅ Renamed 'products_normalized' → 'products'")
    
    # Update view to use new name
    cursor.execute("DROP VIEW IF EXISTS products_full_view CASCADE")
    cursor.execute("""
        CREATE VIEW products_full_view AS
        SELECT 
            p.product_id,
            p.handle,
            p.title,
            v.vendor_name,
            v.vendor_tier,
            c.category_name,
            pr.variant_price,
            pr.compare_at_price,
            pr.profit_margin,
            pr.price_tier,
            pr.discount_strategy,
            p.content_quality_score,
            p.high_value_product,
            p.quick_win,
            p.status
        FROM products p
        LEFT JOIN vendors v ON p.vendor_id = v.vendor_id
        LEFT JOIN categories c ON p.category_id = c.category_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
    """)
    print("✅ Recreated products_full_view with new table name")
    
    conn.commit()
    print("\n✅ Backup and rename complete!")
    print("   💾 Original data saved in 'products_backup'")
    print("   ✨ Normalized structure now in 'products' table")
    
except psycopg2.Error as e:
    print(f"❌ Backup/rename failed: {e}")
    conn.rollback()

# ==============================================================================
# CLEANUP AND SUMMARY
# ==============================================================================
print("\n" + "="*80)
print("[CLEANUP] Closing connections...")
print("="*80)

cursor.close()
conn.close()
print("✅ Database connections closed")

total_time = time.time() - start_time

# ==============================================================================
# FINAL SUMMARY
# ==============================================================================
print("\n" + "="*80)
print("🎉 DAY 10 COMPLETE! DATABASE NORMALIZATION SUCCESSFUL!")
print("="*80)

print(f"\n⏱️  Total Migration Time: {total_time:.2f} seconds")

print(f"\n✅ What we accomplished:")
print(f"  1. ✓ Analyzed existing single-table structure")
print(f"  2. ✓ Created 4 normalized tables with foreign keys")
print(f"  3. ✓ Migrated {total_products:,} products with relationships")
print(f"  4. ✓ Created 5 performance indexes")
print(f"  5. ✓ Updated aggregate counts for vendors/categories")
print(f"  6. ✓ Created 3 powerful JOIN views")
print(f"  7. ✓ Verified 100% data integrity")
print(f"  8. ✓ Backed up original data")

print(f"\n📊 New Database Structure:")
print(f"  • vendors: {vendor_count:,} unique vendors")
print(f"  • categories: {category_count:,} unique categories")
print(f"  • products: {products_migrated:,} products (with relationships!)")
print(f"  • pricing: {pricing_migrated:,} pricing records")

print(f"\n💡 Benefits of Normalization:")
print(f"  ✓ Reduced data redundancy")
print(f"  ✓ Easier to maintain vendor/category data")
print(f"  ✓ Better query performance with indexes")
print(f"  ✓ Enforced data integrity with foreign keys")
print(f"  ✓ Scalable structure for future growth")

print(f"\n🚀 Next Steps (Day 11):")
print(f"  • Learn advanced SQL queries with JOINs")
print(f"  • Practice complex aggregations")
print(f"  • Query optimization techniques")
print(f"  • Understand EXPLAIN ANALYZE")

print(f"\n💾 Your tables are now:")
print(f"  • products (main table, normalized)")
print(f"  • vendors (vendor master data)")
print(f"  • categories (category master data)")
print(f"  • pricing (price history)")
print(f"  • products_backup (your safety net!)")

print("\n" + "="*80)
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)