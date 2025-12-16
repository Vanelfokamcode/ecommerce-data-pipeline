"""
Database Functions Library for Shopify Product Database
Reusable functions for common database operations
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import List, Dict, Optional, Tuple

# ==============================================================================
# DATABASE CONFIGURATION
# ==============================================================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'shopify_db',
    'user': 'postgres',
    'password': '2003'
}

# ==============================================================================
# 1. CONNECTION MANAGEMENT
# ==============================================================================

def get_connection(dict_cursor=True):
    """
    Create and return a database connection.
    
    Args:
        dict_cursor (bool): Use RealDictCursor if True
    
    Returns:
        psycopg2.connection: Active database connection
    """
    try:
        if dict_cursor:
            conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        else:
            conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"❌ Connection failed: {e}")
        return None


def close_connection(conn):
    """
    Properly close a database connection.
    
    Args:
        conn: Database connection to close
    """
    if conn:
        conn.close()


def execute_query(query: str, params: tuple = None, fetch: bool = True):
    """
    Execute a query safely with error handling.
    
    Args:
        query (str): SQL query to execute
        params (tuple): Parameters for query (prevents SQL injection)
        fetch (bool): Whether to fetch results
        
    Returns:
        list: Query results if fetch=True, None otherwise
    """
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        if fetch:
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return results
        else:
            conn.commit()
            cursor.close()
            conn.close()
            return True
            
    except psycopg2.Error as e:
        print(f"❌ Query failed: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return None


# ==============================================================================
# 2. PRODUCT QUERIES
# ==============================================================================

def get_products_by_vendor(vendor_name: str) -> List[Dict]:
    """
    Get all products from a specific vendor.
    
    Args:
        vendor_name (str): Name of the vendor
        
    Returns:
        list: List of products with full details
        
    Example:
        products = get_products_by_vendor('Perlys')
    """
    query = """
        SELECT p.*, v.vendor_name, pr.variant_price, pr.profit_margin
        FROM products p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
        WHERE v.vendor_name = %s
        ORDER BY p.title
    """
    return execute_query(query, (vendor_name,))


def get_high_value_products(limit: int = 10) -> List[Dict]:
    """
    Get top high-value products (premium + high margin + good content).
    
    Args:
        limit (int): Number of products to return
        
    Returns:
        list: Top high-value products
        
    Example:
        top_products = get_high_value_products(20)
    """
    query = """
        SELECT p.*, v.vendor_name, pr.variant_price, pr.profit_margin, pr.price_tier
        FROM products p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        WHERE p.high_value_product = true
        ORDER BY pr.profit_margin DESC, pr.variant_price DESC
        LIMIT %s
    """
    return execute_query(query, (limit,))


def get_products_by_price_range(min_price: float, max_price: float) -> List[Dict]:
    """
    Get products within a specific price range.
    
    Args:
        min_price (float): Minimum price
        max_price (float): Maximum price
        
    Returns:
        list: Products in price range
        
    Example:
        mid_range = get_products_by_price_range(50, 100)
    """
    query = """
        SELECT p.*, v.vendor_name, pr.variant_price
        FROM products p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        WHERE pr.variant_price BETWEEN %s AND %s
        ORDER BY pr.variant_price
    """
    return execute_query(query, (min_price, max_price))


def get_product_by_handle(handle: str) -> Optional[Dict]:
    """
    Get a single product by its handle.
    
    Args:
        handle (str): Product handle (unique identifier)
        
    Returns:
        dict: Product details or None if not found
        
    Example:
        product = get_product_by_handle('collier-perle-123')
    """
    query = """
        SELECT p.*, v.vendor_name, c.category_name, pr.*
        FROM products p
        LEFT JOIN vendors v ON p.vendor_id = v.vendor_id
        LEFT JOIN categories c ON p.category_id = c.category_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
        WHERE p.handle = %s
    """
    results = execute_query(query, (handle,))
    return results[0] if results else None


def search_products_by_title(search_term: str) -> List[Dict]:
    """
    Search products by title (case-insensitive).
    
    Args:
        search_term (str): Text to search for in titles
        
    Returns:
        list: Matching products
        
    Example:
        necklaces = search_products_by_title('necklace')
    """
    query = """
        SELECT p.*, v.vendor_name, pr.variant_price
        FROM products p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
        WHERE p.title ILIKE %s
        ORDER BY p.title
    """
    return execute_query(query, (f'%{search_term}%',))


# ==============================================================================
# 3. VENDOR QUERIES
# ==============================================================================

def get_all_vendors() -> List[Dict]:
    """
    Get list of all vendors with their stats.
    
    Returns:
        list: All vendors with product counts and metrics
        
    Example:
        vendors = get_all_vendors()
    """
    query = """
        SELECT v.*, 
               COUNT(p.product_id) as actual_product_count,
               AVG(pr.profit_margin) as avg_margin
        FROM vendors v
        LEFT JOIN products p ON v.vendor_id = p.vendor_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
        GROUP BY v.vendor_id
        ORDER BY actual_product_count DESC
    """
    return execute_query(query)


def get_vendor_performance(vendor_name: str) -> Optional[Dict]:
    """
    Get detailed performance stats for a vendor.
    
    Args:
        vendor_name (str): Vendor name
        
    Returns:
        dict: Vendor performance metrics
        
    Example:
        stats = get_vendor_performance('Perlys')
    """
    query = """
        SELECT 
            v.vendor_name,
            COUNT(p.product_id) as total_products,
            COUNT(CASE WHEN p.high_value_product THEN 1 END) as high_value_count,
            AVG(pr.variant_price) as avg_price,
            AVG(pr.profit_margin) as avg_margin,
            MIN(pr.variant_price) as min_price,
            MAX(pr.variant_price) as max_price
        FROM vendors v
        JOIN products p ON v.vendor_id = p.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        WHERE v.vendor_name = %s
        GROUP BY v.vendor_name
    """
    results = execute_query(query, (vendor_name,))
    return results[0] if results else None


def get_top_vendors_by_margin(limit: int = 5) -> List[Dict]:
    """
    Get vendors with highest profit margins.
    
    Args:
        limit (int): Number of vendors to return
        
    Returns:
        list: Top performing vendors
        
    Example:
        top_vendors = get_top_vendors_by_margin(10)
    """
    query = """
        SELECT 
            v.vendor_name,
            COUNT(p.product_id) as product_count,
            AVG(pr.profit_margin) as avg_margin,
            AVG(pr.variant_price) as avg_price
        FROM vendors v
        JOIN products p ON v.vendor_id = p.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        GROUP BY v.vendor_name
        HAVING AVG(pr.profit_margin) IS NOT NULL
        ORDER BY avg_margin DESC
        LIMIT %s
    """
    return execute_query(query, (limit,))


# ==============================================================================
# 4. PRICING QUERIES
# ==============================================================================

def get_products_needing_review() -> List[Dict]:
    """
    Get products flagged as needing pricing review.
    
    Returns:
        list: Products with pricing issues
        
    Example:
        problem_products = get_products_needing_review()
    """
    query = """
        SELECT p.*, v.vendor_name, pr.variant_price, pr.cost_per_item, pr.profit_margin
        FROM products p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        WHERE pr.needs_pricing_review = true
        ORDER BY pr.profit_margin ASC
    """
    return execute_query(query)


def get_products_by_tier(tier_name: str) -> List[Dict]:
    """
    Get all products in a specific price tier.
    
    Args:
        tier_name (str): Price tier (Budget, Mid-Range, Premium, Luxury)
        
    Returns:
        list: Products in tier
        
    Example:
        premium = get_products_by_tier('Premium')
    """
    query = """
        SELECT p.*, v.vendor_name, pr.variant_price, pr.profit_margin
        FROM products p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        WHERE pr.price_tier = %s
        ORDER BY pr.variant_price DESC
    """
    return execute_query(query, (tier_name,))


def update_product_price(product_id: int, new_price: float) -> bool:
    """
    Update a product's price safely.
    
    Args:
        product_id (int): ID of product to update
        new_price (float): New price value
        
    Returns:
        bool: True if successful
        
    Example:
        success = update_product_price(123, 99.99)
    """
    query = """
        UPDATE pricing
        SET variant_price = %s,
            effective_date = CURRENT_TIMESTAMP
        WHERE product_id = %s
    """
    result = execute_query(query, (new_price, product_id), fetch=False)
    return result is not None


# ==============================================================================
# 5. ANALYTICS QUERIES
# ==============================================================================

def get_category_summary() -> List[Dict]:
    """
    Get summary statistics by category.
    
    Returns:
        list: Category stats (count, avg price, etc.)
        
    Example:
        summary = get_category_summary()
    """
    query = """
        SELECT 
            c.category_name,
            COUNT(p.product_id) as product_count,
            AVG(pr.variant_price) as avg_price,
            MIN(pr.variant_price) as min_price,
            MAX(pr.variant_price) as max_price,
            AVG(pr.profit_margin) as avg_margin
        FROM categories c
        LEFT JOIN products p ON c.category_id = p.category_id
        LEFT JOIN pricing pr ON p.product_id = pr.product_id
        GROUP BY c.category_name
        ORDER BY product_count DESC
    """
    return execute_query(query)


def get_profit_margin_distribution() -> Dict:
    """
    Get statistics on profit margin distribution.
    
    Returns:
        dict: Stats on profit margins across all products
        
    Example:
        stats = get_profit_margin_distribution()
    """
    query = """
        SELECT 
            COUNT(*) as total_products,
            AVG(profit_margin) as avg_margin,
            MIN(profit_margin) as min_margin,
            MAX(profit_margin) as max_margin,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY profit_margin) as median_margin,
            COUNT(CASE WHEN profit_margin < 0 THEN 1 END) as loss_products,
            COUNT(CASE WHEN profit_margin BETWEEN 0 AND 25 THEN 1 END) as low_margin,
            COUNT(CASE WHEN profit_margin BETWEEN 25 AND 50 THEN 1 END) as healthy_margin,
            COUNT(CASE WHEN profit_margin > 50 THEN 1 END) as high_margin
        FROM pricing
        WHERE profit_margin IS NOT NULL
    """
    results = execute_query(query)
    return results[0] if results else None


def get_quick_wins(limit: int = 20) -> List[Dict]:
    """
    Get quick win opportunities (good price, needs content improvement).
    
    Args:
        limit (int): Number of opportunities to return
        
    Returns:
        list: Quick win products
        
    Example:
        opportunities = get_quick_wins(50)
    """
    query = """
        SELECT p.*, v.vendor_name, pr.variant_price, p.content_quality_score
        FROM products p
        JOIN vendors v ON p.vendor_id = v.vendor_id
        JOIN pricing pr ON p.product_id = pr.product_id
        WHERE p.quick_win = true
        ORDER BY pr.variant_price DESC, p.content_quality_score ASC
        LIMIT %s
    """
    return execute_query(query, (limit,))


# ==============================================================================
# 6. UTILITY FUNCTIONS
# ==============================================================================

def get_database_stats() -> Dict:
    """
    Get overall database statistics.
    
    Returns:
        dict: Database stats (sizes, counts, etc.)
        
    Example:
        stats = get_database_stats()
    """
    conn = get_connection(dict_cursor=False)  # Use regular cursor
    if not conn:
        return None
    
    cursor = conn.cursor()
    stats = {}
    
    try:
        # Database size
        cursor.execute("SELECT pg_size_pretty(pg_database_size('shopify_db'))")
        stats['database_size'] = cursor.fetchone()[0]
        
        # Table counts
        cursor.execute("SELECT COUNT(*) FROM products")
        stats['product_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM vendors")
        stats['vendor_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM categories")
        stats['category_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM pricing")
        stats['pricing_records'] = cursor.fetchone()[0]
        
        # Table sizes
        cursor.execute("""
            SELECT 
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
            FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename IN ('products', 'vendors', 'categories', 'pricing')
        """)
        stats['table_sizes'] = dict(cursor.fetchall())
        
        cursor.close()
        conn.close()
        return stats
        
    except psycopg2.Error as e:
        print(f"❌ Error getting stats: {e}")
        cursor.close()
        conn.close()
        return None


def export_to_dataframe(query: str, params: tuple = None) -> pd.DataFrame:
    """
    Execute any query and return results as pandas DataFrame.
    
    Args:
        query (str): SQL query to execute
        params (tuple): Query parameters
        
    Returns:
        pd.DataFrame: Results as DataFrame
        
    Example:
        df = export_to_dataframe("SELECT * FROM products LIMIT 100")
    """
    conn = get_connection()
    if not conn:
        return None
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Error creating DataFrame: {e}")
        conn.close()
        return None


# ==============================================================================
# MAIN FUNCTION - EXAMPLES
# ==============================================================================

def main():
    """
    Example usage of all functions.
    """
    print("="*80)
    print("DATABASE FUNCTIONS LIBRARY - EXAMPLES")
    print("="*80)
    
    # Example 1: Get products by vendor
    print("\n1. Products from Perlys:")
    products = get_products_by_vendor('Perlys')
    if products:
        print(f"   Found {len(products)} products")
        print(f"   Sample: {products[0]['title'][:50]}...")
    
    # Example 2: High-value products
    print("\n2. Top 5 High-Value Products:")
    high_value = get_high_value_products(5)
    if high_value:
        for i, p in enumerate(high_value, 1):
            print(f"   {i}. {p['title'][:40]} - ${p['variant_price']} ({p['profit_margin']:.1f}% margin)")
    
    # Example 3: Vendor performance
    print("\n3. Perlys Performance:")
    perf = get_vendor_performance('Perlys')
    if perf:
        print(f"   Products: {perf['total_products']}")
        print(f"   Avg Price: ${perf['avg_price']:.2f}")
        print(f"   Avg Margin: {perf['avg_margin']:.1f}%")
    
    # Example 4: Database stats
    print("\n4. Database Statistics:")
    stats = get_database_stats()
    if stats:
        print(f"   Database Size: {stats['database_size']}")
        print(f"   Total Products: {stats['product_count']:,}")
        print(f"   Vendors: {stats['vendor_count']}")
    
    print("\n" + "="*80)
    print("✅ All functions working!")
    print("="*80)


if __name__ == "__main__":
    main()