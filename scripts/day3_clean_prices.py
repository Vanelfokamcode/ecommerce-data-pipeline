import pandas as pd
import numpy as np

print("="*80)
print("DAY 3: PRICE VALIDATION & CLEANING")
print("="*80)

# ==============================================================================
# 1. LOAD & INSPECT
# ==============================================================================
print("\n[STEP 1] Loading data...")
df = pd.read_csv('../data/shopify_products.csv')
print(f"✅ Loaded {len(df):,} products")

print("\n[STEP 1] Current state of price columns:")
print(f"\nVariant Price:")
print(f"  - Count: {df['Variant Price'].count():,}")
print(f"  - Missing: {df['Variant Price'].isna().sum():,}")
print(f"  - Min: ${df['Variant Price'].min():.2f}")
print(f"  - Max: ${df['Variant Price'].max():.2f}")
print(f"  - Mean: ${df['Variant Price'].mean():.2f}")

print(f"\nVariant Compare At Price:")
print(f"  - Count: {df['Variant Compare At Price'].count():,}")
print(f"  - Missing: {df['Variant Compare At Price'].isna().sum():,}")
if df['Variant Compare At Price'].count() > 0:
    print(f"  - Min: ${df['Variant Compare At Price'].min():.2f}")
    print(f"  - Max: ${df['Variant Compare At Price'].max():.2f}")

print(f"\nCost per item:")
print(f"  - Count: {df['Cost per item'].count():,}")
print(f"  - Missing: {df['Cost per item'].isna().sum():,}")
if df['Cost per item'].count() > 0:
    print(f"  - Min: ${df['Cost per item'].min():.2f}")
    print(f"  - Max: ${df['Cost per item'].max():.2f}")

# ==============================================================================
# 2. PRICE VALIDATION & CLEANING
# ==============================================================================
print("\n" + "="*80)
print("[STEP 2] PRICE VALIDATION")
print("="*80)

# Flag invalid prices
df['price_valid'] = True

# Check for missing prices
missing_price = df['Variant Price'].isna()
missing_count = missing_price.sum()
df.loc[missing_price, 'price_valid'] = False
print(f"\n❌ Products with missing price: {missing_count:,}")

# Check for zero prices
zero_price = df['Variant Price'] == 0
zero_count = zero_price.sum()
df.loc[zero_price, 'price_valid'] = False
print(f"❌ Products with price = $0: {zero_count:,}")

# Check for negative prices
negative_price = df['Variant Price'] < 0
negative_count = negative_price.sum()
df.loc[negative_price, 'price_valid'] = False
print(f"❌ Products with negative price: {negative_count:,}")

total_invalid_price = (~df['price_valid']).sum()
print(f"\n📊 TOTAL INVALID PRICES: {total_invalid_price:,} ({total_invalid_price/len(df)*100:.2f}%)")
print(f"✅ VALID PRICES: {df['price_valid'].sum():,} ({df['price_valid'].sum()/len(df)*100:.2f}%)")

# ==============================================================================
# 3. COMPARE AT PRICE LOGIC
# ==============================================================================
print("\n" + "="*80)
print("[STEP 3] COMPARE AT PRICE VALIDATION")
print("="*80)

df['discount_valid'] = False
df['discount_amount'] = 0.0
df['discount_percentage'] = 0.0

# Only check compare at price for products with valid prices
valid_price_mask = df['price_valid'] & df['Variant Compare At Price'].notna()

# Case 1: Compare At Price < Variant Price (impossible)
impossible_discount = valid_price_mask & (df['Variant Compare At Price'] < df['Variant Price'])
impossible_count = impossible_discount.sum()
print(f"\n❌ Products where Compare At Price < Variant Price (impossible): {impossible_count:,}")

# Case 2: Compare At Price = Variant Price (no actual discount)
no_discount = valid_price_mask & (df['Variant Compare At Price'] == df['Variant Price'])
no_discount_count = no_discount.sum()
print(f"⚠️  Products where Compare At Price = Variant Price (no discount): {no_discount_count:,}")

# Case 3: Valid discounts (Compare At Price > Variant Price)
valid_discount = valid_price_mask & (df['Variant Compare At Price'] > df['Variant Price'])
valid_discount_count = valid_discount.sum()
print(f"✅ Products with valid discounts: {valid_discount_count:,}")

# Calculate discount for valid discounts
df.loc[valid_discount, 'discount_valid'] = True
df.loc[valid_discount, 'discount_amount'] = df.loc[valid_discount, 'Variant Compare At Price'] - df.loc[valid_discount, 'Variant Price']
df.loc[valid_discount, 'discount_percentage'] = ((df.loc[valid_discount, 'Variant Compare At Price'] - df.loc[valid_discount, 'Variant Price']) / df.loc[valid_discount, 'Variant Compare At Price'] * 100)

if valid_discount_count > 0:
    print(f"\nDiscount Statistics (for valid discounts):")
    print(f"  - Average discount amount: ${df.loc[valid_discount, 'discount_amount'].mean():.2f}")
    print(f"  - Average discount percentage: {df.loc[valid_discount, 'discount_percentage'].mean():.2f}%")
    print(f"  - Max discount: ${df.loc[valid_discount, 'discount_amount'].max():.2f} ({df.loc[valid_discount, 'discount_percentage'].max():.2f}%)")

# ==============================================================================
# 4. COST PER ITEM ANALYSIS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 4] COST & PROFIT MARGIN ANALYSIS")
print("="*80)

df['profit_margin'] = np.nan

has_cost = df['Cost per item'].notna()
has_cost_count = has_cost.sum()
print(f"\n📊 Products with cost data: {has_cost_count:,} ({has_cost_count/len(df)*100:.2f}%)")

if has_cost_count > 0:
    # Check for products selling at a loss (cost > price)
    valid_price_and_cost = df['price_valid'] & has_cost & (df['Variant Price'] > 0)
    
    selling_at_loss = valid_price_and_cost & (df['Cost per item'] > df['Variant Price'])
    loss_count = selling_at_loss.sum()
    print(f"\n❌ Products selling at a LOSS (Cost > Price): {loss_count:,}")
    
    if loss_count > 0:
        avg_loss = (df.loc[selling_at_loss, 'Cost per item'] - df.loc[selling_at_loss, 'Variant Price']).mean()
        print(f"   Average loss per item: ${avg_loss:.2f}")
    
    # Calculate profit margin for valid products
    # Profit Margin = (Price - Cost) / Price * 100
    can_calculate_margin = valid_price_and_cost & (df['Variant Price'] > 0)
    df.loc[can_calculate_margin, 'profit_margin'] = ((df.loc[can_calculate_margin, 'Variant Price'] - df.loc[can_calculate_margin, 'Cost per item']) / df.loc[can_calculate_margin, 'Variant Price'] * 100)
    
    print(f"\n✅ Products with calculated profit margin: {can_calculate_margin.sum():,}")
    
    if can_calculate_margin.sum() > 0:
        print(f"\nProfit Margin Statistics:")
        print(f"  - Average margin: {df.loc[can_calculate_margin, 'profit_margin'].mean():.2f}%")
        print(f"  - Median margin: {df.loc[can_calculate_margin, 'profit_margin'].median():.2f}%")
        print(f"  - Min margin: {df.loc[can_calculate_margin, 'profit_margin'].min():.2f}%")
        print(f"  - Max margin: {df.loc[can_calculate_margin, 'profit_margin'].max():.2f}%")
        
        # Categorize margins
        high_margin = (df['profit_margin'] >= 50).sum()
        medium_margin = ((df['profit_margin'] >= 25) & (df['profit_margin'] < 50)).sum()
        low_margin = ((df['profit_margin'] >= 0) & (df['profit_margin'] < 25)).sum()
        negative_margin = (df['profit_margin'] < 0).sum()
        
        print(f"\nMargin Distribution:")
        print(f"  - High margin (≥50%): {high_margin:,}")
        print(f"  - Medium margin (25-50%): {medium_margin:,}")
        print(f"  - Low margin (0-25%): {low_margin:,}")
        print(f"  - Negative margin (<0%): {negative_margin:,}")

# ==============================================================================
# 5. SUMMARY OF ALL ISSUES
# ==============================================================================
print("\n" + "="*80)
print("[STEP 5] SUMMARY OF ISSUES FOUND")
print("="*80)

issues_summary = {
    'Missing Price': missing_count,
    'Zero Price': zero_count,
    'Negative Price': negative_count,
    'Invalid Compare At Price': impossible_count,
    'No Actual Discount': no_discount_count,
    'Selling at Loss': loss_count if has_cost_count > 0 else 0,
}

print("\nIssue Type | Count | Percentage")
print("-" * 50)
for issue, count in issues_summary.items():
    print(f"{issue:<25} | {count:>5,} | {count/len(df)*100:>6.2f}%")

total_issues = sum(issues_summary.values())
print("-" * 50)
print(f"{'TOTAL ISSUES':<25} | {total_issues:>5,} | {total_issues/len(df)*100:>6.2f}%")

# ==============================================================================
# 6. SAVE OUTPUTS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 6] SAVING OUTPUTS")
print("="*80)

# Identify all problematic rows
has_any_issue = (
    ~df['price_valid'] | 
    impossible_discount | 
    no_discount |
    (selling_at_loss if has_cost_count > 0 else False)
)

issues_df = df[has_any_issue].copy()

# Add issue description column
def describe_issues(row):
    issues = []
    if pd.isna(row['Variant Price']):
        issues.append("Missing price")
    elif row['Variant Price'] <= 0:
        issues.append("Invalid price (≤0)")
    
    if pd.notna(row['Variant Compare At Price']):
        if row['Variant Compare At Price'] < row['Variant Price']:
            issues.append("Compare price < Variant price")
        elif row['Variant Compare At Price'] == row['Variant Price']:
            issues.append("No actual discount")
    
    if pd.notna(row['Cost per item']) and row['price_valid']:
        if row['Cost per item'] > row['Variant Price']:
            issues.append("Selling at loss")
    
    return " | ".join(issues)

if len(issues_df) > 0:
    issues_df['issue_description'] = issues_df.apply(describe_issues, axis=1)
    
    # Save price issues
    issues_df.to_csv('price_issues.csv', index=False)
    print(f"\n✅ Saved {len(issues_df):,} products with issues to 'price_issues.csv'")

# Save cleaned dataset with new columns
df.to_csv('../data/shopify_clean_step3.csv', index=False)
print(f"✅ Saved cleaned dataset to '../data/shopify_clean_step3.csv'")

# ==============================================================================
# 7. BEFORE/AFTER STATISTICS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 7] BEFORE/AFTER STATISTICS")
print("="*80)

print("\n📊 ORIGINAL DATA:")
print(f"  - Total products: {len(df):,}")
print(f"  - Products with valid pricing: {df['price_valid'].sum():,}")
print(f"  - Products with issues: {total_issues:,}")

print("\n📊 NEW COLUMNS ADDED:")
print(f"  - price_valid: Flags {(~df['price_valid']).sum():,} invalid prices")
print(f"  - discount_valid: Identifies {df['discount_valid'].sum():,} valid discounts")
print(f"  - discount_amount: Calculated for {(df['discount_amount'] > 0).sum():,} products")
print(f"  - discount_percentage: Calculated for {(df['discount_percentage'] > 0).sum():,} products")
print(f"  - profit_margin: Calculated for {df['profit_margin'].notna().sum():,} products")

print("\n✅ CLEANED DATASET READY:")
print(f"  - All {len(df):,} products preserved")
print(f"  - {len([c for c in df.columns if c not in pd.read_csv('../data/shopify_products.csv').columns])} new calculated columns added")
print(f"  - {len(issues_df):,} products flagged for review")

print("\n" + "="*80)
print("DAY 3 COMPLETE! 🎉")
print("="*80)
print("\nNext steps:")
print("  1. Review 'price_issues.csv' to fix problematic products")
print("  2. Use 'shopify_clean_step3.csv' for Day 4 (text cleaning)")
print("  3. Commit your work to Git!")