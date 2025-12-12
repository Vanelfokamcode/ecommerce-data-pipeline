import pandas as pd
from datetime import datetime

# Load the CSV file
print("Loading Shopify products data...")
df = pd.read_csv('../data/shopify_products.csv')

# Create report content
report_lines = []
report_lines.append("="*80)
report_lines.append("DATA QUALITY REPORT - SHOPIFY PRODUCTS")
report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
report_lines.append(f"Total Products: {len(df):,}")
report_lines.append("="*80)

# ==============================================================================
# 1. PRICE ANALYSIS
# ==============================================================================
report_lines.append("\n" + "="*80)
report_lines.append("1. PRICE ANALYSIS")
report_lines.append("="*80)

price_zero_or_missing = df['Variant Price'].isna().sum() + (df['Variant Price'] == 0).sum()
report_lines.append(f"\nProducts with Price = 0 or missing: {price_zero_or_missing:,} ({price_zero_or_missing/len(df)*100:.2f}%)")

valid_prices = df[df['Variant Price'] > 0]['Variant Price']
report_lines.append(f"\nPrice Range:")
report_lines.append(f"  - Minimum: ${valid_prices.min():.2f}")
report_lines.append(f"  - Maximum: ${valid_prices.max():.2f}")
report_lines.append(f"  - Average: ${valid_prices.mean():.2f}")
report_lines.append(f"  - Median: ${valid_prices.median():.2f}")

has_compare_price = df['Variant Compare At Price'].notna().sum()
report_lines.append(f"\nProducts with Compare At Price (discount): {has_compare_price:,} ({has_compare_price/len(df)*100:.2f}%)")

# Calculate average discount percentage
df_with_discount = df[(df['Variant Compare At Price'].notna()) & (df['Variant Price'] > 0)]
if len(df_with_discount) > 0:
    df_with_discount['discount_pct'] = ((df_with_discount['Variant Compare At Price'] - df_with_discount['Variant Price']) / df_with_discount['Variant Compare At Price'] * 100)
    avg_discount = df_with_discount['discount_pct'].mean()
    report_lines.append(f"Average Discount Percentage: {avg_discount:.2f}%")
else:
    report_lines.append("Average Discount Percentage: N/A (no valid discounts)")

# ==============================================================================
# 2. VENDOR ANALYSIS
# ==============================================================================
report_lines.append("\n" + "="*80)
report_lines.append("2. VENDOR ANALYSIS")
report_lines.append("="*80)

unique_vendors = df['Vendor'].nunique()
report_lines.append(f"\nUnique Vendors: {unique_vendors:,}")

no_vendor = df['Vendor'].isna().sum()
report_lines.append(f"Products with no Vendor: {no_vendor:,} ({no_vendor/len(df)*100:.2f}%)")

report_lines.append(f"\nTop 10 Vendors by Product Count:")
top_vendors = df['Vendor'].value_counts().head(10)
for idx, (vendor, count) in enumerate(top_vendors.items(), 1):
    report_lines.append(f"  {idx}. {vendor}: {count:,} products ({count/len(df)*100:.2f}%)")

# ==============================================================================
# 3. CATEGORY ANALYSIS
# ==============================================================================
report_lines.append("\n" + "="*80)
report_lines.append("3. CATEGORY ANALYSIS")
report_lines.append("="*80)

unique_categories = df['Product Category'].nunique()
report_lines.append(f"\nUnique Product Categories: {unique_categories:,}")

no_category = df['Product Category'].isna().sum()
report_lines.append(f"Products with no Category: {no_category:,} ({no_category/len(df)*100:.2f}%)")

report_lines.append(f"\nTop 10 Categories by Product Count:")
top_categories = df['Product Category'].value_counts().head(10)
for idx, (category, count) in enumerate(top_categories.items(), 1):
    report_lines.append(f"  {idx}. {category}: {count:,} products ({count/len(df)*100:.2f}%)")

# ==============================================================================
# 4. INVENTORY & STATUS
# ==============================================================================
report_lines.append("\n" + "="*80)
report_lines.append("4. INVENTORY & STATUS")
report_lines.append("="*80)

report_lines.append(f"\nProduct Status Distribution:")
status_counts = df['Status'].value_counts()
for status, count in status_counts.items():
    report_lines.append(f"  - {status}: {count:,} products ({count/len(df)*100:.2f}%)")

report_lines.append(f"\nInventory Tracker Distribution:")
tracker_counts = df['Variant Inventory Tracker'].value_counts()
for tracker, count in tracker_counts.items():
    report_lines.append(f"  - {tracker}: {count:,} products ({count/len(df)*100:.2f}%)")

has_cost = df['Cost per item'].notna().sum()
report_lines.append(f"\nProducts with Cost per item data: {has_cost:,} ({has_cost/len(df)*100:.2f}%)")

# ==============================================================================
# 5. SEO & CONTENT
# ==============================================================================
report_lines.append("\n" + "="*80)
report_lines.append("5. SEO & CONTENT ANALYSIS")
report_lines.append("="*80)

missing_seo_title = df['SEO Title'].isna().sum()
report_lines.append(f"\nProducts missing SEO Title: {missing_seo_title:,} ({missing_seo_title/len(df)*100:.2f}%)")

missing_seo_desc = df['SEO Description'].isna().sum()
report_lines.append(f"Products missing SEO Description: {missing_seo_desc:,} ({missing_seo_desc/len(df)*100:.2f}%)")

no_tags = df['Tags'].isna().sum()
report_lines.append(f"Products with no Tags: {no_tags:,} ({no_tags/len(df)*100:.2f}%)")

avg_title_length = df['Title'].str.len().mean()
report_lines.append(f"\nAverage Title Length: {avg_title_length:.0f} characters")

# ==============================================================================
# 6. OPTIONS/VARIANTS
# ==============================================================================
report_lines.append("\n" + "="*80)
report_lines.append("6. OPTIONS/VARIANTS ANALYSIS")
report_lines.append("="*80)

has_option1 = df['Option1 Name'].notna().sum()
has_option2 = df['Option2 Name'].notna().sum()
has_option3 = df['Option3 Name'].notna().sum()

report_lines.append(f"\nProducts using options:")
report_lines.append(f"  - Option1: {has_option1:,} products ({has_option1/len(df)*100:.2f}%)")
report_lines.append(f"  - Option2: {has_option2:,} products ({has_option2/len(df)*100:.2f}%)")
report_lines.append(f"  - Option3: {has_option3:,} products ({has_option3/len(df)*100:.2f}%)")

report_lines.append(f"\nMost common Option1 names:")
top_option1 = df['Option1 Name'].value_counts().head(5)
for option, count in top_option1.items():
    report_lines.append(f"  - {option}: {count:,} products")

if has_option2 > 0:
    report_lines.append(f"\nMost common Option2 names:")
    top_option2 = df['Option2 Name'].value_counts().head(5)
    for option, count in top_option2.items():
        report_lines.append(f"  - {option}: {count:,} products")

# ==============================================================================
# SUMMARY & KEY FINDINGS
# ==============================================================================
report_lines.append("\n" + "="*80)
report_lines.append("KEY FINDINGS & RECOMMENDATIONS")
report_lines.append("="*80)

findings = []

if price_zero_or_missing > 0:
    findings.append(f"⚠️  {price_zero_or_missing:,} products have invalid pricing - needs cleanup")

if missing_seo_desc/len(df) > 0.5:
    findings.append(f"⚠️  {missing_seo_desc/len(df)*100:.0f}% products missing SEO descriptions - SEO optimization needed")

if no_tags/len(df) > 0.3:
    findings.append(f"⚠️  {no_tags/len(df)*100:.0f}% products have no tags - consider adding tags for better organization")

if has_cost/len(df) < 0.5:
    findings.append(f"⚠️  Only {has_cost/len(df)*100:.0f}% products have cost data - profit analysis limited")

if has_compare_price/len(df) > 0.2:
    findings.append(f"✅ {has_compare_price/len(df)*100:.0f}% products have discount pricing - good for promotions")

if len(findings) == 0:
    findings.append("✅ Data quality looks good overall!")

for finding in findings:
    report_lines.append(f"\n{finding}")

report_lines.append("\n" + "="*80)
report_lines.append("END OF REPORT")
report_lines.append("="*80)

# Save to file
report_text = "\n".join(report_lines)
with open('data_quality_report.txt', 'w', encoding='utf-8') as f:
    f.write(report_text)

# Print to console
print(report_text)

print("\n✅ Report saved to 'data_quality_report.txt'")