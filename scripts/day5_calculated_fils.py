import pandas as pd
import numpy as np

print("="*80)
print("DAY 5: BUSINESS INTELLIGENCE & CALCULATED FIELDS")
print("="*80)

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================
print("\n[STEP 1] Loading cleaned data from Day 4...")
df = pd.read_csv('../data/shopify_clean_step4.csv')
print(f"✅ Loaded {len(df):,} products with {len(df.columns)} columns")

# ==============================================================================
# 2. PRICE TIER CLASSIFICATION
# ==============================================================================
print("\n" + "="*80)
print("[STEP 2] PRICE TIER CLASSIFICATION")
print("="*80)

def classify_price_tier(price):
    if pd.isna(price) or price <= 0:
        return "Invalid"
    elif price < 30:
        return "Budget"
    elif price < 80:
        return "Mid-Range"
    elif price < 150:
        return "Premium"
    else:
        return "Luxury"

df['price_tier'] = df['Variant Price'].apply(classify_price_tier)

print("\n📊 Price Tier Distribution:")
tier_dist = df['price_tier'].value_counts()
for tier, count in tier_dist.items():
    print(f"  {tier:<12}: {count:>5,} products ({count/len(df)*100:>5.1f}%)")

# Calculate average profit margin by tier
print("\n💰 Average Profit Margin by Price Tier:")
for tier in ['Budget', 'Mid-Range', 'Premium', 'Luxury']:
    tier_data = df[df['price_tier'] == tier]
    if 'profit_margin' in df.columns:
        avg_margin = tier_data['profit_margin'].mean()
        if pd.notna(avg_margin):
            print(f"  {tier:<12}: {avg_margin:>6.2f}%")
        else:
            print(f"  {tier:<12}: No cost data")

# ==============================================================================
# 3. DISCOUNT STRATEGY ANALYSIS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 3] DISCOUNT STRATEGY ANALYSIS")
print("="*80)

def classify_discount_strategy(row):
    if pd.isna(row['Variant Compare At Price']) or row['Variant Compare At Price'] == 0:
        return "No Discount"
    
    if not row['discount_valid']:
        return "Invalid Discount"
    
    discount_pct = row['discount_percentage']
    
    if discount_pct < 1:
        return "No Discount"
    elif discount_pct <= 15:
        return "Small Discount"
    elif discount_pct <= 30:
        return "Medium Discount"
    else:
        return "Large Discount"

df['discount_strategy'] = df.apply(classify_discount_strategy, axis=1)

print("\n📊 Discount Strategy Distribution:")
discount_dist = df['discount_strategy'].value_counts()
for strategy, count in discount_dist.items():
    print(f"  {strategy:<17}: {count:>5,} products ({count/len(df)*100:>5.1f}%)")

print("\n🎯 Discount Usage by Price Tier:")
discount_by_tier = pd.crosstab(df['price_tier'], df['discount_strategy'], normalize='index') * 100
print(discount_by_tier.round(1))

# ==============================================================================
# 4. PROFIT MARGIN CATEGORIES
# ==============================================================================
print("\n" + "="*80)
print("[STEP 4] PROFIT MARGIN CATEGORIES")
print("="*80)

def classify_profit_margin(margin):
    if pd.isna(margin):
        return "No Cost Data"
    elif margin < 0:
        return "Loss"
    elif margin < 25:
        return "Low Margin"
    elif margin < 50:
        return "Healthy Margin"
    else:
        return "High Margin"

df['profit_category'] = df['profit_margin'].apply(classify_profit_margin)

print("\n📊 Profit Margin Distribution:")
profit_dist = df['profit_category'].value_counts()
for category, count in profit_dist.items():
    print(f"  {category:<16}: {count:>5,} products ({count/len(df)*100:>5.1f}%)")

# Profit by price tier
print("\n💼 Profit Categories by Price Tier:")
has_margin = df[df['profit_category'] != 'No Cost Data']
if len(has_margin) > 0:
    profit_by_tier = pd.crosstab(has_margin['price_tier'], has_margin['profit_category'])
    print(profit_by_tier)

# Flag products needing review
needs_review = df[df['profit_category'] == 'Loss']
print(f"\n⚠️  Products selling at a LOSS: {len(needs_review):,}")

# ==============================================================================
# 5. INVENTORY HEALTH SCORE
# ==============================================================================
print("\n" + "="*80)
print("[STEP 5] INVENTORY HEALTH SCORE")
print("="*80)

def calculate_inventory_score(row):
    score = 0
    
    # Has inventory tracking (25 points)
    if pd.notna(row['Variant Inventory Tracker']) and row['Variant Inventory Tracker'] != '':
        score += 25
    
    # Has valid price (25 points)
    if row.get('price_valid', False):
        score += 25
    
    # Has cost data (25 points)
    if pd.notna(row['Cost per item']) and row['Cost per item'] > 0:
        score += 25
    
    # Not selling at loss (25 points)
    if row['profit_category'] not in ['Loss', 'No Cost Data']:
        score += 25
    elif row['profit_category'] == 'No Cost Data':
        score += 12  # Partial credit if no cost data
    
    return score

df['inventory_health_score'] = df.apply(calculate_inventory_score, axis=1)

def classify_inventory_status(score):
    if score >= 90:
        return "Excellent"
    elif score >= 70:
        return "Good"
    elif score >= 50:
        return "Needs Attention"
    else:
        return "Critical"

df['inventory_status'] = df['inventory_health_score'].apply(classify_inventory_status)

print("\n📊 Inventory Health Distribution:")
health_dist = df['inventory_status'].value_counts()
for status, count in health_dist.items():
    print(f"  {status:<17}: {count:>5,} products ({count/len(df)*100:>5.1f}%)")

print(f"\n⚠️  Products needing attention: {(df['inventory_health_score'] < 70).sum():,}")

# ==============================================================================
# 6. PRODUCT COMPLEXITY
# ==============================================================================
print("\n" + "="*80)
print("[STEP 6] PRODUCT COMPLEXITY ANALYSIS")
print("="*80)

def classify_variant_complexity(row):
    has_option1 = pd.notna(row['Option1 Name']) and row['Option1 Name'] != ''
    has_option2 = pd.notna(row['Option2 Name']) and row['Option2 Name'] != ''
    has_option3 = pd.notna(row['Option3 Name']) and row['Option3 Name'] != ''
    
    if has_option3:
        return "Complex"
    elif has_option2:
        return "Medium"
    elif has_option1:
        return "Simple"
    else:
        return "Simple"

df['variant_complexity'] = df.apply(classify_variant_complexity, axis=1)

print("\n📊 Product Complexity Distribution:")
complexity_dist = df['variant_complexity'].value_counts()
for complexity, count in complexity_dist.items():
    print(f"  {complexity:<10}: {count:>5,} products ({count/len(df)*100:>5.1f}%)")

print("\n💰 Average Price by Complexity:")
for complexity in ['Simple', 'Medium', 'Complex']:
    complex_data = df[df['variant_complexity'] == complexity]
    avg_price = complex_data['Variant Price'].mean()
    print(f"  {complexity:<10}: ${avg_price:>7.2f}")

# ==============================================================================
# 7. CONTENT COMPLETENESS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 7] CONTENT COMPLETENESS ANALYSIS")
print("="*80)

def classify_content_tier(score):
    if pd.isna(score):
        return "Unknown"
    elif score >= 90:
        return "Excellent"
    elif score >= 70:
        return "Good"
    elif score >= 50:
        return "Needs Work"
    else:
        return "Poor"

df['content_tier'] = df['content_quality_score'].apply(classify_content_tier)

print("\n📊 Content Quality Distribution:")
content_dist = df['content_tier'].value_counts()
for tier, count in content_dist.items():
    print(f"  {tier:<12}: {count:>5,} products ({count/len(df)*100:>5.1f}%)")

print("\n🎯 Content Quality by Price Tier:")
for price_tier in ['Budget', 'Mid-Range', 'Premium', 'Luxury']:
    tier_data = df[df['price_tier'] == price_tier]
    avg_content = tier_data['content_quality_score'].mean()
    print(f"  {price_tier:<12}: {avg_content:>5.1f}/100")

# ==============================================================================
# 8. VENDOR PERFORMANCE METRICS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 8] VENDOR PERFORMANCE ANALYSIS")
print("="*80)

vendor_metrics = df.groupby('Vendor').agg({
    'Variant Price': 'mean',
    'profit_margin': 'mean',
    'content_quality_score': 'mean',
    'Handle': 'count'
}).round(2)

vendor_metrics.columns = ['avg_price', 'avg_margin', 'avg_content_score', 'total_products']
vendor_metrics = vendor_metrics.sort_values('avg_margin', ascending=False)

print("\n🏆 Top 10 Vendors by Profit Margin:")
print("\nVendor                    | Products | Avg Price | Avg Margin | Content Score")
print("-" * 80)
for vendor, row in vendor_metrics.head(10).iterrows():
    margin_str = f"{row['avg_margin']:.1f}%" if pd.notna(row['avg_margin']) else "N/A"
    print(f"{vendor:<25} | {row['total_products']:>8,} | ${row['avg_price']:>8.2f} | {margin_str:>10} | {row['avg_content_score']:>5.1f}")

# Classify vendor performance
def classify_vendor_tier(vendor_name):
    vendor_data = df[df['Vendor'] == vendor_name]
    
    avg_margin = vendor_data['profit_margin'].mean()
    avg_content = vendor_data['content_quality_score'].mean()
    
    if pd.notna(avg_margin) and avg_margin >= 40 and avg_content >= 70:
        return "Top Performer"
    elif pd.notna(avg_margin) and (avg_margin < 20 or avg_content < 50):
        return "Needs Attention"
    else:
        return "Standard"

df['vendor_tier'] = df['Vendor'].apply(classify_vendor_tier)

print("\n📊 Vendor Performance Distribution:")
vendor_tier_dist = df['vendor_tier'].value_counts()
for tier, count in vendor_tier_dist.items():
    print(f"  {tier:<17}: {count:>5,} products")

# ==============================================================================
# 9. BUSINESS INTELLIGENCE FLAGS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 9] BUSINESS INTELLIGENCE FLAGS")
print("="*80)

# Flag: Needs pricing review
df['needs_pricing_review'] = (
    (df['profit_category'] == 'Loss') | 
    (df['Variant Price'] <= 0) |
    (df['price_valid'] == False)
)

# Flag: Needs content update
df['needs_content_update'] = df['content_quality_score'] < 60

# Flag: High-value product
df['high_value_product'] = (
    (df['price_tier'].isin(['Premium', 'Luxury'])) &
    (df['profit_category'].isin(['Healthy Margin', 'High Margin'])) &
    (df['content_quality_score'] >= 70)
)

# Flag: Quick win (easy improvements)
df['quick_win'] = (
    (df['price_tier'].isin(['Mid-Range', 'Premium'])) &
    (df['content_quality_score'] < 70) &
    (df['price_valid'] == True)
)

# Flag: Discount opportunity
# Products with no discount in categories where discounts are common
category_discount_rate = df.groupby('Product Category')['discount_valid'].mean()
high_discount_categories = category_discount_rate[category_discount_rate > 0.3].index

df['discount_opportunity'] = (
    (df['discount_strategy'] == 'No Discount') &
    (df['Product Category'].isin(high_discount_categories)) &
    (df['price_tier'].isin(['Mid-Range', 'Premium']))
)

print("\n🚨 Actionable Flags Summary:")
print(f"  Needs Pricing Review:    {df['needs_pricing_review'].sum():>5,} products")
print(f"  Needs Content Update:    {df['needs_content_update'].sum():>5,} products")
print(f"  High-Value Products:     {df['high_value_product'].sum():>5,} products")
print(f"  Quick Win Opportunities: {df['quick_win'].sum():>5,} products")
print(f"  Discount Opportunities:  {df['discount_opportunity'].sum():>5,} products")

# ==============================================================================
# 10. GENERATE INSIGHTS REPORT
# ==============================================================================
print("\n" + "="*80)
print("[STEP 10] BUSINESS INTELLIGENCE INSIGHTS")
print("="*80)

# Save data
df.to_csv('../data/shopify_clean_step5.csv', index=False)
print(f"\n✅ Saved enhanced dataset to '../data/shopify_clean_step5.csv'")

# Generate business intelligence report
report = []
report.append("="*80)
report.append("BUSINESS INTELLIGENCE REPORT - DAY 5")
report.append("="*80)
report.append(f"\nGenerated for: {len(df):,} products")
report.append(f"\n{'='*80}")
report.append("\n🎯 KEY BUSINESS INSIGHTS:")
report.append("\n" + "="*80)

# Best performing price tier
best_tier = None
best_margin = -999
for tier in ['Budget', 'Mid-Range', 'Premium', 'Luxury']:
    tier_data = df[df['price_tier'] == tier]
    avg_margin = tier_data['profit_margin'].mean()
    if pd.notna(avg_margin) and avg_margin > best_margin:
        best_margin = avg_margin
        best_tier = tier

if best_tier:
    report.append(f"\n💰 MOST PROFITABLE TIER: {best_tier}")
    report.append(f"   Average margin: {best_margin:.2f}%")
    tier_count = (df['price_tier'] == best_tier).sum()
    report.append(f"   Products in tier: {tier_count:,}")

# Top vendors by margin
report.append(f"\n🏆 TOP PERFORMING VENDORS:")
top_vendors = vendor_metrics.head(5)
for i, (vendor, row) in enumerate(top_vendors.iterrows(), 1):
    if pd.notna(row['avg_margin']):
        report.append(f"   {i}. {vendor}: {row['avg_margin']:.1f}% margin, {int(row['total_products'])} products")

# Critical issues
report.append(f"\n⚠️  PRODUCTS REQUIRING IMMEDIATE ATTENTION:")
report.append(f"   - Selling at Loss: {df[df['profit_category'] == 'Loss'].shape[0]:,} products")
report.append(f"   - Invalid Pricing: {df[~df['price_valid']].shape[0]:,} products")
report.append(f"   - Poor Content: {df[df['content_tier'] == 'Poor'].shape[0]:,} products")

# Opportunities
report.append(f"\n💡 REVENUE OPTIMIZATION OPPORTUNITIES:")
report.append(f"   - Quick Wins (improve content): {df['quick_win'].sum():,} products")
report.append(f"   - Discount Opportunities: {df['discount_opportunity'].sum():,} products")
report.append(f"   - High-Value Products: {df['high_value_product'].sum():,} products")

# Content vs Price correlation
report.append(f"\n📊 CONTENT QUALITY IMPACT:")
for tier in ['Budget', 'Mid-Range', 'Premium', 'Luxury']:
    tier_data = df[df['price_tier'] == tier]
    avg_content = tier_data['content_quality_score'].mean()
    report.append(f"   {tier:<12}: {avg_content:.1f}/100 average content score")

# Complexity analysis
report.append(f"\n🔧 PRODUCT COMPLEXITY INSIGHTS:")
for complexity in ['Simple', 'Medium', 'Complex']:
    complex_data = df[df['variant_complexity'] == complexity]
    avg_price = complex_data['Variant Price'].mean()
    count = len(complex_data)
    report.append(f"   {complexity:<10}: {count:>5,} products, ${avg_price:.2f} avg price")

# Recommended actions
report.append(f"\n{'='*80}")
report.append("\n📋 RECOMMENDED ACTIONS:")
report.append("="*80)

actions = []
if df['needs_pricing_review'].sum() > 0:
    actions.append(f"1. URGENT: Review {df['needs_pricing_review'].sum():,} products with pricing issues")

if df['quick_win'].sum() > 100:
    actions.append(f"2. QUICK WIN: Improve content for {df['quick_win'].sum():,} mid-tier products (easy revenue boost)")

if df['discount_opportunity'].sum() > 0:
    actions.append(f"3. STRATEGY: Consider discounts for {df['discount_opportunity'].sum():,} products in high-discount categories")

if (df['content_tier'] == 'Poor').sum() > 1000:
    actions.append(f"4. CONTENT: {(df['content_tier'] == 'Poor').sum():,} products need content overhaul")

actions.append(f"5. FOCUS: {df['high_value_product'].sum():,} high-value products are your profit drivers - protect them")

for action in actions:
    report.append(f"\n{action}")

report.append(f"\n{'='*80}")
report.append("\n✅ NEW COLUMNS ADDED:")
report.append("   - price_tier: Budget/Mid-Range/Premium/Luxury")
report.append("   - discount_strategy: Discount categorization")
report.append("   - profit_category: Margin classification")
report.append("   - inventory_health_score: 0-100 health metric")
report.append("   - inventory_status: Overall inventory health")
report.append("   - variant_complexity: Product variant complexity")
report.append("   - content_tier: Content quality tier")
report.append("   - vendor_tier: Vendor performance classification")
report.append("   - needs_pricing_review: Action flag")
report.append("   - needs_content_update: Action flag")
report.append("   - high_value_product: Premium product flag")
report.append("   - quick_win: Opportunity flag")
report.append("   - discount_opportunity: Strategy flag")

report.append(f"\n{'='*80}")
report.append("END OF BUSINESS INTELLIGENCE REPORT")
report.append("="*80)

report_text = "\n".join(report)

with open('business_intelligence_report.txt', 'w', encoding='utf-8') as f:
    f.write(report_text)

print(f"✅ Saved business intelligence report to 'business_intelligence_report.txt'")

# Print key insights to console
print("\n" + "="*80)
print("KEY TAKEAWAYS")
print("="*80)
print(f"\n✅ Added 13 new business intelligence columns")
print(f"✅ Identified {df['quick_win'].sum():,} quick win opportunities")
print(f"✅ Flagged {df['needs_pricing_review'].sum():,} products for pricing review")
print(f"✅ Found {df['high_value_product'].sum():,} high-value products")
if best_tier:
    print(f"✅ {best_tier} tier is most profitable at {best_margin:.1f}% margin")

print("\n" + "="*80)
print("DAY 5 COMPLETE! 🎉")
print("="*80)
print("\nYou now have:")
print("  ✅ Business intelligence flags for decision-making")
print("  ✅ Profit and pricing analysis")
print("  ✅ Vendor performance rankings")
print("  ✅ Actionable insights for revenue optimization")
print("\nReady for Day 6: Data Validation & Quality Checks!")
print("="*80)