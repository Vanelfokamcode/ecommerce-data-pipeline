import pandas as pd
import numpy as np
from collections import defaultdict

print("="*80)
print("DAY 6: DATA VALIDATION & QUALITY GATES")
print("="*80)

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================
print("\n[STEP 1] Loading data from Day 5...")
df = pd.read_csv('../data/shopify_clean_step5.csv')
print(f"✅ Loaded {len(df):,} products for validation")

# Initialize validation tracking
validation_results = []
violations_by_product = defaultdict(list)

def add_validation(rule_name, severity, passed, failed_products, recommendation):
    """Track validation results"""
    validation_results.append({
        'rule': rule_name,
        'severity': severity,
        'passed': passed,
        'violations': len(failed_products),
        'failed_products': failed_products,
        'recommendation': recommendation
    })
    
    # Track violations per product
    for idx in failed_products.index:
        violations_by_product[idx].append({
            'rule': rule_name,
            'severity': severity
        })

# ==============================================================================
# 2. CRITICAL BUSINESS RULES VALIDATION
# ==============================================================================
print("\n" + "="*80)
print("[STEP 2] RUNNING BUSINESS RULES VALIDATION")
print("="*80)

print("\n🔍 PRICING RULES:")

# Rule: No product should have price <= 0
invalid_price = df[df['Variant Price'] <= 0]
add_validation(
    "Valid Price (> 0)",
    "CRITICAL",
    len(df) - len(invalid_price),
    invalid_price,
    "Fix pricing immediately - these products cannot be sold"
)
print(f"  ✓ Price > 0: {len(invalid_price)} violations")

# Rule: Compare At Price >= Variant Price
invalid_compare = df[
    (df['Variant Compare At Price'].notna()) & 
    (df['Variant Compare At Price'] < df['Variant Price'])
]
add_validation(
    "Compare At Price Logic",
    "HIGH",
    len(df) - len(invalid_compare),
    invalid_compare,
    "Fix discount pricing logic - compare price must be higher than sale price"
)
print(f"  ✓ Compare price logic: {len(invalid_compare)} violations")

# Rule: Cost < Price (not selling at loss)
selling_at_loss = df[
    (df['Cost per item'].notna()) & 
    (df['Cost per item'] > df['Variant Price'])
]
add_validation(
    "Not Selling at Loss",
    "HIGH",
    len(df) - len(selling_at_loss),
    selling_at_loss,
    "Review pricing strategy - these products lose money on each sale"
)
print(f"  ✓ Not selling at loss: {len(selling_at_loss)} violations")

# Rule: Price within reasonable range of category average
category_stats = df.groupby('Product Category')['Variant Price'].agg(['mean', 'std'])
suspicious_prices = []
for idx, row in df.iterrows():
    if row['Product Category'] in category_stats.index:
        cat_mean = category_stats.loc[row['Product Category'], 'mean']
        cat_std = category_stats.loc[row['Product Category'], 'std']
        if pd.notna(cat_std) and cat_std > 0:
            if abs(row['Variant Price'] - cat_mean) > 3 * cat_std:
                suspicious_prices.append(idx)

suspicious_df = df.loc[suspicious_prices]
add_validation(
    "Price Within Category Range",
    "MEDIUM",
    len(df) - len(suspicious_df),
    suspicious_df,
    "Review pricing - significantly different from category average"
)
print(f"  ✓ Price anomalies: {len(suspicious_df)} violations")

print("\n🔍 CONTENT RULES:")

# Rule: Title length 10-200 characters
invalid_title_length = df[
    (df['Title'].str.len() < 10) | 
    (df['Title'].str.len() > 200)
]
add_validation(
    "Title Length (10-200 chars)",
    "CRITICAL",
    len(df) - len(invalid_title_length),
    invalid_title_length,
    "Fix titles - too short or too long for proper display"
)
print(f"  ✓ Valid title length: {len(invalid_title_length)} violations")

# Rule: SEO Title exists
missing_seo_title = df[df['SEO Title'].isna() | (df['SEO Title'] == '')]
add_validation(
    "SEO Title Exists",
    "HIGH",
    len(df) - len(missing_seo_title),
    missing_seo_title,
    "Generate SEO titles for search engine optimization"
)
print(f"  ✓ Has SEO title: {len(missing_seo_title)} violations")

# Rule: SEO Description 50-160 characters
invalid_seo_desc = df[
    (df['SEO Description'].str.len() < 50) | 
    (df['SEO Description'].str.len() > 160)
]
add_validation(
    "SEO Description Length (50-160)",
    "MEDIUM",
    len(df) - len(invalid_seo_desc),
    invalid_seo_desc,
    "Optimize SEO descriptions for search engines"
)
print(f"  ✓ Valid SEO description: {len(invalid_seo_desc)} violations")

# Rule: Premium/Luxury need good content
premium_poor_content = df[
    (df['price_tier'].isin(['Premium', 'Luxury'])) & 
    (df['content_quality_score'] < 70)
]
add_validation(
    "Premium Products Need Quality Content",
    "HIGH",
    len(df[df['price_tier'].isin(['Premium', 'Luxury'])]) - len(premium_poor_content),
    premium_poor_content,
    "Improve content for premium products - they deserve better presentation"
)
print(f"  ✓ Premium content quality: {len(premium_poor_content)} violations")

print("\n🔍 INVENTORY RULES:")

# Rule: Published products must have valid prices
published_invalid_price = df[
    (df['Published'] == 'TRUE') & 
    ((df['Variant Price'] <= 0) | (df['Variant Price'].isna()))
]
add_validation(
    "Published Products Have Valid Prices",
    "CRITICAL",
    len(df[df['Published'] == 'TRUE']) - len(published_invalid_price),
    published_invalid_price,
    "Critical: Published products with invalid prices will break storefront"
)
print(f"  ✓ Published with valid price: {len(published_invalid_price)} violations")

# Rule: Active products should have inventory tracking
active_no_tracking = df[
    (df['Status'] == 'active') & 
    ((df['Variant Inventory Tracker'].isna()) | (df['Variant Inventory Tracker'] == ''))
]
add_validation(
    "Active Products Have Inventory Tracking",
    "MEDIUM",
    len(df[df['Status'] == 'active']) - len(active_no_tracking),
    active_no_tracking,
    "Enable inventory tracking to prevent overselling"
)
print(f"  ✓ Active with tracking: {len(active_no_tracking)} violations")

# Rule: Products with variants need Option1 Name
has_variants = df['Option1 Value'].notna()
variant_no_name = df[has_variants & (df['Option1 Name'].isna() | (df['Option1 Name'] == ''))]
add_validation(
    "Variants Have Option Names",
    "HIGH",
    len(df[has_variants]) - len(variant_no_name),
    variant_no_name,
    "Fix variant configuration - option names are required"
)
print(f"  ✓ Variants properly named: {len(variant_no_name)} violations")

print("\n🔍 VENDOR/CATEGORY RULES:")

# Rule: Vendor cannot be empty or Unknown
invalid_vendor = df[(df['Vendor'].isna()) | (df['Vendor'] == 'Unknown Vendor') | (df['Vendor'] == '')]
add_validation(
    "Valid Vendor",
    "MEDIUM",
    len(df) - len(invalid_vendor),
    invalid_vendor,
    "Assign proper vendors for inventory management"
)
print(f"  ✓ Has valid vendor: {len(invalid_vendor)} violations")

# Rule: Premium/Luxury shouldn't be uncategorized
premium_uncategorized = df[
    (df['price_tier'].isin(['Premium', 'Luxury'])) & 
    (df['Product Category'] == 'Uncategorized')
]
add_validation(
    "Premium Products Categorized",
    "HIGH",
    len(df[df['price_tier'].isin(['Premium', 'Luxury'])]) - len(premium_uncategorized),
    premium_uncategorized,
    "Categorize premium products for better discoverability"
)
print(f"  ✓ Premium categorized: {len(premium_uncategorized)} violations")

print("\n🔍 BUSINESS LOGIC RULES:")

# Rule: High-value products should have good content
high_value_poor_content = df[
    (df['high_value_product'] == True) & 
    (df['content_quality_score'] < 70)
]
add_validation(
    "High-Value Products Have Quality Content",
    "HIGH",
    len(df[df['high_value_product'] == True]) - len(high_value_poor_content),
    high_value_poor_content,
    "Improve content for profit drivers - these products deserve priority"
)
print(f"  ✓ High-value with good content: {len(high_value_poor_content)} violations")

# Rule: Pricing review threshold < 5%
needs_pricing_review_count = df['needs_pricing_review'].sum()
pricing_review_threshold = len(df) * 0.05
pricing_review_pass = needs_pricing_review_count < pricing_review_threshold
add_validation(
    "Pricing Review < 5% of Products",
    "MEDIUM",
    len(df) if pricing_review_pass else 0,
    df[df['needs_pricing_review'] == True] if not pricing_review_pass else pd.DataFrame(),
    f"Too many products need pricing review ({needs_pricing_review_count/len(df)*100:.1f}%)"
)
print(f"  ✓ Pricing review threshold: {'PASS' if pricing_review_pass else 'FAIL'} ({needs_pricing_review_count} products)")

# Rule: Reasonable discount percentages
excessive_discount = df[df['discount_percentage'] >= 80]
add_validation(
    "Reasonable Discount Percentage",
    "MEDIUM",
    len(df) - len(excessive_discount),
    excessive_discount,
    "Review excessive discounts - may indicate pricing errors"
)
print(f"  ✓ Reasonable discounts: {len(excessive_discount)} violations")

# ==============================================================================
# 3. ANOMALY DETECTION
# ==============================================================================
print("\n" + "="*80)
print("[STEP 3] ANOMALY DETECTION")
print("="*80)

# Duplicate titles
duplicate_titles = df[df.duplicated(subset=['Title'], keep=False)]
add_validation(
    "No Duplicate Titles",
    "LOW",
    len(df) - len(duplicate_titles),
    duplicate_titles,
    "Review duplicate titles - may be data entry errors"
)
print(f"  ✓ Duplicate titles found: {len(duplicate_titles)} products")

# Unusual title lengths by category
unusual_titles = []
for category in df['Product Category'].unique():
    cat_data = df[df['Product Category'] == category]
    if len(cat_data) > 5:  # Only for categories with enough data
        mean_len = cat_data['title_length'].mean()
        std_len = cat_data['title_length'].std()
        if pd.notna(std_len) and std_len > 0:
            outliers = cat_data[abs(cat_data['title_length'] - mean_len) > 3 * std_len]
            unusual_titles.extend(outliers.index.tolist())

unusual_titles_df = df.loc[unusual_titles] if unusual_titles else pd.DataFrame()
add_validation(
    "Title Length Normal for Category",
    "LOW",
    len(df) - len(unusual_titles_df),
    unusual_titles_df,
    "Review unusual title lengths for consistency"
)
print(f"  ✓ Unusual title lengths: {len(unusual_titles_df)} products")

# ==============================================================================
# 4-6. VALIDATION SCORING & REPORTING
# ==============================================================================
print("\n" + "="*80)
print("[STEP 4-6] CALCULATING VALIDATION SCORE")
print("="*80)

# Calculate score
total_score = 100
severity_weights = {
    'CRITICAL': 10,
    'HIGH': 5,
    'MEDIUM': 2,
    'LOW': 1
}

for result in validation_results:
    if result['violations'] > 0:
        penalty = min(severity_weights[result['severity']], result['violations'] * 0.1)
        total_score -= penalty

total_score = max(0, total_score)

# Count by severity
critical_count = sum(1 for r in validation_results if r['severity'] == 'CRITICAL' and r['violations'] > 0)
high_count = sum(1 for r in validation_results if r['severity'] == 'HIGH' and r['violations'] > 0)
medium_count = sum(1 for r in validation_results if r['severity'] == 'MEDIUM' and r['violations'] > 0)
low_count = sum(1 for r in validation_results if r['severity'] == 'LOW' and r['violations'] > 0)

print(f"\n📊 VALIDATION SUMMARY:")
print(f"  Total Rules Checked: {len(validation_results)}")
print(f"  Overall Quality Score: {total_score:.1f}/100")
print(f"\nViolations by Severity:")
print(f"  🔴 CRITICAL: {critical_count} rules failed")
print(f"  🟠 HIGH:     {high_count} rules failed")
print(f"  🟡 MEDIUM:   {medium_count} rules failed")
print(f"  🟢 LOW:      {low_count} rules failed")

# ==============================================================================
# 7. CREATE VIOLATION DETAILS FILE
# ==============================================================================
print("\n" + "="*80)
print("[STEP 7] GENERATING VIOLATION DETAILS")
print("="*80)

violation_records = []
for idx, violations in violations_by_product.items():
    row = df.loc[idx]
    severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
    highest_severity = min(violations, key=lambda x: severity_order[x['severity']])['severity']
    
    violation_records.append({
        'Handle': row['Handle'],
        'Title': row['Title'],
        'Vendor': row['Vendor'],
        'Price': row['Variant Price'],
        'validation_failures': ', '.join([v['rule'] for v in violations]),
        'failure_count': len(violations),
        'severity': highest_severity
    })

if violation_records:
    violations_df = pd.DataFrame(violation_records)
    violations_df = violations_df.sort_values('severity')
    violations_df.to_csv('validation_failures.csv', index=False)
    print(f"✅ Saved {len(violation_records):,} products with violations to 'validation_failures.csv'")
else:
    print("✅ No violations found!")

# ==============================================================================
# 8. AUTOMATED QUALITY GATES
# ==============================================================================
print("\n" + "="*80)
print("[STEP 8] QUALITY GATE ASSESSMENT")
print("="*80)

if total_score >= 90:
    status = "PRODUCTION READY ✅"
    status_color = "🟢"
    recommendation = "Data quality is excellent. Ready for dashboards, APIs, and production use."
elif total_score >= 70:
    status = "NEEDS REVIEW ⚠️"
    status_color = "🟡"
    recommendation = "Data quality is acceptable but has issues. Review and fix high-priority violations before production."
else:
    status = "CRITICAL ISSUES ❌"
    status_color = "🔴"
    recommendation = "Data quality is poor. Must fix critical issues before proceeding."

print(f"\n{status_color} STATUS: {status}")
print(f"Quality Score: {total_score:.1f}/100")
print(f"\n💡 RECOMMENDATION:")
print(f"   {recommendation}")

# ==============================================================================
# 9. VALIDATION SUMMARY DASHBOARD
# ==============================================================================
print("\n" + "="*80)
print("[STEP 9] VALIDATION DASHBOARD")
print("="*80)

print(f"\n📋 VALIDATION SUMMARY:")
print(f"   Total Products Validated: {len(df):,}")
print(f"   Overall Quality Score: {total_score:.1f}/100")
print(f"   Products with Violations: {len(violation_records):,} ({len(violation_records)/len(df)*100:.1f}%)")
print(f"   Critical Issues: {critical_count} rules")
print(f"   High Priority Issues: {high_count} rules")

# Top violations
print(f"\n🔝 TOP 5 MOST COMMON VIOLATIONS:")
violation_counts = [(r['rule'], r['violations']) for r in validation_results if r['violations'] > 0]
violation_counts.sort(key=lambda x: x[1], reverse=True)
for i, (rule, count) in enumerate(violation_counts[:5], 1):
    print(f"   {i}. {rule}: {count:,} products ({count/len(df)*100:.1f}%)")

print(f"\n{'='*80}")
print(f"VALIDATION STATUS: {status}")
print(f"{'='*80}")

# ==============================================================================
# 10. SAVE OUTPUTS
# ==============================================================================
print("\n" + "="*80)
print("[STEP 10] SAVING VALIDATION OUTPUTS")
print("="*80)

# Add validation status to dataset
df['validation_status'] = 'PASS'
for idx in violations_by_product.keys():
    df.loc[idx, 'validation_status'] = 'FAIL'

df['validation_score'] = total_score
df.to_csv('../data/shopify_validated_step6.csv', index=False)
print(f"✅ Saved validated dataset to '../data/shopify_validated_step6.csv'")

# Generate detailed report
report_lines = []
report_lines.append("="*80)
report_lines.append("DATA VALIDATION REPORT - DAY 6")
report_lines.append("="*80)
report_lines.append(f"\nValidation Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
report_lines.append(f"Total Products: {len(df):,}")
report_lines.append(f"Overall Quality Score: {total_score:.1f}/100")
report_lines.append(f"Status: {status}")
report_lines.append(f"\n{'='*80}")
report_lines.append("\nVALIDATION RESULTS BY RULE:")
report_lines.append("="*80)

for result in validation_results:
    report_lines.append(f"\n[{result['severity']}] {result['rule']}")
    report_lines.append(f"   Violations: {result['violations']:,}")
    report_lines.append(f"   Passed: {result['passed']:,}")
    if result['violations'] > 0:
        report_lines.append(f"   Recommendation: {result['recommendation']}")
        if len(result['failed_products']) > 0:
            report_lines.append(f"   Sample failures:")
            for i, (idx, row) in enumerate(result['failed_products'].head(3).iterrows(), 1):
                report_lines.append(f"      {i}. {row.get('Title', 'N/A')} (Handle: {row.get('Handle', 'N/A')})")

report_lines.append(f"\n{'='*80}")
report_lines.append("\nRECOMMENDATION:")
report_lines.append(f"{recommendation}")
report_lines.append(f"\n{'='*80}")

report_text = "\n".join(report_lines)
with open('validation_report.txt', 'w', encoding='utf-8') as f:
    f.write(report_text)

print(f"✅ Saved detailed validation report to 'validation_report.txt'")

# Final recommendation
print("\n" + "="*80)
print("DAY 6 COMPLETE! 🎉")
print("="*80)

if total_score >= 90:
    print("\n✅ EXCELLENT! Your data is production-ready.")
    print("✅ Ready to proceed to Day 7: Master Pipeline")
elif total_score >= 70:
    print("\n⚠️  GOOD PROGRESS but review the violations in 'validation_failures.csv'")
    print("✅ You can proceed to Day 7 and iterate on improvements")
else:
    print("\n🔴 CRITICAL ISSUES FOUND - Review 'validation_failures.csv' carefully")
    print("💡 Consider fixing critical issues before Day 7")

print("\n📁 Files Generated:")
print("   ✓ validation_failures.csv - Products needing attention")
print("   ✓ validation_report.txt - Detailed validation report")
print("   ✓ shopify_validated_step6.csv - Dataset with validation status")

print("\n🚀 Next: Day 7 - Master Pipeline (combine all cleaning steps!)")
print("="*80)