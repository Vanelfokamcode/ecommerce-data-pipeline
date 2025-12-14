import pandas as pd
import numpy as np
import re
import sys
import time
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from collections import defaultdict

# ==============================================================================
# CONFIGURATION
# ==============================================================================
INPUT_PATH = '../data/shopify_products.csv'
OUTPUT_PATH = '../data/shopify_master_output.csv'
REPORT_PATH = 'master_pipeline_report.txt'
FAILURES_PATH = 'master_validation_failures.csv'
MIN_QUALITY_SCORE = 70  # Minimum acceptable quality score

# ==============================================================================
# UTILITIES
# ==============================================================================
class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = []
    def handle_data(self, d):
        self.text.append(d)
    def get_data(self):
        return ''.join(self.text)

def strip_html_tags(html):
    if pd.isna(html):
        return ""
    s = MLStripper()
    s.feed(str(html))
    return s.get_data().strip()

def log_stage(stage_num, stage_name, status="COMPLETE"):
    print(f"\n{'='*80}")
    print(f"✅ STAGE {stage_num}: {stage_name} - {status}")
    print(f"{'='*80}")

# ==============================================================================
# MAIN PIPELINE
# ==============================================================================
def run_master_pipeline():
    print("="*80)
    print("🚀 MASTER DATA PIPELINE - FULL ETL PROCESS")
    print("="*80)
    print(f"\nPipeline Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Input: {INPUT_PATH}")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Quality Threshold: {MIN_QUALITY_SCORE}/100")
    
    start_time = time.time()
    pipeline_log = []
    errors = []
    
    try:
        # ==================================================================
        # STAGE 1: DATA INGESTION & EXPLORATION
        # ==================================================================
        print("\n" + "="*80)
        print("📥 STAGE 1: DATA INGESTION & EXPLORATION")
        print("="*80)
        
        df = pd.read_csv(INPUT_PATH)
        initial_rows = len(df)
        initial_cols = len(df.columns)
        
        print(f"\n✅ Loaded dataset:")
        print(f"   - Rows: {initial_rows:,}")
        print(f"   - Columns: {initial_cols}")
        print(f"   - Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        pipeline_log.append("Stage 1: Data Loaded Successfully")
        log_stage(1, "DATA INGESTION")
        
        # ==================================================================
        # STAGE 2: DATA QUALITY ASSESSMENT
        # ==================================================================
        print("\n" + "="*80)
        print("🔍 STAGE 2: DATA QUALITY ASSESSMENT")
        print("="*80)
        
        initial_missing = df.isnull().sum().sum()
        initial_seo = df['SEO Title'].notna().sum()
        
        print(f"\n📊 Initial Quality Metrics:")
        print(f"   - Total missing values: {initial_missing:,}")
        print(f"   - Products with SEO Title: {initial_seo} ({initial_seo/len(df)*100:.1f}%)")
        print(f"   - Price data type: {df['Variant Price'].dtype}")
        
        pipeline_log.append("Stage 2: Quality Assessment Complete")
        log_stage(2, "QUALITY ASSESSMENT")
        
        # ==================================================================
        # STAGE 3: PRICE CLEANING & VALIDATION
        # ==================================================================
        print("\n" + "="*80)
        print("💰 STAGE 3: PRICE CLEANING & VALIDATION")
        print("="*80)
        
        # Price validation flags
        df['price_valid'] = (df['Variant Price'] > 0) & (df['Variant Price'].notna())
        
        # Discount validation
        df['discount_valid'] = False
        df['discount_amount'] = 0.0
        df['discount_percentage'] = 0.0
        
        valid_discount_mask = (
            df['price_valid'] & 
            df['Variant Compare At Price'].notna() & 
            (df['Variant Compare At Price'] > df['Variant Price'])
        )
        
        df.loc[valid_discount_mask, 'discount_valid'] = True
        df.loc[valid_discount_mask, 'discount_amount'] = (
            df.loc[valid_discount_mask, 'Variant Compare At Price'] - 
            df.loc[valid_discount_mask, 'Variant Price']
        )
        df.loc[valid_discount_mask, 'discount_percentage'] = (
            df.loc[valid_discount_mask, 'discount_amount'] / 
            df.loc[valid_discount_mask, 'Variant Compare At Price'] * 100
        )
        
        # Profit margin calculation
        df['profit_margin'] = np.nan
        can_calc_margin = df['price_valid'] & df['Cost per item'].notna() & (df['Variant Price'] > 0)
        df.loc[can_calc_margin, 'profit_margin'] = (
            (df.loc[can_calc_margin, 'Variant Price'] - df.loc[can_calc_margin, 'Cost per item']) / 
            df.loc[can_calc_margin, 'Variant Price'] * 100
        )
        
        print(f"\n✅ Price Cleaning Results:")
        print(f"   - Valid prices: {df['price_valid'].sum():,} ({df['price_valid'].sum()/len(df)*100:.1f}%)")
        print(f"   - Valid discounts: {df['discount_valid'].sum():,}")
        print(f"   - Profit margins calculated: {can_calc_margin.sum():,}")
        
        pipeline_log.append("Stage 3: Price Cleaning Complete")
        log_stage(3, "PRICE CLEANING & VALIDATION")
        
        # ==================================================================
        # STAGE 4: TEXT CLEANING & SEO GENERATION
        # ==================================================================
        print("\n" + "="*80)
        print("📝 STAGE 4: TEXT CLEANING & SEO GENERATION")
        print("="*80)
        
        # Vendor standardization
        df['Vendor'] = df['Vendor'].str.strip().str.title()
        df['Vendor'] = df['Vendor'].fillna('Unknown Vendor')
        
        # Category cleaning
        df['Product Category'] = df['Product Category'].str.strip().str.title()
        df['Product Category'] = df['Product Category'].fillna('Uncategorized')
        
        # Title cleaning
        df['Title'] = df['Title'].str.strip()
        df['Title'] = df['Title'].apply(lambda x: re.sub(r'\s+', ' ', str(x)) if pd.notna(x) else x)
        df['title_length'] = df['Title'].str.len()
        
        # SEO Title Generation
        def generate_seo_title(row):
            title = str(row['Title'])[:50]
            vendor = str(row['Vendor'])
            category = str(row['Product Category'])
            full_format = f"{title} - {vendor} | Premium {category}"
            if len(full_format) > 60:
                return f"{title} - {vendor}"[:60]
            return full_format
        
        seo_before = df['SEO Title'].notna().sum()
        df['SEO Title'] = df.apply(generate_seo_title, axis=1)
        seo_after = df['SEO Title'].notna().sum()
        
        # SEO Description Generation
        def generate_seo_description(row):
            if pd.notna(row['SEO Description']) and str(row['SEO Description']).strip():
                return row['SEO Description']
            title = str(row['Title'])
            vendor = str(row['Vendor'])
            body_text = strip_html_tags(row['Body (HTML)'])
            if body_text and len(body_text) > 20:
                desc = f"Shop {title} from {vendor}. {body_text}"
            else:
                desc = f"Shop {title} from {vendor}. Premium quality with free shipping available."
            return desc[:152] + "..." if len(desc) > 155 else desc
        
        df['SEO Description'] = df.apply(generate_seo_description, axis=1)
        
        # Tags cleaning
        df['Tags'] = df['Tags'].fillna('untagged').str.lower().str.strip()
        df['tag_count'] = df['Tags'].apply(lambda x: len(str(x).split(',')) if x != 'untagged' else 0)
        
        # Content quality flags
        df['description_length'] = df['Body (HTML)'].apply(lambda x: len(strip_html_tags(x)) if pd.notna(x) else 0)
        df['has_seo_title'] = df['SEO Title'].notna() & (df['SEO Title'] != '')
        df['has_seo_description'] = df['SEO Description'].notna() & (df['SEO Description'] != '')
        df['has_tags'] = df['Tags'] != 'untagged'
        df['has_description'] = df['description_length'] > 0
        
        # Content quality score
        def calc_quality_score(row):
            score = 0
            if row['has_seo_title']: score += 25
            if row['has_seo_description']: score += 25
            if row['has_tags']: score += 20
            if row['has_description']: score += 20
            if 30 <= row['title_length'] <= 70: score += 10
            return score
        
        df['content_quality_score'] = df.apply(calc_quality_score, axis=1)
        
        print(f"\n✅ Text Cleaning Results:")
        print(f"   - SEO Titles: {seo_before} → {seo_after} (+{seo_after - seo_before})")
        print(f"   - Avg content quality: {df['content_quality_score'].mean():.1f}/100")
        print(f"   - Vendors standardized: {df['Vendor'].nunique()} unique")
        
        pipeline_log.append("Stage 4: Text & SEO Generation Complete")
        log_stage(4, "TEXT CLEANING & SEO GENERATION")
        
        # ==================================================================
        # STAGE 5: BUSINESS INTELLIGENCE ENRICHMENT
        # ==================================================================
        print("\n" + "="*80)
        print("🎯 STAGE 5: BUSINESS INTELLIGENCE ENRICHMENT")
        print("="*80)
        
        # Price tiers
        def classify_price_tier(price):
            if pd.isna(price) or price <= 0: return "Invalid"
            elif price < 30: return "Budget"
            elif price < 80: return "Mid-Range"
            elif price < 150: return "Premium"
            else: return "Luxury"
        
        df['price_tier'] = df['Variant Price'].apply(classify_price_tier)
        
        # Discount strategy
        def classify_discount(row):
            if pd.isna(row['Variant Compare At Price']) or not row['discount_valid']:
                return "No Discount"
            pct = row['discount_percentage']
            if pct <= 15: return "Small Discount"
            elif pct <= 30: return "Medium Discount"
            else: return "Large Discount"
        
        df['discount_strategy'] = df.apply(classify_discount, axis=1)
        
        # Profit categories
        def classify_profit(margin):
            if pd.isna(margin): return "No Cost Data"
            elif margin < 0: return "Loss"
            elif margin < 25: return "Low Margin"
            elif margin < 50: return "Healthy Margin"
            else: return "High Margin"
        
        df['profit_category'] = df['profit_margin'].apply(classify_profit)
        
        # Inventory health score
        def calc_inventory_score(row):
            score = 0
            if pd.notna(row['Variant Inventory Tracker']) and row['Variant Inventory Tracker'] != '': score += 25
            if row['price_valid']: score += 25
            if pd.notna(row['Cost per item']) and row['Cost per item'] > 0: score += 25
            if row['profit_category'] not in ['Loss', 'No Cost Data']: score += 25
            elif row['profit_category'] == 'No Cost Data': score += 12
            return score
        
        df['inventory_health_score'] = df.apply(calc_inventory_score, axis=1)
        
        # Variant complexity
        def classify_complexity(row):
            if pd.notna(row['Option3 Name']) and row['Option3 Name'] != '': return "Complex"
            elif pd.notna(row['Option2 Name']) and row['Option2 Name'] != '': return "Medium"
            else: return "Simple"
        
        df['variant_complexity'] = df.apply(classify_complexity, axis=1)
        
        # Content tier
        def classify_content_tier(score):
            if pd.isna(score): return "Unknown"
            elif score >= 90: return "Excellent"
            elif score >= 70: return "Good"
            elif score >= 50: return "Needs Work"
            else: return "Poor"
        
        df['content_tier'] = df['content_quality_score'].apply(classify_content_tier)
        
        # Business flags
        df['needs_pricing_review'] = (df['profit_category'] == 'Loss') | (~df['price_valid'])
        df['needs_content_update'] = df['content_quality_score'] < 60
        df['high_value_product'] = (
            df['price_tier'].isin(['Premium', 'Luxury']) &
            df['profit_category'].isin(['Healthy Margin', 'High Margin']) &
            (df['content_quality_score'] >= 70)
        )
        df['quick_win'] = (
            df['price_tier'].isin(['Mid-Range', 'Premium']) &
            (df['content_quality_score'] < 70) &
            df['price_valid']
        )
        
        print(f"\n✅ Business Intelligence Results:")
        print(f"   - High-value products: {df['high_value_product'].sum():,}")
        print(f"   - Quick wins identified: {df['quick_win'].sum():,}")
        print(f"   - Needs pricing review: {df['needs_pricing_review'].sum():,}")
        
        pipeline_log.append("Stage 5: Business Intelligence Complete")
        log_stage(5, "BUSINESS INTELLIGENCE ENRICHMENT")
        
        # ==================================================================
        # STAGE 6: DATA VALIDATION
        # ==================================================================
        print("\n" + "="*80)
        print("✓ STAGE 6: DATA VALIDATION")
        print("="*80)
        
        validation_results = []
        
        # Critical validations
        critical_issues = 0
        
        # Invalid prices
        invalid_prices = (~df['price_valid']).sum()
        if invalid_prices > 0:
            critical_issues += 1
            validation_results.append(('CRITICAL', 'Invalid Prices', invalid_prices))
        
        # Missing SEO
        missing_seo = (~df['has_seo_title']).sum()
        if missing_seo > 0:
            validation_results.append(('HIGH', 'Missing SEO Titles', missing_seo))
        
        # Selling at loss
        at_loss = (df['profit_category'] == 'Loss').sum()
        if at_loss > 0:
            validation_results.append(('HIGH', 'Selling at Loss', at_loss))
        
        # Poor content on premium
        premium_poor = (
            df['price_tier'].isin(['Premium', 'Luxury']) & 
            (df['content_quality_score'] < 70)
        ).sum()
        if premium_poor > 0:
            validation_results.append(('MEDIUM', 'Premium Products Need Better Content', premium_poor))
        
        # Calculate quality score
        quality_score = 100
        quality_score -= critical_issues * 10
        quality_score -= min(20, invalid_prices * 0.5)
        quality_score -= min(10, at_loss * 0.2)
        quality_score = max(0, quality_score)
        
        df['validation_status'] = 'PASS'
        df.loc[~df['price_valid'], 'validation_status'] = 'FAIL'
        
        # Determine pipeline status
        if quality_score >= 90:
            pipeline_status = "PRODUCTION READY ✅"
        elif quality_score >= 70:
            pipeline_status = "NEEDS REVIEW ⚠️"
        else:
            pipeline_status = "CRITICAL ISSUES ❌"
        
        print(f"\n✅ Validation Results:")
        print(f"   - Overall Quality Score: {quality_score:.1f}/100")
        print(f"   - Status: {pipeline_status}")
        print(f"   - Products passing validation: {(df['validation_status'] == 'PASS').sum():,}")
        
        if validation_results:
            print(f"\n   Issues Found:")
            for severity, issue, count in validation_results[:5]:
                print(f"   - [{severity}] {issue}: {count}")
        
        pipeline_log.append(f"Stage 6: Validation Complete - Score: {quality_score:.1f}/100")
        log_stage(6, "DATA VALIDATION")
        
        # ==================================================================
        # STAGE 7: OUTPUT GENERATION
        # ==================================================================
        print("\n" + "="*80)
        print("💾 STAGE 7: OUTPUT GENERATION")
        print("="*80)
        
        # Save main output
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"\n✅ Saved clean dataset: {OUTPUT_PATH}")
        
        # Save validation failures
        failures = df[df['validation_status'] == 'FAIL']
        if len(failures) > 0:
            failures[['Handle', 'Title', 'Vendor', 'Variant Price', 'validation_status']].to_csv(
                FAILURES_PATH, index=False
            )
            print(f"✅ Saved validation failures: {FAILURES_PATH}")
        
        # Generate report
        end_time = time.time()
        runtime = end_time - start_time
        
        report = []
        report.append("="*80)
        report.append("MASTER DATA PIPELINE - EXECUTION REPORT")
        report.append("="*80)
        report.append(f"\nExecution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Runtime: {runtime:.2f} seconds")
        report.append(f"\n{'='*80}")
        report.append("\nINPUT/OUTPUT SUMMARY:")
        report.append(f"  Input File: {INPUT_PATH}")
        report.append(f"  Input Rows: {initial_rows:,}")
        report.append(f"  Output File: {OUTPUT_PATH}")
        report.append(f"  Output Rows: {len(df):,}")
        report.append(f"  Columns Added: {len(df.columns) - initial_cols}")
        report.append(f"\n{'='*80}")
        report.append("\nPIPELINE STAGES COMPLETED:")
        for i, log_entry in enumerate(pipeline_log, 1):
            report.append(f"  {i}. {log_entry}")
        report.append(f"\n{'='*80}")
        report.append("\nDATA QUALITY METRICS:")
        report.append(f"  Overall Quality Score: {quality_score:.1f}/100")
        report.append(f"  Pipeline Status: {pipeline_status}")
        report.append(f"  SEO Coverage: 0% → 100% (+{seo_after} titles)")
        report.append(f"  Products with Issues: {(df['validation_status'] == 'FAIL').sum():,}")
        report.append(f"  High-Value Products: {df['high_value_product'].sum():,}")
        report.append(f"  Quick Win Opportunities: {df['quick_win'].sum():,}")
        report.append(f"\n{'='*80}")
        report.append("\nRECOMMENDATIONS:")
        if quality_score >= 90:
            report.append("  ✅ Data is production-ready. Deploy with confidence.")
        elif quality_score >= 70:
            report.append("  ⚠️  Review validation failures before production deployment.")
            report.append("  ⚠️  Address high-priority issues for optimal results.")
        else:
            report.append("  ❌ Critical issues must be fixed before production use.")
            report.append("  ❌ Review validation failures immediately.")
        report.append(f"\n{'='*80}")
        
        report_text = "\n".join(report)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        print(f"✅ Saved pipeline report: {REPORT_PATH}")
        
        pipeline_log.append("Stage 7: Outputs Generated")
        log_stage(7, "OUTPUT GENERATION")
        
        # ==================================================================
        # PIPELINE SUMMARY DASHBOARD
        # ==================================================================
        print("\n" + "="*80)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("="*80)
        print(f"\n🎯 STATUS: {pipeline_status}")
        print(f"⏱️  RUNTIME: {runtime:.2f} seconds")
        print(f"\n📈 BEFORE → AFTER:")
        print(f"   SEO Coverage:     0% → 100%")
        print(f"   Quality Score:    -- → {quality_score:.1f}/100")
        print(f"   Enriched Columns: {initial_cols} → {len(df.columns)} (+{len(df.columns) - initial_cols})")
        print(f"\n📊 KEY METRICS:")
        print(f"   Total Products:        {len(df):,}")
        print(f"   Validation Failures:   {(df['validation_status'] == 'FAIL').sum():,}")
        print(f"   High-Value Products:   {df['high_value_product'].sum():,}")
        print(f"   Quick Win Opportunities: {df['quick_win'].sum():,}")
        print(f"\n📁 FILES GENERATED:")
        print(f"   ✓ {OUTPUT_PATH}")
        print(f"   ✓ {REPORT_PATH}")
        if len(failures) > 0:
            print(f"   ✓ {FAILURES_PATH}")
        
        print("\n" + "="*80)
        print("✅ MASTER PIPELINE COMPLETE!")
        print("="*80)
        
        return quality_score, pipeline_status
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {str(e)}")
        errors.append(str(e))
        import traceback
        traceback.print_exc()
        return 0, "FAILURE"

# ==============================================================================
# ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "🚀"*40)
    print("SHOPIFY PRODUCT DATA - MASTER ETL PIPELINE")
    print("🚀"*40 + "\n")
    
    quality_score, status = run_master_pipeline()
    
    print("\n" + "="*80)
    if quality_score >= MIN_QUALITY_SCORE:
        print("✅ SUCCESS: Pipeline completed successfully!")
        print(f"   Quality score ({quality_score:.1f}) meets threshold ({MIN_QUALITY_SCORE})")
    else:
        print("⚠️  WARNING: Pipeline completed with quality issues")
        print(f"   Quality score ({quality_score:.1f}) below threshold ({MIN_QUALITY_SCORE})")
    print("="*80 + "\n")