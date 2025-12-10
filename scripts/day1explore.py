import pandas as pd

# Load the CSV file
print("Loading Shopify products data...")
df = pd.read_csv('../data/shopify_products.csv')

print("\n" + "="*80)
print("STEP 1: FIRST 5 ROWS")
print("="*80)
print(df.head())

print("\n" + "="*80)
print("STEP 2: BASIC INFO")
print("="*80)
print(f"Total Rows: {df.shape[0]:,}")
print(f"Total Columns: {df.shape[1]}")
print(f"\nDataset Size: {df.shape[0]:,} rows × {df.shape[1]} columns")

print("\n" + "="*80)
print("STEP 3: DATA TYPES")
print("="*80)
print(df.dtypes)

print("\n" + "="*80)
print("STEP 4: MISSING VALUES ANALYSIS")
print("="*80)

# Calculate missing values
missing_data = pd.DataFrame({
    'Column': df.columns,
    'Missing_Count': df.isnull().sum(),
    'Missing_Percentage': (df.isnull().sum() / len(df) * 100).round(2)
})

# Sort by missing count (highest first)
missing_data = missing_data.sort_values('Missing_Count', ascending=False)

# Only show columns with missing values
missing_data_filtered = missing_data[missing_data['Missing_Count'] > 0]

if len(missing_data_filtered) > 0:
    print(f"\nColumns with missing values ({len(missing_data_filtered)} out of {len(df.columns)}):\n")
    print(missing_data_filtered.to_string(index=False))
else:
    print("\n✅ Great! No missing values found in any column.")

# Summary statistics for key numeric columns
print("\n" + "="*80)
print("BONUS: KEY NUMERIC COLUMNS SUMMARY")
print("="*80)

numeric_columns = ['Variant Price', 'Variant Compare At Price', 'Cost per item', 'Variant Grams']
available_numeric = [col for col in numeric_columns if col in df.columns]

if available_numeric:
    print(df[available_numeric].describe())

print("\n" + "="*80)
print("EXPLORATION COMPLETE!")
print("="*80)
print(f"✅ Successfully loaded {df.shape[0]:,} products")
print(f"✅ Dataset contains {df.shape[1]} columns")
print(f"✅ Ready for cleaning and analysis!")