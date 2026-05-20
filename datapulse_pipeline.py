"""
DataPulse Pipeline - Automated Data Processing Pipeline
Applies the following transformations in execution priority order:
1. drop_duplicates
2. dropna estructural  
3. Limpieza de strings
4. Conversión de tipos
5. Imputaciones (fillna_mean for TV, Radio, Social Media, Sales)
6. Outliers handling
7. Encoding (onehot for Influencer)
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path


def load_data(filepath: str) -> pd.DataFrame:
    """Load data from CSV file."""
    return pd.read_csv(filepath)


def generate_quality_report(df: pd.DataFrame, filepath: str) -> dict:
    """Generate quality report before processing."""
    report = {
        "file": filepath,
        "shape": {"rows": int(df.shape[0]), "columns": int(df.shape[1])},
        "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "null_counts": {col: int(count) for col, count in df.isnull().sum().items()},
        "duplicates": int(df.duplicated().sum()),
        "numeric_summary": {}
    }
    
    # Numeric columns summary
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        report["numeric_summary"][col] = {
            "mean": float(df[col].mean()) if df[col].notna().any() else None,
            "std": float(df[col].std()) if df[col].notna().any() else None,
            "min": float(df[col].min()) if df[col].notna().any() else None,
            "max": float(df[col].max()) if df[col].notna().any() else None,
            "skew": float(df[col].skew()) if df[col].notna().any() else None
        }
    
    # Categorical columns summary
    cat_cols = df.select_dtypes(include=['object']).columns
    for col in cat_cols:
        report[f"{col}_unique"] = df[col].unique().tolist() if df[col].notna().any() else []
        report[f"{col}_value_counts"] = df[col].value_counts().to_dict()
    
    return report


def run_pipeline(input_file: str, output_file: str = None, generate_report: bool = True) -> pd.DataFrame:
    """
    Run the complete DataPulse pipeline.
    
    Priority execution order:
    1. drop_duplicates
    2. dropna structural
    3. String cleaning
    4. Type conversion
    5. Imputations (fillna_mean)
    6. Outliers handling
    7. Encoding (onehot)
    """
    print(f"Loading data from {input_file}...")
    df = load_data(input_file)
    initial_shape = df.shape
    
    print(f"Initial shape: {initial_shape}")
    
    # Generate initial quality report if requested
    if generate_report:
        initial_report = generate_quality_report(df, input_file)
        with open("quality_report.json", "w") as f:
            json.dump(initial_report, f, indent=2)
        print("Generated quality_report.json")
    
    # ============================================
    # STEP 1: DROP DUPLICATES
    # ============================================
    print("\n[1/7] Removing duplicates...")
    before_count = len(df)
    df = df.drop_duplicates()
    after_count = len(df)
    removed = before_count - after_count
    print(f"  Removed {removed} duplicate rows")
    
    # ============================================
    # STEP 2: STRUCTURAL DROPNA
    # ============================================
    print("\n[2/7] Structural dropna...")
    # For this dataset, we'll track but not aggressive dropna since user wants imputation
    # We'll only drop rows where ALL numeric fields are null
    numeric_cols = ['TV', 'Radio', 'Social Media', 'Sales']
    before_dropna = len(df)
    df = df.dropna(subset=numeric_cols, how='all')
    after_dropna = len(df)
    print(f"  Removed {before_dropna - after_dropna} rows with all NaN values")
    
    # ============================================
    # STEP 3: STRING CLEANING
    # ============================================
    print("\n[3/7] String cleaning...")
    # Clean leading/trailing whitespace from string columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    print(f"  Cleaned {len(string_cols)} string column(s)")
    
    # ============================================
    # STEP 4: TYPE CONVERSION
    # ============================================
    print("\n[4/7] Type conversion...")
    # All columns are already numeric except Influencer
    # TV should be integer
    df['TV'] = pd.to_numeric(df['TV'], errors='coerce')
    print("  Ensured numeric types for numeric columns")
    
    # ============================================
    # STEP 5: IMPUTATIONS (fillna_mean)
    # ============================================
    print("\n[5/7] Applying fillna_mean imputations...")
    # Fill missing values with mean for symmetric distributions
    fillna_mean_cols = ['TV', 'Radio', 'Social Media', 'Sales']
    
    for col in fillna_mean_cols:
        if col in df.columns and df[col].isnull().any():
            null_count = df[col].isnull().sum()
            mean_value = df[col].mean()
            df[col] = df[col].fillna(mean_value)
            print(f"  {col}: filled {null_count} nulls with mean ({mean_value:.4f})")
    
    # ============================================
    # STEP 6: OUTLIERS HANDLING
    # ============================================
    print("\n[6/7] Outliers handling...")
    # No explicit outliers handling requested, but we can check
    # Using IQR method for reference (not removing, just logging)
    outlier_summary = {}
    for col in fillna_mean_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
        outlier_summary[col] = {"count": int(outliers), "bounds": [float(lower_bound), float(upper_bound)]}
    print(f"  Detected outliers (IQR method): {outlier_summary}")
    # Note: Not removing outliers as per user instructions
    
    # ============================================
    # STEP 7: ENCODING (onehot for Influencer)
    # ============================================
    print("\n[7/7] Applying onehot encoding for Influencer...")
    influencer_dummies = pd.get_dummies(df['Influencer'], prefix='Influencer')
    df = pd.concat([df, influencer_dummies], axis=1)
    print(f"  Created {len(influencer_dummies.columns)} onehot columns: {list(influencer_dummies.columns)}")
    
    # ============================================
    # SAVE OUTPUT
    # ============================================
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"\nSaved processed data to {output_file}")
    
    final_shape = df.shape
    print(f"\n{'='*50}")
    print(f"Pipeline completed!")
    print(f"  Initial shape: {initial_shape}")
    print(f"  Final shape: {final_shape}")
    print(f"  Total rows removed: {initial_shape[0] - final_shape[0]}")
    print(f"  New columns added: {final_shape[1] - initial_shape[1]}")
    
    return df


if __name__ == "__main__":
    INPUT_FILE = "Dummy Data HSS.csv"
    OUTPUT_FILE = "processed_data.csv"
    
    # Run the pipeline
    result_df = run_pipeline(INPUT_FILE, OUTPUT_FILE, generate_report=True)