"""
Data Quality Assessment Module
Contains all functions for assessing data quality using 6 dimensions
"""

import pandas as pd
import json
import re
from datetime import datetime


def load_data(file_path, filename, original_filename=None):
    """Load data from various file formats"""
    # Use original filename if provided, otherwise use filename
    check_filename = original_filename or filename
    
    # Safely get file extension
    ext = None
    if '.' in check_filename:
        ext = check_filename.rsplit('.', 1)[1].lower()
    elif '.' in filename:
        ext = filename.rsplit('.', 1)[1].lower()
    
    # If no extension found, try to detect from file content
    if not ext:
        # Try to detect file type by reading first few bytes
        try:
            with open(file_path, 'rb') as f:
                first_bytes = f.read(8)
                # Check for Excel file signature
                if first_bytes[:2] == b'PK':  # Excel files are ZIP archives
                    ext = 'xlsx'
                elif first_bytes[:4] == b'\xd0\xcf\x11\xe0':  # Old Excel format
                    ext = 'xls'
                elif first_bytes[0] == ord('{') or first_bytes[0] == ord('['):  # JSON
                    ext = 'json'
                else:
                    # Try CSV as default
                    ext = 'csv'
        except:
            raise ValueError("Cannot determine file type. Please ensure your file has an extension (.csv, .xlsx, .xls, or .json)")
    
    try:
        if ext == 'csv':
            df = pd.read_csv(file_path)
        elif ext in ['xlsx', 'xls']:
            df = pd.read_excel(file_path)
        elif ext == 'json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # Try to convert JSON to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # If it's a dict, try to find a list or convert directly
                df = pd.DataFrame([data])
            else:
                raise ValueError("Unsupported JSON structure")
        else:
            raise ValueError(f"Unsupported file type: {ext}. Please upload CSV, Excel, or JSON files.")
        
        return df
    except Exception as e:
        raise ValueError(f"Error loading file: {str(e)}")


def assess_data_quality(df):
    """Assess the quality of the data using 6 dimensions"""
    total_rows = len(df)
    total_columns = len(df.columns)
    total_cells = total_rows * total_columns if total_rows * total_columns > 0 else 1
    
    # ========== 1. Completeness ==========
    missing_values = df.isnull().sum().sum()
    missing_percentage = (missing_values / total_cells) * 100
    completeness_score = 100 - missing_percentage
    
    # ========== 2. Consistency ==========
    consistency_issues = 0
    type_inconsistencies = 0
    
    for col in df.columns:
        # Check for mixed data types in object columns
        if df[col].dtype == 'object':
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                # Check if column contains mixed types
                numeric_count = 0
                for val in non_null_values.head(100):  # Sample first 100
                    try:
                        float(str(val))
                        numeric_count += 1
                    except:
                        pass
                
                # If more than 20% are numeric but column is object, it's inconsistent
                if numeric_count > len(non_null_values.head(100)) * 0.2 and numeric_count < len(non_null_values.head(100)) * 0.8:
                    type_inconsistencies += 1
        
        # Check for inconsistent formats (e.g., dates, emails, etc.)
        non_null_values = df[col].dropna().astype(str)
        if len(non_null_values) > 0:
            # Check for email-like patterns
            if '@' in str(non_null_values.iloc[0]) if len(non_null_values) > 0 else False:
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                invalid_emails = sum(1 for v in non_null_values if not re.match(email_pattern, str(v)))
                if invalid_emails > 0:
                    consistency_issues += invalid_emails
    
    consistency_score = max(0, 100 - (consistency_issues / total_cells * 100) - (type_inconsistencies * 10))
    
    # ========== 3. Uniqueness ==========
    duplicate_rows = df.duplicated().sum()
    duplicate_percentage = (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0
    
    # Check for duplicate values in key columns (columns that should be unique)
    duplicate_values_count = 0
    for col in df.columns:
        if df[col].dtype == 'object':
            duplicates = df[col].duplicated().sum()
            duplicate_values_count += duplicates
    
    uniqueness_score = max(0, 100 - duplicate_percentage - (duplicate_values_count / total_cells * 50))
    
    # ========== 4. Validity ==========
    validity_issues = 0
    
    for col in df.columns:
        non_null_values = df[col].dropna()
        if len(non_null_values) == 0:
            continue
            
        # Check numeric columns for invalid values
        if df[col].dtype in ['int64', 'float64']:
            # Check for infinite values
            if hasattr(non_null_values, 'isin'):
                inf_count = non_null_values.isin([float('inf'), float('-inf')]).sum()
                validity_issues += inf_count
        
        # Check for negative values where they shouldn't exist (e.g., age, count)
        if df[col].dtype in ['int64', 'float64']:
            if 'age' in col.lower() or 'count' in col.lower() or 'quantity' in col.lower():
                negative_count = (non_null_values < 0).sum()
                validity_issues += negative_count
        
        # Check string columns for reasonable length
        if df[col].dtype == 'object':
            # Check for extremely long strings (potential errors)
            if len(non_null_values) > 0:
                max_length = non_null_values.astype(str).str.len().max()
                if max_length > 1000:  # Suspiciously long
                    validity_issues += (non_null_values.astype(str).str.len() > 1000).sum()
    
    validity_score = max(0, 100 - (validity_issues / total_cells * 100))
    
    # ========== 5. Accuracy ==========
    # Accuracy is hard to measure without reference data, so we check for outliers and anomalies
    accuracy_issues = 0
    
    for col in df.columns:
        if df[col].dtype in ['int64', 'float64']:
            non_null_values = df[col].dropna()
            if len(non_null_values) > 10:  # Need enough data for statistical analysis
                Q1 = non_null_values.quantile(0.25)
                Q3 = non_null_values.quantile(0.75)
                IQR = Q3 - Q1
                if IQR > 0:
                    lower_bound = Q1 - 3 * IQR
                    upper_bound = Q3 + 3 * IQR
                    outliers = ((non_null_values < lower_bound) | (non_null_values > upper_bound)).sum()
                    accuracy_issues += outliers
    
    accuracy_score = max(0, 100 - (accuracy_issues / total_cells * 50))
    
    # ========== 6. Timeliness ==========
    # Check for year columns (عام، سنة، year) and assess if they're recent
    timeliness_score = None  # None means not applicable
    year_columns_found = 0
    old_years_count = 0
    current_year = datetime.now().year
    
    for col in df.columns:
        col_lower = str(col).lower()
        
        # Check if column name contains year-related keywords
        is_year_column = any(keyword in col_lower for keyword in ['year', 'عام', 'سنة', 'سنه', 'تاريخ', 'date'])
        
        # Check if column contains year values (4-digit numbers between 1900-2100)
        year_values_found = False
        if df[col].dtype in ['int64', 'float64']:
            # Check if values look like years
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                # Check if most values are between 1900 and 2100 (likely years)
                year_like = ((non_null_values >= 1900) & (non_null_values <= 2100)).sum()
                if year_like > len(non_null_values) * 0.7:  # More than 70% look like years
                    year_values_found = True
                    year_columns_found += 1
                    # Check for old years (more than 10 years ago)
                    old_years = (non_null_values < (current_year - 10)).sum()
                    old_years_count += old_years
        elif df[col].dtype == 'object':
            # Try to extract years from string dates or check for year patterns
            non_null_values = df[col].dropna().astype(str)
            if len(non_null_values) > 0:
                # Try to parse as dates first
                try:
                    parsed_dates = pd.to_datetime(non_null_values, errors='coerce')
                    valid_dates = parsed_dates.notna()
                    if valid_dates.sum() > len(non_null_values) * 0.5:  # More than 50% are dates
                        year_columns_found += 1
                        years = parsed_dates[valid_dates].dt.year
                        old_years = (years < (current_year - 10)).sum()
                        old_years_count += old_years
                        year_values_found = True
                except:
                    pass
                
                # If not dates, try to extract 4-digit numbers that look like years
                if not year_values_found:
                    year_pattern = re.compile(r'\b(19|20)\d{2}\b')
                    year_matches = 0
                    extracted_years = []
                    for val in non_null_values.head(100):  # Sample first 100
                        matches = year_pattern.findall(str(val))
                        if matches:
                            # Try to extract full year
                            full_year_match = re.search(r'\b(19|20)\d{2}\b', str(val))
                            if full_year_match:
                                try:
                                    year_val = int(full_year_match.group())
                                    if 1900 <= year_val <= 2100:
                                        extracted_years.append(year_val)
                                        year_matches += 1
                                except:
                                    pass
                    
                    if year_matches > len(non_null_values.head(100)) * 0.5:  # More than 50% have years
                        year_columns_found += 1
                        if extracted_years:
                            old_years = sum(1 for y in extracted_years if y < (current_year - 10))
                            old_years_count += old_years
                        year_values_found = True
        
        # Also check by column name if it contains year keywords
        if is_year_column and not year_values_found:
            # Column name suggests it's a year column, try to extract years
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                if df[col].dtype in ['int64', 'float64']:
                    # Check if values are in reasonable year range
                    if ((non_null_values >= 1900) & (non_null_values <= 2100)).sum() > len(non_null_values) * 0.5:
                        year_columns_found += 1
                        old_years = (non_null_values < (current_year - 10)).sum()
                        old_years_count += old_years
                        year_values_found = True
    
    # Calculate timeliness score only if year columns were found
    if year_columns_found > 0 and total_rows > 0:
        # Calculate score based on how many old years there are
        old_years_percentage = (old_years_count / total_rows) * 100
        timeliness_score = max(0, 100 - (old_years_percentage * 1.5))  # Penalize old years
    else:
        # No year columns found, timeliness is not applicable
        timeliness_score = None  # Will be handled in frontend
    
    # Column details
    column_details = []
    for col in df.columns:
        col_missing = df[col].isnull().sum()
        col_type = str(df[col].dtype)
        column_details.append({
            'name': col,
            'type': col_type,
            'missing': int(col_missing)
        })
    
    # Overall quality score (average of applicable dimensions)
    # If timeliness is not applicable, calculate average of 5 dimensions
    applicable_scores = [
        completeness_score, 
        consistency_score, 
        uniqueness_score, 
        validity_score, 
        accuracy_score
    ]
    
    if timeliness_score is not None:
        applicable_scores.append(timeliness_score)
    
    overall_score = sum(applicable_scores) / len(applicable_scores) if applicable_scores else 0
    
    return {
        'total_rows': int(total_rows),
        'total_columns': int(total_columns),
        'overall_score': float(overall_score),
        # Completeness
        'completeness_score': float(completeness_score),
        'missing_values': int(missing_values),
        'missing_percentage': float(missing_percentage),
        # Consistency
        'consistency_score': float(consistency_score),
        'consistency_issues': int(consistency_issues),
        'type_inconsistencies': int(type_inconsistencies),
        # Uniqueness
        'uniqueness_score': float(uniqueness_score),
        'duplicate_rows': int(duplicate_rows),
        'duplicate_percentage': float(duplicate_percentage),
        # Validity
        'validity_score': float(validity_score),
        'validity_issues': int(validity_issues),
        # Accuracy
        'accuracy_score': float(accuracy_score),
        'accuracy_issues': int(accuracy_issues),
        # Timeliness
        'timeliness_score': float(timeliness_score) if timeliness_score is not None else None,
        'year_columns_found': int(year_columns_found),
        'old_years_count': int(old_years_count),
        'timeliness_applicable': timeliness_score is not None,
        'column_details': column_details
    }

