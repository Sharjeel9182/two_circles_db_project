def validate_leads_data(**context):
    """
    Validates the combined leads data for quality issues.
    Fails the task if critical quality issues are found.
    """
    import pandas as pd
    import logging
    
    ti = context['ti']
    records = ti.xcom_pull(task_ids='combine_sources_data')
    
    logger = logging.getLogger(__name__)
    logger.info(f"Validating {len(records)} lead records")
    
    df = pd.DataFrame(records)
    
    # Log columns to debug
    logger.info(f"Columns in the dataframe: {df.columns.tolist()}")

    # Count records by Source
    if 'Source' in df.columns:
        priority_counts = df['Source'].value_counts().to_dict()
        logger.info("Records by Source:")
        for priority, count in priority_counts.items():
            logger.info(f"  - {priority}: {count} records")
        
        # Get percentages for better understanding
        total_records = len(df)
        logger.info("Distribution by Source:")
        for priority, count in priority_counts.items():
            percentage = (count / total_records) * 100
            logger.info(f"  - {priority}: {percentage:.2f}%")
    else:
        logger.warning("Source column not found in the data")
    
    # Check for email column with different case
    email_columns = [col for col in df.columns if col.lower() == 'email']
    if not email_columns:
        logger.warning("No email column found. Available columns: " + ", ".join(df.columns.tolist()))
        # Try to identify a likely email column
        potential_email_cols = [col for col in df.columns if 'mail' in col.lower()]
        if potential_email_cols:
            logger.info(f"Potential email columns found: {potential_email_cols}")
            email_column = potential_email_cols[0]
        else:
            logger.error("No email column found and couldn't identify a potential email column")
            return False
    else:
        email_column = email_columns[0]
    
    # Check for duplicate emails
    duplicated_emails = df[email_column].duplicated()
    duplicate_count = duplicated_emails.sum()
    
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate email addresses")
        
        # Identify the duplicate values
        duplicate_values = df[df[email_column].duplicated(keep=False)][email_column].sort_values().tolist()
        
        # Group duplicates to make the log more readable
        from collections import Counter
        duplicate_counts = Counter(duplicate_values)
        
        # Log the duplicate emails with their occurrence count
        logger.warning("Duplicate emails found:")
        for email, count in duplicate_counts.items():
            logger.warning(f"  - {email}: appears {count} times")
        
        # Optionally log some sample duplicate records with more details
        logger.warning("Sample duplicate records:")
        for email in list(duplicate_counts.keys())[:5]:  # Show first 5 duplicates
            sample_records = df[df[email_column] == email].head(2).to_dict('records')
            for i, record in enumerate(sample_records):
                logger.warning(f"  Record {i+1} for {email}:")
                for key, value in record.items():
                    if key in ['CustomerID', 'FirstName', 'LastName', email_column, 'isSalesLT', 'Source']:
                        logger.warning(f"    {key}: {value}")
    
    # Check for missing values in critical fields
    # Use dynamic field mapping for more resilience
    critical_fields_mapping = {
        'email': email_column,
        'salesforce_flag': next((col for col in df.columns if 'salesforce' in col.lower()), None),
        'erp_flag': next((col for col in df.columns if 'saleslt' in col.lower() or 'erp' in col.lower()), None)
    }
    
    logger.info(f"Critical field mapping: {critical_fields_mapping}")
    
    for field_purpose, field_name in critical_fields_mapping.items():
        if field_name and field_name in df.columns:
            missing_count = df[field_name].isna().sum()
            if missing_count > 0:
                logger.warning(f"Found {missing_count} records with missing {field_name}")
        else:
            if field_purpose == 'email':
                logger.error(f"Critical field for {field_purpose} not found in data")
                return False
            else:
                logger.warning(f"Field for {field_purpose} not found in data")
    
    # Verify DoNotCall exclusion if present
    donotcall_columns = [col for col in df.columns if 'donotcall' in col.lower()]
    if donotcall_columns:
        donotcall_col = donotcall_columns[0]
        do_not_call_count = df[df[donotcall_col] == True].shape[0]
        if do_not_call_count > 0:
            logger.warning(f"Found {do_not_call_count} records with DoNotCall=True that should have been excluded")
    
    # Verify source system flags if present
    sf_flag = critical_fields_mapping.get('salesforce_flag')
    erp_flag = critical_fields_mapping.get('erp_flag')
    priority_columns = [col for col in df.columns if 'priority' in col.lower() or 'source' in col.lower()]
    priority_col = priority_columns[0] if priority_columns else None
    
    if sf_flag and erp_flag and priority_col:
        # Convert to proper boolean if needed
        if df[sf_flag].dtype != 'bool':
            df[sf_flag] = df[sf_flag].astype(str).str.lower() == 'true'
        if df[erp_flag].dtype != 'bool':
            df[erp_flag] = df[erp_flag].astype(str).str.lower() == 'true'
            
        # Count source combinations
        both_count = df[(df[sf_flag] == True) & (df[erp_flag] == True)].shape[0]
        sf_only_count = df[(df[sf_flag] == True) & (df[erp_flag] == False)].shape[0]
        erp_only_count = df[(df[sf_flag] == False) & (df[erp_flag] == True)].shape[0]
        
        logger.info(f"Source breakdown - Salesforce only: {sf_only_count}, ERP only: {erp_only_count}, Both: {both_count}")
        
        # Check for inconsistent source flags if priority column exists
        if priority_col:
            inconsistent_flags = 0
            for _, row in df.iterrows():
                sf = row[sf_flag]
                erp = row[erp_flag]
                priority = str(row[priority_col]).lower() if not pd.isna(row[priority_col]) else ''
                
                if sf and erp and 'both' not in priority:
                    inconsistent_flags += 1
                elif sf and not erp and 'salesforce' not in priority:
                    inconsistent_flags += 1
                elif not sf and erp and 'erp' not in priority:
                    inconsistent_flags += 1
            
            if inconsistent_flags > 0:
                logger.warning(f"Found {inconsistent_flags} records with inconsistent source flags and priority")
    
    logger.info("Data validation completed successfully")
    return True