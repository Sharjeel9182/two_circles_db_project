def combine_data_for_leads_table(**context):
    """
    Combines data from Salesforce and ERP systems into a unified Leads table.
    
    This function:
    1. Retrieves DataFrames from upstream tasks (Salesforce and ERP extraction)
    2. Merges them based on email address
    3. Applies business rules for prioritization and deduplication
    4. Creates a unified DataFrame ready for loading to the destination table
    
    Parameters:
    ----------
    **context : dict
        Airflow context variables including task instance for XCOM data retrieval
        
    Returns:
    -------
    list
        List of dictionaries containing records from the unified Leads table
    """
    import pandas as pd
    import logging
    from datetime import datetime
    import json
    
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.info("Starting data combination process")
    
    try:
        # Get task instance from context
        ti = context['ti']
        
        # Retrieve dataframes from upstream tasks with detailed logging
        sf_df = ti.xcom_pull(task_ids='fetch_salesforce_data')
        erp_df = ti.xcom_pull(task_ids='extract_erp_data')
        
        logger.info(f"Salesforce data type: {type(sf_df)}")
        logger.info(f"ERP data type: {type(erp_df)}")
        
        # Convert lists back to DataFrames if needed
        if isinstance(sf_df, list):
            logger.info(f"Converting Salesforce list to DataFrame (length: {len(sf_df)})")
            sf_df = pd.DataFrame(sf_df)
        elif sf_df is None:
            logger.warning("No Salesforce data received!")
            sf_df = pd.DataFrame()
        
        if isinstance(erp_df, list):
            logger.info(f"Converting ERP list to DataFrame (length: {len(erp_df)})")
            erp_df = pd.DataFrame(erp_df)
        elif erp_df is None:
            logger.warning("No ERP data received!")
            erp_df = pd.DataFrame()
        
        # Log column information
        if not sf_df.empty:
            logger.info(f"Salesforce columns: {sf_df.columns.tolist()}")
            logger.info(f"Salesforce record count: {len(sf_df)}")
        
        if not erp_df.empty:
            logger.info(f"ERP columns: {erp_df.columns.tolist()}")
            logger.info(f"ERP record count: {len(erp_df)}")
        
        # Proceed only if at least one source has data
        if sf_df.empty and erp_df.empty:
            logger.warning("Both sources are empty, nothing to combine")
            return []
        
        # Ensure source flags are consistent
        if not sf_df.empty:
            sf_df['isSalesforce'] = True
            sf_df['isSalesLT'] = False
            sf_df['DataSourcePriority'] = 'Salesforce'
            
            # Ensure DoNotCall filter
            if 'DoNotCall' in sf_df.columns:
                sf_df = sf_df[sf_df['DoNotCall'] != True]
                logger.info(f"After DoNotCall filter: {len(sf_df)} Salesforce records")
        
        if not erp_df.empty:
            erp_df['isSalesforce'] = False
            erp_df['isSalesLT'] = True
            erp_df['DataSourcePriority'] = 'ERP'
        
        # Ensure we have proper email columns
        sf_email_col = next((col for col in sf_df.columns if col.lower() == 'email'), None) if not sf_df.empty else None
        erp_email_col = next((col for col in erp_df.columns if col.lower() == 'email' or col.lower() == 'emailaddress'), None) if not erp_df.empty else None
        
        if not sf_df.empty and sf_email_col is None:
            logger.error("No email column found in Salesforce data")
            if not erp_df.empty:
                return erp_df.to_dict('records')
            return []
            
        if not erp_df.empty and erp_email_col is None:
            logger.error("No email column found in ERP data")
            if not sf_df.empty:
                return sf_df.to_dict('records')
            return []
        
        # Standardize email columns
        if not sf_df.empty:
            sf_df = sf_df.rename(columns={sf_email_col: 'Email'})
            sf_df['Email'] = sf_df['Email'].str.lower().str.strip()
            
        if not erp_df.empty:
            erp_df = erp_df.rename(columns={erp_email_col: 'Email'})
        
        # Create a mapping between the two schemas
        field_mapping = {
            'FirstName': {
                'sf': 'FirstName',
                'erp': 'FirstName',
                'final': 'FirstName'
            },
            'LastName': {
                'sf': 'LastName',
                'erp': 'LastName',
                'final': 'LastName'
            },
            'FullName': {
                'sf': 'Name',
                'erp': 'FullName',
                'final': 'FullName'
            },
            'Phone': {
                'sf': 'Phone',
                'erp': 'PhoneNumber',
                'final': 'Phone'
            },
            'Address': {
                'sf': 'MailingStreet',
                'erp': 'AddressLine1',
                'final': 'Address'
            },
            'City': {
                'sf': 'MailingCity',
                'erp': 'City',
                'final': 'City'
            },
            'State': {
                'sf': 'MailingState',
                'erp': 'StateProvince',
                'final': 'State'
            },
            'PostalCode': {
                'sf': 'MailingPostalCode',
                'erp': 'PostalCode',
                'final': 'PostalCode'
            },
            'Country': {
                'sf': 'MailingCountry',
                'erp': 'Country',
                'final': 'Country'
            }
        }
        
        # Rename columns in each dataframe to standard names
        sf_renamed_columns = {}
        erp_renamed_columns = {}
        
        if not sf_df.empty:
            for field, mapping in field_mapping.items():
                if mapping['sf'] in sf_df.columns:
                    sf_renamed_columns[mapping['sf']] = mapping['final']
            sf_df = sf_df.rename(columns=sf_renamed_columns)
        
        if not erp_df.empty:
            for field, mapping in field_mapping.items():
                if mapping['erp'] in erp_df.columns:
                    erp_renamed_columns[mapping['erp']] = mapping['final']
            erp_df = erp_df.rename(columns=erp_renamed_columns)
        
        # Set Email as index for both dataframes
        if not sf_df.empty:
            sf_df = sf_df.set_index('Email')
        
        if not erp_df.empty:
            erp_df = erp_df.set_index('Email')
        
        # Find common and unique records
        if not sf_df.empty and not erp_df.empty:
            common_emails = set(sf_df.index).intersection(set(erp_df.index))
            only_sf_emails = set(sf_df.index).difference(set(erp_df.index))
            only_erp_emails = set(erp_df.index).difference(set(sf_df.index))
        elif not sf_df.empty:
            common_emails = set()
            only_sf_emails = set(sf_df.index)
            only_erp_emails = set()
        elif not erp_df.empty:
            common_emails = set()
            only_sf_emails = set()
            only_erp_emails = set(erp_df.index)
        else:
            common_emails = set()
            only_sf_emails = set()
            only_erp_emails = set()
        
        logger.info(f"Emails found in both systems: {len(common_emails)}")
        logger.info(f"Emails found only in Salesforce: {len(only_sf_emails)}")
        logger.info(f"Emails found only in ERP: {len(only_erp_emails)}")
        
        # Create the unified dataframe
        combined_records = []
        
        # Process common emails (in both systems)
        for email in common_emails:
            sf_record = sf_df.loc[email].to_dict() if isinstance(sf_df.loc[email], pd.Series) else sf_df.loc[email].iloc[0].to_dict()
            erp_record = erp_df.loc[email].to_dict() if isinstance(erp_df.loc[email], pd.Series) else erp_df.loc[email].iloc[0].to_dict()
            
            # Start with Salesforce record (higher priority)
            combined_record = sf_record.copy()
            
            # Set both flags to True
            combined_record['isSalesforce'] = True
            combined_record['isSalesLT'] = True
            combined_record['DataSourcePriority'] = 'Both'
            
            # For any missing fields in SF, supplement from ERP
            for field in erp_record:
                if field not in combined_record or pd.isna(combined_record[field]) or combined_record[field] == '':
                    combined_record[field] = erp_record[field]
            
            # Add email back as a field
            combined_record['Email'] = email
            combined_records.append(combined_record)
        
        # Process Salesforce-only emails
        for email in only_sf_emails:
            sf_record = sf_df.loc[email].to_dict() if isinstance(sf_df.loc[email], pd.Series) else sf_df.loc[email].iloc[0].to_dict()
            sf_record['isSalesforce'] = True
            sf_record['isSalesLT'] = False
            sf_record['DataSourcePriority'] = 'Salesforce'
            # Add email back as a field
            sf_record['Email'] = email
            combined_records.append(sf_record)
        
        # Process ERP-only emails
        for email in only_erp_emails:
            erp_record = erp_df.loc[email].to_dict() if isinstance(erp_df.loc[email], pd.Series) else erp_df.loc[email].iloc[0].to_dict()
            erp_record['isSalesforce'] = False
            erp_record['isSalesLT'] = True
            erp_record['DataSourcePriority'] = 'ERP'
            # Add email back as a field
            erp_record['Email'] = email
            combined_records.append(erp_record)
        
        # Create the final dataframe
        final_df = pd.DataFrame(combined_records)
        
        # Add creation timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_df['CreatedDate'] = timestamp
        final_df['ModifiedDate'] = timestamp
        
        # After creating the final dataframe, do a final deduplication check
        if not final_df.empty and 'Email' in final_df.columns:
            duplicate_check = final_df.duplicated(subset=['Email'], keep=False)
            if duplicate_check.any():
                logger.warning(f"Found {duplicate_check.sum()} duplicate emails in final result. Deduplicating...")
                # Get some examples of duplicates for debugging
                if duplicate_check.sum() > 0:
                    dup_emails = final_df[duplicate_check]['Email'].unique()
                    logger.info(f"Sample duplicate emails: {list(dup_emails)[:5]}")
                
                # Keep records with priority: Both > Salesforce > ERP
                def priority_order(row):
                    if row['DataSourcePriority'] == 'Both':
                        return 0
                    elif row['DataSourcePriority'] == 'Salesforce':
                        return 1
                    else:
                        return 2
                
                final_df['_priority'] = final_df.apply(priority_order, axis=1)
                final_df = final_df.sort_values('_priority').drop_duplicates(subset=['Email'], keep='first')
                final_df = final_df.drop('_priority', axis=1)
                
                logger.info(f"After final deduplication: {len(final_df)} records")
        
        logger.info(f"Successfully created unified Leads table with {len(final_df)} records")
        
        # Convert all datetime columns to strings to avoid serialization issues
        for column in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[column]):
                final_df[column] = final_df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert DataFrame to a list of dictionaries for JSON serialization
        # Handle non-serializable types carefully
        records = []
        for _, row in final_df.iterrows():
            record = {}
            for col, val in row.items():
                if isinstance(val, pd.Timestamp):
                    record[col] = val.strftime('%Y-%m-%d %H:%M:%S')
                elif pd.isna(val):
                    record[col] = None
                else:
                    record[col] = val
            records.append(record)
        
        return records
        
    except Exception as e:
        logger.error(f"Error combining data for Leads table: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise