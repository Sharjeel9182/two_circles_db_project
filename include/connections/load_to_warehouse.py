def load_leads_to_warehouse(**context):
    """
    Loads the unified Leads data to the data warehouse using an upsert operation.
    
    This function:
    1. Takes the combined and deduplicated data from the upstream task
    2. Connects to the data warehouse
    3. Performs an upsert operation based on email address
    4. Returns statistics about the operation
    
    Parameters:
    ----------
    **context : dict
        Airflow context variables
        
    Returns:
    -------
    dict
        Statistics about the upsert operation
    """
    import mysql.connector
    import pandas as pd
    import logging
    from datetime import datetime
    
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.info("Starting data load to warehouse")
    
    try:
        # Get task instance from context
        ti = context['ti']
        
        # Retrieve combined data from upstream task
        records = ti.xcom_pull(task_ids='combine_data')
        
        if not records:
            logger.warning("No records to load to the warehouse")
            return {"status": "warning", "records_processed": 0, "message": "No records to process"}
        
        logger.info(f"Retrieved {len(records)} records for loading")
        
        # Connect to data warehouse
        logger.info("Connecting to data warehouse...")
        conn = mysql.connector.connect(
            host="kinterview-db.cluster-cnawrkmxrmmc.us-west-2.rds.amazonaws.com",
            port=3306,
            database="dw_sharjeel",
            user="dw_sharjeel",
            password="e51wsMz2FRKopC0Q"
        )
        logger.info("Connected to data warehouse")
        
        cursor = conn.cursor()
        
        # Create Leads table if it doesn't exist


        drop_table_if_exists ="""
        DROP TABLE IF EXISTS Leadtest;
        """
        cursor.execute(drop_table_if_exists)
        logger.info("Leadtest table dropped")

        
        create_table_sql = """
        CREATE TABLE Leadtest (
            LeadID INT AUTO_INCREMENT PRIMARY KEY,
            Email VARCHAR(255) NOT NULL,
            FirstName VARCHAR(100),
            LastName VARCHAR(100),
            FullName VARCHAR(255),
            Phone VARCHAR(50),
            Address VARCHAR(255),
            City VARCHAR(100),
            State VARCHAR(100),
            PostalCode VARCHAR(20),
            Country VARCHAR(100),
            isSalesforce BOOLEAN DEFAULT FALSE,
            isSalesLT BOOLEAN DEFAULT FALSE,
            DataSourcePriority VARCHAR(20),
            CreatedDate DATETIME,
            ModifiedDate DATETIME,
            UNIQUE KEY idx_email (Email)
        )
        """
        cursor.execute(create_table_sql)
        logger.info("Leadtest table created or verified")
        
        # Create temporary table for the staging operation
        drop_temp_table = "DROP TABLE IF EXISTS TempLeads"
        cursor.execute(drop_temp_table)
        
        create_temp_table = """
        CREATE TEMPORARY TABLE TempLeads (
            Email VARCHAR(255) NOT NULL,
            FirstName VARCHAR(100),
            LastName VARCHAR(100),
            FullName VARCHAR(255),
            Phone VARCHAR(50),
            Address VARCHAR(255),
            City VARCHAR(100),
            State VARCHAR(100),
            PostalCode VARCHAR(20),
            Country VARCHAR(100),
            isSalesforce BOOLEAN DEFAULT FALSE,
            isSalesLT BOOLEAN DEFAULT FALSE,
            DataSourcePriority VARCHAR(20),
            CreatedDate DATETIME,
            ModifiedDate DATETIME,
            PRIMARY KEY (Email)
        )
        """
        cursor.execute(create_temp_table)
        logger.info("Temporary table created")
        
        # Prepare the data for loading
        all_columns = set()
        for record in records:
            all_columns.update(record.keys())
        
        # Identify required columns for the database
        required_columns = [
            'Email', 'FirstName', 'LastName', 'FullName', 'Phone', 'JobTitle',
            'Address', 'City', 'State', 'PostalCode', 'Country',
            'isSalesforce', 'isSalesLT', 'DataSourcePriority',
            'CreatedDate', 'ModifiedDate'
        ]
        
        # Filter the columns to only those we need
        load_columns = [col for col in required_columns if col in all_columns]
        
        # Insert into temporary table
        # Build placeholders for SQL query
        placeholders = ', '.join(['%s'] * len(load_columns))
        insert_sql = f"INSERT INTO TempLeads ({', '.join(load_columns)}) VALUES ({placeholders})"
        
        # Prepare the data
        load_data = []
        for record in records:
            row = [record.get(col) for col in load_columns]
            load_data.append(row)
        
        # Execute the insert
        cursor.executemany(insert_sql, load_data)
        logger.info(f"Inserted {len(load_data)} records into temporary table")
        
        # Perform the upsert from temporary to target table
        upsert_sql = f"""
        INSERT INTO Leadtest ({', '.join(load_columns)})
        SELECT {', '.join(load_columns)}
        FROM TempLeads
        ON DUPLICATE KEY UPDATE
        """
        
        # Build the update part for each column except Email (which is the key)
        update_parts = []
        for col in load_columns:
            if col != 'Email':
                update_parts.append(f"{col} = VALUES({col})")
        
        upsert_sql += ', '.join(update_parts)
        
        # Execute the upsert
        cursor.execute(upsert_sql)
        
        # Get count of affected rows (2 = updated, 1 = inserted)
        affected_rows = cursor.rowcount
        
        # Count the records in leads table for reporting
        cursor.execute("SELECT COUNT(*) FROM Leadtest")
        total_leads = cursor.fetchone()[0]
        
        # Get counts by source
        source_counts = {}
        for priority in ['Both', 'Salesforce', 'ERP']:
            cursor.execute(f"SELECT COUNT(*) FROM Leadtest WHERE DataSourcePriority = '{priority}'")
            source_counts[priority] = cursor.fetchone()[0]
        
        # Commit the transaction
        conn.commit()
        logger.info("Transaction committed successfully")
        
        # Close the connection
        cursor.close()
        conn.close()
        logger.info("Database connection closed")
        
        # Return operation statistics
        return {
            "status": "success",
            "records_processed": len(records),
            "records_affected": affected_rows,
            "total_leads": total_leads,
            "source_counts": source_counts
        }
    
        
    except Exception as e:
        logger.error(f"Error loading data to warehouse: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Rollback if needed
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
            logger.info("Transaction rolled back")
            cursor.close()
            conn.close()
            logger.info("Database connection closed")
        
        raise