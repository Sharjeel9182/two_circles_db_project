def inspect_leads_table(**context):
    """
    Inspects the Leads table in the data warehouse:
    1. Checks if the table exists
    2. Gets the table schema
    3. Retrieves a sample of data
    
    Returns:
    -------
    dict
        Information about the Leads table
    """
    import mysql.connector
    import logging
    import pandas as pd
    from tabulate import tabulate
    
    logger = logging.getLogger(__name__)
    logger.info("Inspecting Leads table in data warehouse")
    
    try:
        # Connect to data warehouse
        conn = mysql.connector.connect(
            host="kinterview-db.cluster-cnawrkmxrmmc.us-west-2.rds.amazonaws.com",
            port=3306,
            database="dw_sharjeel",
            user="dw_sharjeel",
            password="e51wsMz2FRKopC0Q"
        )
        logger.info("Connected to data warehouse")
        
        cursor = conn.cursor(dictionary=True)
        
        # Check if table exists
        cursor.execute("SHOW TABLES LIKE 'Leads'")
        table_exists = len(cursor.fetchall()) > 0
        
        if not table_exists:
            logger.info("Leads table does not exist in the data warehouse")
            cursor.close()
            conn.close()
            return {"exists": False, "message": "Leads table does not exist"}
        
        # Get table schema
        cursor.execute("DESCRIBE Leads")
        schema = cursor.fetchall()
        logger.info(f"Leads table schema: {schema}")
        
        # Get column names
        columns = [col['Field'] for col in schema]
        logger.info(f"Columns: {columns}")
        
        # Get table row count
        cursor.execute("SELECT COUNT(*) as count FROM Leads")
        row_count = cursor.fetchone()['count']
        logger.info(f"Leads table has {row_count} rows")
        
        # Get sample data (limit to 10 rows)
        sample_data = []
        if row_count > 0:
            cursor.execute("SELECT * FROM Leads LIMIT 10")
            sample_data = cursor.fetchall()
            
            # Format sample data for display
            df = pd.DataFrame(sample_data)
            logger.info(f"Sample data:\n{tabulate(df, headers='keys', tablefmt='psql')}")
            
            # Get source distribution
            cursor.execute("""
                SELECT 
                    DataSourcePriority, 
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Leads), 2) as percentage
                FROM Leads 
                GROUP BY DataSourcePriority
            """)
            source_distribution = cursor.fetchall()
            logger.info(f"Source distribution: {source_distribution}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return {
            "exists": True,
            "schema": schema,
            "row_count": row_count,
            "sample_data": sample_data,
            "source_distribution": source_distribution if row_count > 0 else []
        }
        
    except Exception as e:
        logger.error(f"Error inspecting Leads table: {str(e)}")
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
        return {"error": str(e)}