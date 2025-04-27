def check_warehouse_permissions(**context):
    """
    Checks the permissions of the user in the data warehouse
    to determine if they can delete tables.
    
    Returns:
    -------
    dict
        Information about the user's permissions
    """
    import mysql.connector
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Checking data warehouse permissions")
    
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
        
        # Check what tables we can see
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        logger.info(f"Visible tables: {[list(table.values())[0] for table in tables]}")
        
        # Check user privileges
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        grants = cursor.fetchall()
        grant_info = [list(grant.values())[0] for grant in grants]
        logger.info(f"User grants: {grant_info}")
        
        # Try to create a test table
        test_table_name = "permission_test_table"
        try:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {test_table_name} (id INT)")
            logger.info(f"Successfully created test table {test_table_name}")
            
            # Try to drop the test table
            cursor.execute(f"DROP TABLE {test_table_name}")
            logger.info(f"Successfully dropped test table {test_table_name}")
            can_delete = True
        except Exception as e:
            logger.error(f"Error testing table operations: {str(e)}")
            can_delete = False
        
        # Close connection
        cursor.close()
        conn.close()
        
        return {
            "can_see_tables": len(tables) > 0,
            "visible_tables": [list(table.values())[0] for table in tables],
            "grants": grant_info,
            "can_delete_tables": can_delete
        }
        
    except Exception as e:
        logger.error(f"Error checking permissions: {str(e)}")
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
        return {
            "error": str(e),
            "can_delete_tables": False
        }