def extract_erp_customer_data(date,host,port,database,user,password):
    """
    Extracts comprehensive customer data from the ERP system.
    
    This function connects to the AdventureWorks database, extracts customer 
    information by joining various tables, and returns a DataFrame with the results.
    
    Parameters:
    ----------
    **context : dict
        Airflow context variables
        
    Returns:
    -------
    pandas.DataFrame
        DataFrame containing comprehensive customer data
    """
    import mysql.connector
    import pandas as pd
    import logging
    
    # Set up logging
    logger = logging.getLogger(__name__)
    
    # Get execution date from Airflow context, or use current date if not available
    execution_date = date
    logger.info(f"Running ERP data extraction for {execution_date}")
    
    try:
        # Connect to MySQL ERP database
        logger.info("Connecting to MySQL ERP database...")
        conn = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logger.info("Connected successfully to ERP database")
        
        # Create a cursor
        cursor = conn.cursor(dictionary=True)
        
        # Comprehensive query joining all relevant tables for complete contact information
        comprehensive_query = """
        SELECT 
            c.CustomerID,
            c.AccountNumber,
            c.StoreID,
            p.BusinessEntityID,
            p.PersonType,
            p.Title,
            p.FirstName,
            p.MiddleName, 
            p.LastName,
            CONCAT(IFNULL(p.FirstName, ''), ' ', IFNULL(p.MiddleName, ''), ' ', IFNULL(p.LastName, '')) AS FullName,
            e.EmailAddress,
            pp.PhoneNumber,
            pnt.Name AS PhoneType,
            a.AddressLine1,
            a.AddressLine2,
            a.City,
            a.PostalCode,
            sp.Name AS StateProvince,
            cr.Name AS Country,
            c.TerritoryID,
            st.Name AS Territory,
            st.CountryRegionCode,
            MAX(soh.OrderDate) AS LastOrderDate,
            MIN(soh.OrderDate) AS FirstOrderDate,
            COUNT(soh.SalesOrderID) AS TotalOrders,
            p.ModifiedDate,
            c.ModifiedDate AS CustomerModifiedDate
        FROM 
            Sales_Customer c
        JOIN 
            Person_Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN
            Person_EmailAddress e ON p.BusinessEntityID = e.BusinessEntityID
        LEFT JOIN
            Person_PersonPhone pp ON p.BusinessEntityID = pp.BusinessEntityID
        LEFT JOIN
            Person_PhoneNumberType pnt ON pp.PhoneNumberTypeID = pnt.PhoneNumberTypeID
        LEFT JOIN
            Person_BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
        LEFT JOIN
            Person_Address a ON bea.AddressID = a.AddressID
        LEFT JOIN
            Person_StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        LEFT JOIN
            Person_CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
        LEFT JOIN
            Sales_SalesTerritory st ON c.TerritoryID = st.TerritoryID
        LEFT JOIN
            Sales_SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
        GROUP BY
            c.CustomerID, p.BusinessEntityID, e.EmailAddress, pp.PhoneNumber, pnt.Name,
            a.AddressLine1, a.City, sp.Name, cr.Name, st.Name
        """
        
        logger.info("Executing comprehensive query...")
        cursor.execute(comprehensive_query)
        results = cursor.fetchall()
        logger.info(f"Retrieved {len(results)} customer records from ERP")
        
        # Convert to DataFrame
        df_comprehensive = pd.DataFrame(results)
        
        # Add source flag
        df_comprehensive['isSalesLT'] = True
        df_comprehensive['Source'] = 'ERP'
        
        # FIX: Add extraction date to all rows - convert execution_date to string first
        extracted_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        df_comprehensive['ExtractedDate'] = extracted_date_str
        
        # Log data quality metrics
        logger.info(f"Data quality metrics:")
        logger.info(f"- Total records: {len(df_comprehensive)}")
        
        # Check if certain columns exist before counting
        if 'EmailAddress' in df_comprehensive.columns:
            logger.info(f"- Records with email: {df_comprehensive['EmailAddress'].notnull().sum()}")
        if 'PhoneNumber' in df_comprehensive.columns:
            logger.info(f"- Records with phone: {df_comprehensive['PhoneNumber'].notnull().sum()}")
        if 'AddressLine1' in df_comprehensive.columns:
            logger.info(f"- Records with address: {df_comprehensive['AddressLine1'].notnull().sum()}")
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        logger.info("Database connection closed")
        
        return df_comprehensive
        
    except Exception as e:
        logger.error(f"Error extracting ERP customer data: {str(e)}")
        # If there's an open connection, close it
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("Database connection closed after error")
        
        # Re-raise the exception to let Airflow handle it
        raise