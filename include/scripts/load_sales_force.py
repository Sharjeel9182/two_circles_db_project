import requests
import urllib.parse

def extract_from_salesforce(token):
    """Extract contact data from Salesforce via REST API
    
    Args:
        token (str): Salesforce OAuth token
        
    Returns:
        list: List of contact dictionaries from Salesforce
    """
    # Define Salesforce API base URL (this would typically come from your config)
    print(f"The token is {token}")
    SF_QUERY_URL_BASE = "https://koresoftware-dev-ed.develop.my.salesforce.com"
    
    # Define fields to extract
    fields = [
        "Id", "FirstName", "LastName", "Email", "Phone", 
        "MailingStreet", "MailingCity", "MailingState", 
        "MailingPostalCode", "MailingCountry", "Title", 
        "LastModifiedDate", "AccountId"
    ]
    field_list = ",".join(fields)
    
    # Construct SOQL query - filter out DoNotCall contacts
    query = f"SELECT {field_list} FROM Contact WHERE DoNotCall != true"
    encoded_query = urllib.parse.quote(query)
    
    # Make API request
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(
        f"{SF_QUERY_URL_BASE}/query/?q={encoded_query}",
        headers=headers
    )
    
    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"Salesforce API request failed: {response.status_code} - {response.text}")
    
    # Process records
    data = response.json()
    contacts = data.get("records", [])
    
    # Add source flags
    for contact in contacts:
        contact["isSalesforce"] = True
    
    return contacts