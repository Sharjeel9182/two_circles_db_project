import requests
import urllib.parse
import pandas as pd

def fetch_salesforce_data(client_id, client_secret, username, password, selected_columns):
    """Fetch Salesforce contact data with selected columns and filter DoNotCall = False."""
    
    # 1. Get OAuth Token
    auth_url = "https://login.salesforce.com/services/oauth2/token"
    auth_data = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    # Make authentication request
    auth_response = requests.post(auth_url, data=auth_data, headers=headers)
    auth_result = auth_response.json()
    access_token = auth_result.get("access_token")
    instance_url = auth_result.get("instance_url")

    if not access_token or not instance_url:
        raise Exception("Failed to authenticate with Salesforce API.")

    # 2. Describe Contact Object (Optional: for schema inspection)
    describe_url = f"{instance_url}/services/data/v51.0/sobjects/Contact/describe"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    describe_response = requests.get(describe_url, headers=headers)
    schema = describe_response.json()

    # 3. Build the SOQL query
    field_list = ",".join(selected_columns)  # Convert list of columns into a comma-separated string
    query = f"SELECT {field_list} FROM Contact WHERE DoNotCall != true"  # Ignore records where DoNotCall = true
    encoded_query = urllib.parse.quote(query)  # URL encode the query

    # Query URL
    query_url = f"{instance_url}/services/data/v51.0/query/?q={encoded_query}"

    # Make API request to Salesforce
    query_response = requests.get(query_url, headers=headers)

    if query_response.status_code != 200:
        raise Exception(f"Salesforce API request failed: {query_response.status_code} - {query_response.text}")

    # Process the query response
    data = query_response.json()
    contacts = data.get("records", [])

    # Clean the records by removing the 'attributes' metadata
    cleaned_records = []
    for contact in contacts:
        contact_copy = contact.copy()
        if "attributes" in contact_copy:
            del contact_copy["attributes"]
        # Add IsSalesforce flag to each record
        contact_copy["IsSalesforce"] = True
        cleaned_records.append(contact_copy)

    # Convert cleaned records into a DataFrame
    df_contacts = pd.DataFrame(cleaned_records)

    # Filter the DataFrame to ignore contacts where DoNotCall = True
    df_contacts = df_contacts[df_contacts['DoNotCall'] != True]

        # Before returning, rename the flag to be consistent
    if 'IsSalesforce' in df_contacts.columns and 'isSalesforce' not in df_contacts.columns:
        df_contacts['isSalesforce'] = df_contacts['IsSalesforce']
    
    # Make sure 'Email' column exists (not EmailAddress)
    if 'Email' not in df_contacts.columns and 'EmailAddress' in df_contacts.columns:
        df_contacts = df_contacts.rename(columns={'EmailAddress': 'Email'})

    # Return the DataFrame
    return df_contacts
