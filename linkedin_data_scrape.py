from sqlalchemy import create_engine, exc, text
import pandas as pd
import requests

# Step 1: Set Up Database Connection
def connect_to_db():
    connection_string = "sqlite:///linkedindata.db"  # Adjust connection string as needed
    engine = create_engine(connection_string)
    return engine

# Step 2: Extract Data from SQL Database
def extract_data():
    engine = connect_to_db()
    query = "SELECT * FROM companies"
    df = pd.read_sql(query, engine)
    print("Original Data:")
    print(df.head())
    return df

# Step 3: Enrich Data Using LinkedIn Bulk Data Scraper API
def fetch_company_data(linkedin_url, api_key, api_url):
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "linkedin-bulk-data-scraper.p.rapidapi.com"  # Replace with the actual host
    }
    params = {
        "company_url": "https://www.linkedin.com/company/google",
        "page": 1 
        }
    response = requests.get(api_url, headers=headers, params=params)
    print( "API response : ", response.json())
    return response.json()

def linkedindata(df, api_key, api_url):
    linkedindata = []
    for url in df['company_linkedin_url']:
        data = fetch_company_data(url, api_key, api_url)
        filtered_data = {k: v for k, v in data.items() if 'affiliatedOrganizations' not in k and 'locations' not in k and 'similarOrganizations' not in k}
        linkedindata.append(filtered_data)
    
    enriched_df = pd.DataFrame(linkedindata)
    print("Enriched Data:")
    print(enriched_df.head())
    return enriched_df

# Step 4: Merge DataFrames
def merge_data(df, enriched_df):
    final_df = pd.concat([df, enriched_df], axis=1)
    print("Final Merged Data:")
    print(final_df.head())
    return final_df

# Step 5: Insert Data with Upsert Logic
def insert_data(final_df):
    engine = connect_to_db()

    with engine.connect() as connection:  # Open a connection using the engine
        for index, row in final_df.iterrows():  # Iterate over each row in the final DataFrame
            data = row.to_dict()  # Convert the row into a dictionary
            company_id = data.get('company_id')  # Get the company ID from the dictionary

            if company_id:  # Check if company_id is not None
                # Check if a record with the same company_id already exists in the 'companies' table
                query = text(f"SELECT * FROM companies WHERE company_id = :company_id")
                existing_record = connection.execute(query, {"company_id": company_id}).fetchone()
                
                if existing_record:  # If the record exists
                    # Prepare an UPDATE query to modify the existing record
                    update_query = text("""
                    UPDATE enriched_company_data
                    SET 
                        company_linkedin_url = :company_linkedin_url,
                        company_name = :company_name,
                        industry = :industry,
                        employee_count = :employee_count
                    WHERE company_id = :company_id
                    """)
                    connection.execute(update_query, data)
                    print(f"Updated record with company_id: {company_id}")
                else:  # If the record does not exist
                    try:
                        # Insert the new record into the 'linkedindata' table
                        final_df.loc[[index]].to_sql('linkedindata', con=engine, if_exists='append', index=False)
                        print(f"Inserted new record with company_id: {company_id}")
                    except exc.IntegrityError:  # Handle potential integrity errors (e.g., duplicate keys)
                        print(f"IntegrityError: Record with company_id {company_id} could not be inserted.")
            else:
                print(f"Skipping record at index {index} due to missing company_id.")

# Main execution
if __name__ == "__main__":
    # Extract original data from SQL database
    df = extract_data()

    # Define API details
    api_key = "6e8da72d10msh74634d4d267fe9cp12faeajsn91749d2b6fe8"
    api_url = "https://linkedin-bulk-data-scraper.p.rapidapi.com/company_updates"

    # Enrich the data using the LinkedIn Bulk Data Scraper API
    enriched_df = linkedindata(df, api_key, api_url)

    # Merge original and enriched data
    final_df = merge_data(df, enriched_df)

    # Insert the final merged data into the SQL database with duplicate handling
    insert_data(final_df)