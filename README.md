### Workflow Explanation

1.Database Setup: 
   - Two tables are created in the SQLite database:
     - companies: This table contains the original company data, including `company_id`, `company_name`, and `company_linkedin_url`.
     - enriched_company_data: This table is designed to store the enriched data fetched from the LinkedIn API. It includes fields such as `company_id`, `company_linkedin_url`, `company_name`, `industry`, and `employee_count`.

2.Data Extraction:
   - The script starts by connecting to the SQL database and extracting data from the `companies` table. It fetches the `company_id` and `company_linkedin_url` for each company listed.

3.Data Enrichment Using LinkedIn API:
   - For each company, the LinkedIn Bulk Data Scraper API is called using the company’s LinkedIn URL. The API returns various data points related to the company.
   - The enrichment process involves filtering out unnecessary data fields. Specifically, any data points with keys containing the substrings `'affiliatedOrganizations'`, `'locations'`, or `'similarOrganizations'` are ignored.

4.Merging Data:
   - The original data from the `companies` table is merged with the enriched data obtained from the API. This combined data is then prepared for insertion into the database.

5.Data Insertion:
   - The final merged data is inserted into the `enriched_company_data` table.
   - Before insertion, the script checks if a record with the same `company_id` already exists in the database. If it exists, the existing record is updated; if not, a new record is inserted.
   - This upsert logic ensures that the database is updated with the most recent and enriched data without creating duplicate entries.

6.Final Output:
   - The enriched data is stored in the `enriched_company_data` table, which now contains more comprehensive information about each company, as retrieved from the LinkedIn API.

This workflow efficiently combines the existing company data with additional information from LinkedIn, providing a richer dataset that can be used for further analysis or reporting.