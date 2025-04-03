import requests
import pandas as pd
import json
import time

class ADSQuery:
    def __init__(self, api_token):
        self.api_token = api_token
        self.base_url = "https://api.adsabs.harvard.edu/v1/search/query"
        self.headers = {"Authorization": f"Bearer {self.api_token}"}
        self.requests_made = 0
        self.remaining_requests = None
        self.limit_requests = None
        self.reset_time = None

    def _update_rate_limit_info(self, response):
        self.remaining_requests = response.headers.get("X-RateLimit-Remaining")
        self.limit_requests = response.headers.get("X-RateLimit-Limit")
        self.reset_time = response.headers.get("X-RateLimit-Reset")

    def query_articles(self, start_year, end_year, keyword):
        """
        Query ADS for articles between start_year and end_year that match the keyword.
        Returns a DataFrame of results.
        """
        all_results = []

        for year in range(start_year, end_year + 1):
            params = {
                "q": f"year:{year} {keyword} doctype:article",
                "fl": "abstract,aff,author,bibcode,bibstem,citation_count,date,database,doi,doctype," 
                      "first_author,keyword,pub,pubdate,read_count,title,year,arxiv_class,property",
                "fq": "property:refereed",
                "rows": 2000,
                "start": 0
            }

            while True:
                response = requests.get(self.base_url, headers=self.headers, params=params)
                self.requests_made += 1
                self._update_rate_limit_info(response)

                if response.status_code != 200:
                    print(f"❌ Error querying ADS API for year {year}: {response.status_code}")
                    break

                data = response.json().get("response", {}).get("docs", [])
                if not data:
                    break

                all_results.extend(data)
                params["start"] += params["rows"]

        df = pd.DataFrame(all_results)
        df = df[df['doi'].notnull()]  # 🔍 Exclude entries with no DOI
        return df

    def get_references_or_citations_batched(self, bibcodes, query_type="references", batch_size=50):
        """
        Fetch references or citations for a list of bibcodes in batches using fewer API requests.
        """
        results = {}
        batches = [bibcodes[i:i + batch_size] for i in range(0, len(bibcodes), batch_size)]

        for batch_num, batch in enumerate(batches, start=1):
            bibcode_query = " OR ".join([f"bibcode:{b}" for b in batch])
            params = {
                "q": bibcode_query,
                "fl": "bibcode,reference",
                "rows": len(batch)
            }

            response = requests.get(self.base_url, headers=self.headers, params=params)
            self.requests_made += 1
            self._update_rate_limit_info(response)

            if response.status_code != 200:
                print(f"❌ Error retrieving {query_type} for batch {batch_num}: {response.status_code}")
                for b in batch:
                    results[b] = []
                continue

            docs = response.json().get("response", {}).get("docs", [])
            for doc in docs:
                results[doc["bibcode"]] = doc.get("reference", [])

            print(f"✅ Processed batch {batch_num} / {len(batches)}")
            time.sleep(1)

        return results

    def add_references_to_dataframe(self, df, bibcode_column="bibcode", output_prefix="ads_updated", batch_size=100):
        """
        Add references to the DataFrame using batched API requests.
        """
        bibcodes = df[bibcode_column].dropna().tolist()
        bibcode_to_refs = self.get_references_or_citations_batched(bibcodes, query_type="references", batch_size=batch_size)

        df["references"] = df[bibcode_column].map(lambda b: ", ".join(bibcode_to_refs.get(b, [])) if bibcode_to_refs.get(b) else "None")

        csv_name = f"{output_prefix}_with_references.csv"
        json_name = f"{output_prefix}_with_references.json"
        self.save_to_csv(df, csv_name)
        self.save_to_json(df, json_name)

        return df

    def query_and_add_references(self, start_year, end_year, keyword, output_prefix="ads_combined", batch_size=100):
        """
        Combined function to query articles and fetch their references.
        """
        print(f"🔍 Querying articles from {start_year} to {end_year} with keyword '{keyword}'...")
        df_articles = self.query_articles(start_year, end_year, keyword)
        print(f"📄 Retrieved {len(df_articles)} articles with DOI. Now fetching references...")
        df_final = self.add_references_to_dataframe(df_articles, output_prefix=output_prefix, batch_size=batch_size)

        print(f"📊 Total requests made: {self.requests_made}")
        if self.remaining_requests is not None:
            print(f"🧮 Remaining requests: {self.remaining_requests} / {self.limit_requests}, resets at: {self.reset_time}")

        return df_final

    def save_to_csv(self, df, filename):
        try:
            df.to_csv(filename, index=False)
            print(f"💾 Saved CSV to {filename}")
        except Exception as e:
            print(f"❌ Error saving CSV: {e}")

    def save_to_json(self, df, filename):
        try:
            df.to_json(filename, orient="records", indent=4)
            print(f"💾 Saved JSON to {filename}")
        except Exception as e:
            print(f"❌ Error saving JSON: {e}")

    def get_total_requests_made(self):
        return self.requests_made


# Example usage (replace 'your_api_token' with a valid ADS API token)
api_token = "your_api_token"
keyword = "Astrobiology"
start_year = 2000
end_year = 2025
batch_size = 100
output_prefix = f"ads_{keyword}_{start_year}_{end_year}"
ads = ADSQuery(api_token)
df_combined = ads.query_and_add_references(start_year=start_year, end_year=end_year, keyword=keyword, output_prefix=output_prefix, batch_size=batch_size)