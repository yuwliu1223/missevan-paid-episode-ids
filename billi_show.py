import datetime
import requests
import csv

base_url = "https://show.bilibili.com/api/ticket/project/listV2"
params = {
    "version": 134,
    "page": 1,
    "pagesize": 16,
    "area": -1,
    "filter": "",
    "platform": "web",
    "p_type": "全部类型"
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://show.bilibili.com/platform/detail.html?id=134",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}


def fetch_data(page):
    params["page"] = page
    response = requests.get(base_url, params=params, headers=headers)
    try:
        data = response.json()
        return data
    except ValueError:
        print(f"Error: Unable to decode JSON response for page {page}")
        print("Response content:", response.text)
        return None


def runner():
    all_results = []

    initial_data = fetch_data(1)
    if initial_data is None:
        print("Failed to fetch initial data. Exiting...")
        return

    total_pages = initial_data["data"]["numPages"]
    all_results.extend(initial_data["data"]["result"])

    for page in range(2, total_pages + 1):
        data = fetch_data(page)
        if data is not None:
            all_results.extend(data["data"]["result"])

    print(f"Total results fetched: {len(all_results)}")

    csv_file = f"{datetime.date.today()}_bili_shows.csv"

    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow([
            "city", "countdown", "tlabel", "project_name", "venue_name",
            "sale_flag", "start_time", "end_time", "district_name",
            "price_low", "price_high"
        ])


        # Write the data rows
        for result in all_results:
            writer.writerow([
                result.get("city"),
                result.get("countdown"),
                result.get("tlabel"),
                result.get("project_name"),
                result.get("venue_name"),
                result.get("sale_flag"),
                result.get("start_time"),
                result.get("end_time"),
                result.get("district_name"),
                (int(result.get("price_low")) / 100),
                (int(result.get("price_high")) / 100)
            ])

    print(f"Data saved to {csv_file}")

if __name__ == '__main__':
    runner()
