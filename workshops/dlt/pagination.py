import requests

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

page_number = 1
while True:
    params = {'page': page_number}
    response = requests.get(BASE_API_URL, params=params)
    page_data = response.json()

    if not page_data:
        break

    print(page_data)
    page_number += 1

    # limit the number of pages for testing
    if page_number > 2:
      break