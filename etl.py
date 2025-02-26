# import time
from datetime import date, datetime

import pandas as pd
import psycopg2  # database connection and operation
import requests  # for api interaction
import yaml


def extract_data(URL, start_date, end_date) -> list:
    headers = {
        'Accept': 'application/json',
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(f"{URL}/{str(start_date)}/{str(end_date)}",
                            params={},
                            headers=headers)
    print(f"Response Code: {response.status_code}")
    data = response.json()["data"]
    return data


def transform_data(data) -> list:
    TRANSFORMED_data = []
    for entry in data:
        time = datetime.strptime(entry['from'], '%Y-%m-%dT%H:%MZ')
        date_rec = time.strftime('%Y-%m-%d')
        time_rec = time.strftime('%H:%M')
        day = time.strftime('%A')
        month = time.strftime('%B')
        for region in entry['regions']:
            dnoregion = region['dnoregion']
            regionid = region['regionid']
            intensity_forecast = region['intensity']['forecast']
            intensity_index = region['intensity']['index']
            generation_mix_data = {}
            for fuel_data in region['generationmix']:
                fuel_type = fuel_data['fuel']
                percentage = fuel_data['perc']
                generation_mix_data[fuel_type] = percentage
            TRANSFORMED_data.append({
                'date': date_rec,
                'from': time_rec,
                'day_recorded': day,
                'month_recorded': month,
                'dnoregion': dnoregion,
                'regionid': regionid,
                'intensity_forecast': intensity_forecast,
                'intensity_index': intensity_index,
                **generation_mix_data
            })
    return TRANSFORMED_data
    pass


def connectDB() -> tuple:
    with open('conn.yaml', 'r') as file:
        config = yaml.safe_load(file)

        host = config.get('host')
        user = config.get('user')
        db = config.get('database')
        password = config.get('password')
        port = config.get('port')

        conn = psycopg2.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cur = conn.cursor()
        print('Connected succesfully!')
    return conn, cur


def load_data_db(data, conn, cur) -> None:
    data_count = 0
    for data_point in data:
        # Extract values from the tranformed data
        date_rec = data_point['date']
        time_rec = data_point['from']
        day_recorded = data_point['day_recorded']
        month_recorded = data_point['month_recorded']
        dnoregion = data_point['dnoregion']
        regionid = data_point['regionid']
        intensity_forecast = data_point['intensity_forecast']
        intensity_index = data_point['intensity_index']
        biomass = data_point.get('biomass')  # Use.get() to handle missing values
        coal = data_point.get('coal')
        imports = data_point.get('imports')
        gas = data_point.get('gas')
        nuclear = data_point.get('nuclear')
        other = data_point.get('other')
        hydro = data_point.get('hydro')
        solar = data_point.get('solar')
        wind = data_point.get('wind')

        # SQL query to insert data
        insert_query = """
            INSERT INTO carbon_intensity ("date", "from", day_recorded, month_recorded, 
            dnoregion, region_id, intensity_forecast, intensity_index, 
            biomass, coal, imports, gas, nuclear, other, hydro, solar, wind)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Execute the query with the data
        cur.execute(insert_query, (date_rec, time_rec, day_recorded, month_recorded,
                                   dnoregion, regionid, intensity_forecast,
                                   intensity_index, biomass, coal, imports, gas,
                                   nuclear, other, hydro, solar, wind))

        # Count the number of records
        data_count += 1

    # Commit the changes to the database
    conn.commit()
    print(f"{data_count} records inserted successfully!")


def load_data_csv(data, fname="carbon_intensity_data") -> None:
    df = pd.DataFrame(data)
    re_no = df.shape[0]
    df.to_csv(f"{fname}.csv", index=True, index_label="ID")
    print(f"{re_no} records successfully saved to csv.")


if __name__ == "__main__()":
    BASE_URL = "https://api.carbonintensity.org.uk/regional/intensity"
    start = date(2024, 1, 1)
    end = date(2024, 1, 31)

    print("Commenced Data Extraction!")
    data = extract_data(URL=BASE_URL, start_date=start, end_date=end)
    print("Ended Data Extraction, Commencing Transformation!")
    transformed_data = transform_data(data=data)
    print("Connecting to DB....")
    conn, curr = connectDB()
    print('Loading Data to Database...')
    load_data_db(data=transformed_data, conn=conn, cur=curr)
    print("saving data to csv...")
    load_data_csv(transformed_data)
