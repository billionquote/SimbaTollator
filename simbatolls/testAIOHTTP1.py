import pandas as pd 
from flask import Flask, jsonify, request, json
import requests
import time
import hashlib
import hmac
import aiohttp
import asyncio

# async def fetch(session, url, headers):
#     async with session.get(url,headers=headers) as response:
#         return await response.text()

# async def main():
#     # First API

#     domain = "https://apis.rentalcarmanager.com/"
#     urlpath = "/export/ReservationsExport.ashx"
#     secure_key = "QXVTaW1iYUNhckhpcmU3NTl8YWRtaW5Ac2ltYmFjYXJoaXJlLmNvbS5hdXxPd0psdWhCRA=="
#     url = f"{urlpath}?key={secure_key}&method=repbookingexport&lid=9&sd=20240801-0000&ed=20240802-0000"
#     # add other parameters here as required
#     shared_secret = "flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
    
#     # Hash the url
#     my_hmac = hmac.new(shared_secret.encode('utf-8'), url.encode('utf-8'), hashlib.sha256)
#     hashed_bytes = my_hmac.digest()
    
#     # Convert the hash to hex
#     str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
    
#     headers = {
#         "signature": str_signature
#     }
    
#     # response = requests.get(domain + url, headers=headers)
#     response1 = requests.get(domain + url, headers=headers)
#     response1.raise_for_status()
#     data1 = response1.json()

#     # print(data1);

#     # Extract parameters that meet a specific condition
#     parameters = []
#     for item in data1:
#         parameter = item.get('carid')
#         if parameter:
#             parameters.append(parameter)

#     aggregated_data = {
#         'data1': data1,
#         'data2': []
#     }

#     print(aggregated_data)


#     # Second API
#     if parameters:
#         for parameter in parameters:
#             print(parameter)
#             domain = "https://apis.rentalcarmanager.com/"
                                    
#             urlpath = "/export/ReservationsExport.ashx"
#             secure_key = "QXVTaW1iYUNhckhpcmU3NTl8YWRtaW5Ac2ltYmFjYXJoaXJlLmNvbS5hdXxPd0psdWhCRA=="
#             url = f"{urlpath}?key={secure_key}&method=vehicle&vid={parameter}&sd=20230801-0000&ed=20240802-0000"
#             # print(url);
#             # add other parameters here as required
#             shared_secret = f"flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
#             # print(shared_secret);
#             # Hash the url
#             my_hmac = hmac.new(shared_secret.encode('utf-8'), url.encode('utf-8'), hashlib.sha256)
#             hashed_bytes = my_hmac.digest()
            
#             # Convert the hash to hex
#             str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
            
#             headers = {
#                 "signature": str_signature
#             }
            
#             async with aiohttp.ClientSession() as session:
#                 tasks = []
#                 # for url in urls:
#                 tasks.append(fetch(session, domain + url, headers))
                
#                 responses = await asyncio.gather(*tasks)
                
#                 for response in responses:
#                     print(response)
#                     # return response

# if __name__ == '__main__':
#     asyncio.run(main())



async def fetch(session, url, headers):
    async with session.get(url,headers=headers) as response:
        response.raise_for_status()
        return await response.json()

async def main():
    # url1 = 'https://api.example.com/data1'
    # url2 = 'https://api.example.com/data2'

    domain = "https://apis.rentalcarmanager.com/"
    urlpath = "/export/ReservationsExport.ashx"
    secure_key = "QXVTaW1iYUNhckhpcmU3NTl8YWRtaW5Ac2ltYmFjYXJoaXJlLmNvbS5hdXxPd0psdWhCRA=="
    url1 = f"{urlpath}?key={secure_key}&method=repbookingexport&lid=9&sd=20240801-0000&ed=20240802-0000"
    # add other parameters here as required
    shared_secret = "flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
    
    # Hash the url
    my_hmac = hmac.new(shared_secret.encode('utf-8'), url1.encode('utf-8'), hashlib.sha256)
    hashed_bytes = my_hmac.digest()
    
    # Convert the hash to hex
    str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
    
    headers1 = {
        "signature": str_signature
    }

    # Second API

    url2 = f"{urlpath}?key={secure_key}&method=vehicle&vid=0&sd=20230801-0000&ed=20240802-0000"
    # add other parameters here as required
    shared_secret = "flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
    
    # Hash the url
    my_hmac = hmac.new(shared_secret.encode('utf-8'), url2.encode('utf-8'), hashlib.sha256)
    hashed_bytes = my_hmac.digest()
    
    # Convert the hash to hex
    str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
    
    headers2 = {
        "signature": str_signature
    }
    
    print(url1);
    print(url2);

    async with aiohttp.ClientSession() as session:
        # Fetch data from both APIs concurrently
        data1, data2 = await asyncio.gather(
            fetch(session, domain + url1, headers1),
            fetch(session, domain + url2, headers2)
        )

        # Extract IDs and filter matching ones
        ids1 = {item['carid'] for item in data1}
        ids2 = {item['id'] for item in data2}
        
        matching_ids = ids1.intersection(ids2)
        print(len(matching_ids));
        print(matching_ids);
        
        # Additional parameter condition
        parameter_condition = 'Returned'

        # Filter and combine the items with matching IDs and the parameter condition
        combined_data = []
        for item1 in data1:
            if item1['carid'] in matching_ids and item1.get('bookingtype').strip() == parameter_condition:
                for item2 in data2:
                    if item1['carid'] == item2['id']:
                        # combined_item = {**item1, **item2}
                        PickupDateTime = item1['pickupdatetime'].split(' ')
                        PickupDate = PickupDateTime[0]
                        PickupTime = PickupDateTime[1]
                        DropoffDateTime = item1['dropoffdatetime'].split(' ')
                        DropoffDate = DropoffDateTime[0]
                        DropoffTime = DropoffDateTime[1]

                        fleetno = item2['fleetno'].split(' ')
                        vehicle = fleetno[1]

                        combined_item = {
                            'reservationNo': item1['reservationno'],
                            'referenceNo': item1['referenceno'],
                            'update': '1',
                            'notes': '1',
                            'Status': item1['bookingtype'].strip(),
                            'DropoffDate': DropoffDate,
                            'day': '1',
                            'DropoffTime': DropoffTime,
                            'pickup': item1['pickuplocation'],
                            'PickupDate': PickupDate,
                            'PickupTime': PickupTime,
                            'days': '1',
                            'category': item1['vehiclecategory'],
                            'Colour': 'White',
                            'items': '1',
                            'insurance': '1',
                            'departure': '1',
                            'nextRental': '1',
                            'PickupDateTime': item1['pickupdatetime'],
                            'DropoffDateTime': item1['dropoffdatetime'],
                            'CarID': item1['carid'],
                            'RCM_Rego': item2['registrationno'],
                            'Vehicle': vehicle
                        }
                        
                        combined_data.append(combined_item)

        # Print the combined data
        # print("Combined Data:", combined_data)

        df = pd.DataFrame(combined_data).drop_duplicates(subset=['reservationNo', 'Vehicle', 'PickupDateTime', 'DropoffDateTime'])  
        print(len(df))
        df.to_csv("matching_data.csv")
        df = pd.read_csv('matching_data.csv')
        newdf = df.fillna(1)
        newdf.to_csv("matching_data.csv")

        return newdf

if __name__ == '__main__':
    rcm_df = asyncio.run(main())
    print(rcm_df)

