import pandas as pd 
from flask import Flask, jsonify, request, json
import requests
import time
import hashlib
import hmac
import aiohttp
import asyncio
    
# path="/Users/rakesh/Downloads/All returns till and inclduing 2Aug 24.xlsx"
# rcm_df = pd.read_excel(path, header=None)
# rcm_df.columns=rcm_df.iloc[2]
# rcm_df=rcm_df.iloc[3:]
# rcm_df.reset_index(drop=True, inplace=True)

def returns_multiple_records_demo():
    app = Flask(__name__)
    with app.app_context():
        domain = "https://apis.rentalcarmanager.com/"
        urlpath = "/export/ReservationsExport.ashx"
        secure_key = "QXVTaW1iYUNhckhpcmU3NTl8YWRtaW5Ac2ltYmFjYXJoaXJlLmNvbS5hdXxPd0psdWhCRA=="
        url = f"{urlpath}?key={secure_key}&method=repbookingexport&lid=9&sd=20240801-0000&ed=20240802-0000"
        # add other parameters here as required
        shared_secret = "flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
        
        # Hash the url
        my_hmac = hmac.new(shared_secret.encode('utf-8'), url.encode('utf-8'), hashlib.sha256)
        hashed_bytes = my_hmac.digest()
        
        # Convert the hash to hex
        str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
        
        headers = {
            "signature": str_signature
        }
        
        response = requests.get(domain + url, headers=headers)
        print(domain + url)

        # Check if the request was successful
        if response.status_code != 200:
            return jsonify({'error': 'Failed to fetch data from the API'}), 500
        
        # Check response
        # print(response.text)
        list_records = json.loads(response.text)
        print("Found Records:", len(list_records))
        # print(list_records[0].get('carid'))

        # print(list_records);

        # Process each item in the JSON array
        results = []
        #rcm_df = pd.DataFrame(rcm_df)
        
        for item in list_records:
            # Extract values from each JSON object in the array
                
            # print(item.get('bookingtype').strip());
            try:
                if item.get('bookingtype').strip() == 'Returned':
                    
                    CarID = item.get('carid')
                    Status = item.get('bookingtype')
                    print("CarID:")
                    print(CarID)
                    PickupDateTime = item.get('pickupdatetime').split(' ')
                    PickupDate = PickupDateTime[0]
                    PickupTime = PickupDateTime[1]
                    DropoffDateTime = item.get('dropoffdatetime').split(' ')
                    DropoffDate = DropoffDateTime[0]
                    DropoffTime = DropoffDateTime[1]
                    print(PickupDate)
                    print(PickupTime)
                    category = item.get('vehiclecategory')
                    reservationNo = item.get('reservationno')
                    referenceNo = item.get('referenceno')
                    update = '1'
                    notes = '1'
                    day = '1'
                    pickup = item.get('pickuplocation')
                    days = '1'
                    Colour = 'White'
                    items = '1'
                    insurance = '1'
                    departure = '1'
                    nextRental = '1'
                    PickupDateTime = item.get('pickupdatetime')
                    DropoffDateTime = item.get('dropoffdatetime')
                    CarID = CarID

                    print(item.get('bookingtype').strip());
            
                    result = {
                        'reservationNo': reservationNo,
                        'referenceNo': referenceNo,
                        'update': update,
                        'notes': notes,
                        'Status': Status,
                        'DropoffDate': DropoffDate,
                        'day': day,
                        'DropoffTime': DropoffTime,
                        'pickup': pickup,
                        'PickupDate': PickupDate,
                        'PickupTime': PickupTime,
                        'days': days,
                        'category': category,
                        'Colour': Colour,
                        'items': items,
                        'insurance': insurance,
                        'departure': departure,
                        'nextRental': nextRental,
                        'PickupDateTime': PickupDateTime,
                        'DropoffDateTime': DropoffDateTime,
                        'CarID': CarID
                    }
                    results.append(result)
                    
                    rcm_df = pd.DataFrame(results)
                    newdf = rcm_df.drop_duplicates()
                    print(rcm_df)
                    # print(newdf)

                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
            
        aggregated_data = {
            'data1': results,
            'data2': []
        }

        print(aggregated_data)

        if results:
            
            for result in results:

                print("--------------CarID--------------")
                print(result.get('CarID'))

                domain = "https://apis.rentalcarmanager.com/"
                urlpath = "/export/ReservationsExport.ashx"
                secure_key = "QXVTaW1iYUNhckhpcmU3NTl8YWRtaW5Ac2ltYmFjYXJoaXJlLmNvbS5hdXxPd0psdWhCRA=="
                url = f"{urlpath}?key={secure_key}&method=vehicle&vid={result.get('CarID')}&sd=20230801-0000&ed=20240802-0000"
                # add other parameters here as required
                shared_secret = "flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
                
                # Hash the url
                my_hmac = hmac.new(shared_secret.encode('utf-8'), url.encode('utf-8'), hashlib.sha256)
                hashed_bytes = my_hmac.digest()
                
                # Convert the hash to hex
                str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
                
                headers = {
                    "signature": str_signature
                }
                
                response = requests.get(domain + url, headers=headers)
                        # print(domain + url)

        #         if response.status_code != 200:
        #             return jsonify({'error': 'Failed to fetch data from the API'}), 500
                
        #         # Check response
        #         # print(response.text)
        #         rego_records = json.loads(response.text)
        #         print("Found Records:", len(rego_records))
        #         # print(rego_records);
        #         if rego_records:
        #             for item1 in rego_records:
        #             # Extract values from each JSON object in the array
        #                 fleetno = item1.get('fleetno')
        #                 RCM_Rego = item1.get('registrationno')

        #                 print(RCM_Rego)
        #                 print(fleetno)
        #                 Vehicle = fleetno.split(' ')
        #                 print(Vehicle[1])     
        #                 result = {
        #                     'RCM_Rego': RCM_Rego,
        #                     'Vehicle': Vehicle[1]
        #                 }

        #                 results.append(result)

        # print(aggregated_data);
                
        # try:
        #     if item.get('bookingtype').strip() == 'Returned':
        #         CarID = item.get('carid')
        #         Status = item.get('bookingtype')
        #         print("CarID:")
        #         print(CarID)
        #         PickupDateTime = item.get('pickupdatetime').split(' ')
        #         PickupDate = PickupDateTime[0]
        #         PickupTime = PickupDateTime[1]
        #         DropoffDateTime = item.get('dropoffdatetime').split(' ')
        #         DropoffDate = DropoffDateTime[0]
        #         DropoffTime = DropoffDateTime[1]
        #         print(PickupDate)
        #         print(PickupTime)
        #         category = item.get('vehiclecategory')
        #         reservationNo = item.get('reservationno')
        #         referenceNo = item.get('referenceno')
        #         update = '1'
        #         notes = '1'
        #         day = '1'
        #         pickup = item.get('pickuplocation')
        #         days = '1'
        #         Colour = 'White'
        #         items = '1'
        #         insurance = '1'
        #         departure = '1'
        #         nextRental = '1'
        #         PickupDateTime = item.get('pickupdatetime')
        #         DropoffDateTime = item.get('dropoffdatetime')

        #         # newreferenceNo = referenceNo.fillna(1)


        #         domain = "https://apis.rentalcarmanager.com/"
        #         urlpath = "/export/ReservationsExport.ashx"
        #         secure_key = "QXVTaW1iYUNhckhpcmU3NTl8YWRtaW5Ac2ltYmFjYXJoaXJlLmNvbS5hdXxPd0psdWhCRA=="
        #         url = f"{urlpath}?key={secure_key}&method=vehicle&vid={CarID}&sd=20230801-0000&ed=20240802-0000"
        #         # add other parameters here as required
        #         shared_secret = "flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
                
        #         # Hash the url
        #         my_hmac = hmac.new(shared_secret.encode('utf-8'), url.encode('utf-8'), hashlib.sha256)
        #         hashed_bytes = my_hmac.digest()
                
        #         # Convert the hash to hex
        #         str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
                
        #         headers = {
        #             "signature": str_signature
        #         }
                
        #         response = requests.get(domain + url, headers=headers)
        #         # print(domain + url)
                
        #         # Check response
        #         # print(response.text)
        #         list_records = json.loads(response.text)
        #         print("Found Records:", len(list_records))
        #         for item1 in list_records:
        #         # Extract values from each JSON object in the array
        #             fleetno = item1.get('fleetno')
        #             RCM_Rego = item1.get('registrationno')

        #             print(RCM_Rego)
        #             print(fleetno)
        #             Vehicle = fleetno.split(' ')
        #             print(Vehicle[1])

        #         result = {
        #             'reservationNo': reservationNo,
        #             'referenceNo': referenceNo,
        #             'update': update,
        #             'notes': notes,
        #             'Status': Status,
        #             'DropoffDate': DropoffDate,
        #             'day': day,
        #             'DropoffTime': DropoffTime,
        #             'pickup': pickup,
        #             'PickupDate': PickupDate,
        #             'PickupTime': PickupTime,
        #             'days': days,
        #             'category': category,
        #             'Vehicle': Vehicle[1],
        #             'Colour': Colour,
        #             'items': items,
        #             'insurance': insurance,
        #             'departure': departure,
        #             'nextRental': nextRental,
        #             'RCM_Rego': RCM_Rego,
        #             'PickupDateTime': PickupDateTime,
        #             'DropoffDateTime': DropoffDateTime
        #         }
        #         results.append(result)
        #         print(results);

        #         # return jsonify({'results': results}), 200

        #         # rcm_df = pd.DataFrame(results)
        #         # newdf = rcm_df.drop_duplicates()
        #         # print(rcm_df)
        #         # print(newdf)
        # except Exception as e:
        #         return jsonify({'error': str(e)}), 500

    # Define the file path
    # file_path = 'output_list_records1.txt'
    # with open(file_path, 'a') as file:
    #     file.write(str(list_records))

returns_multiple_records_demo()
    
# rcm_df['RCM_Rego'] = rcm_df.apply(
#         lambda row: row['Vehicle'].split(str(row['Pickup']))[0].strip()
#         if pd.notna(row['Pickup']) and pd.notna(row['Vehicle']) and str(row['Pickup']) in str(row['Vehicle'])
#         else str(row['Vehicle']).strip(),
#         axis=1
#     )
# rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split(' ', n=1).str.get(1)
# rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split('.').str.get(0)
# rcm_df['Vehicle'] = rcm_df['Vehicle'].str.lstrip('0')
# rcm_df['Vehicle']= rcm_df['Vehicle'].astype(str)
# rcm_df['Vehicle'] =  rcm_df['Vehicle'].astype(str).str.replace(r'\.0$', '', regex=True)
#     # Fill NaN values with an empty string (or any other placeholder if needed)
# print("Before filling NaN values in 'Status':")
# rcm_df['Status'] = rcm_df['Status'].fillna('PlaceHolder')
# rcm_df['Status'] =  rcm_df['Status'].str.strip().str.upper()
# print(f"this is before filtering {rcm_df['Status']}") 
# rcm_df['Status'] = rcm_df['Status'].str.replace(r'.*RETURNED.*', 'RETURNED', regex=True)
# rcm_df['Status'] = rcm_df['Status'].str.strip().str.upper()
# print(f"After filtering 'Status' for 'RETURNED': {rcm_df['Status']}")
# rcm_df = rcm_df[rcm_df['Status'] == 'RETURNED']
# print("After filtering 'Status' for 'RETURNED':")
# print(rcm_df['Status'])
# rcm_df = rcm_df.rename(columns={rcm_df.columns[0]: '#'})
# col_to_dedup=['Dropoff', 'Ref.','Update', '#', 'Notes', 'Day', '# Days', 'Category', 'Items', 'Insurance', 'Next Rental', 'Rental Value', 'Daily Rate', 'Departure', 'Balance' ]
# rcm_df[col_to_dedup] = rcm_df[col_to_dedup].fillna('PlaceHolder')
# print("After filling NaN values in columns to dedup:")
# rcm_df[col_to_dedup]=1
# rcm_df = rcm_df.drop_duplicates()
# print("After setting columns to dedup to 'A':")
# print(f'deleting all that does not exist')
# rcm_df.dropna(how='all', inplace=True)

#     #try:
#         #rcm_df['Vehicle'] = rcm_df['Vehicle'].astype(int)
#     #except ValueError:
#          #print('Could not handle formatting rcm file vehicle column file')
# try:
#     rcm_df['Pickup Date'] = pd.to_datetime(rcm_df['Pickup Date'], format='%d/%b/%Y').dt.strftime('%Y-%m-%d')
#     rcm_df['Pickup Date Time'] = pd.to_datetime(rcm_df['Pickup Date'] + ' ' + rcm_df['Time_c13']).dt.strftime('%Y-%m-%d %H:%M:%S')
#     print('fixed pick up date time')
#     rcm_df['Dropoff Date'] = pd.to_datetime(rcm_df['Dropoff Date'], format='%d/%b/%Y').dt.strftime('%Y-%m-%d')
#     print(f" my drop off date: {rcm_df['Dropoff Date']}")
#     print('drop off date fixed ')                                       
#     rcm_df['Dropoff Date Time'] = pd.to_datetime(rcm_df['Dropoff Date'] + ' ' + rcm_df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
#     print('fixed drop off date time')
#     print(f" my drop off date time : {rcm_df['Dropoff Date Time']}")
#     rcm_df.drop(['Customer', 'Mobile', 'Daily Rate', 'Rental Value', 'Balance'], inplace=True, axis=1)
#     rcm_df = rcm_df.drop_duplicates()
# except ValueError:
#     print('Could not handle formatting rcm date and time file')
#     #drop duplicates
# rcm_df.drop_duplicates(subset=['Res.', 'Vehicle', 'Pickup Date Time', 'Dropoff Date Time'], inplace=True)
# rcm_df.to_csv('check end results.csv')