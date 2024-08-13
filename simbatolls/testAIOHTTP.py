import pandas as pd 
from flask import Flask, jsonify, request, json
import requests
import time
import hashlib
import hmac
import aiohttp
import asyncio

app = Flask(__name__)

async def fetch(session, url, headers):
    async with session.get(url, headers=headers) as response:
        response.raise_for_status()
        print(await response.json());
        return await response.json()

# @app.route('/nested_api', methods=['GET'])
def nested_api():
    app = Flask(__name__)
    with app.app_context():
        try:
            # First API call
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
            
            # response = requests.get(domain + url, headers=headers)
            response1 = requests.get(domain + url, headers=headers)
            response1.raise_for_status()
            data1 = response1.json()

            # print(data1);

            # Extract parameters that meet a specific condition
            parameters = []
            for item in data1:
                parameter = item.get('carid')
                if parameter:
                    parameters.append(parameter)

            aggregated_data = {
                'data1': data1,
                'data2': []
            }

            # print(aggregated_data)

            if parameters:
                async def fetch_all():
                    async with aiohttp.ClientSession() as session:
                        tasks = []
                        # print(parameters);
                        for parameter in parameters:
                            # print(parameter);
                            domain = "https://apis.rentalcarmanager.com/"
                            
                            urlpath = "/export/ReservationsExport.ashx"
                            secure_key = "QXVTaW1iYUNhckhpcmU3NTl8YWRtaW5Ac2ltYmFjYXJoaXJlLmNvbS5hdXxPd0psdWhCRA=="
                            url = f"{urlpath}?key={secure_key}&method=vehicle&vid={parameter}&sd=20230801-0000&ed=20240802-0000"
                            # print(url);
                            # add other parameters here as required
                            shared_secret = f"flEuz6agE7uKhdau3ohpxPqPeDmrFGh7"
                            # print(shared_secret);
                            # Hash the url
                            my_hmac = hmac.new(shared_secret.encode('utf-8'), url.encode('utf-8'), hashlib.sha256)
                            hashed_bytes = my_hmac.digest()
                            
                            # Convert the hash to hex
                            str_signature = ''.join(f"{b:02x}" for b in hashed_bytes)
                            
                            headers = {
                                "signature": str_signature
                            }
                            # print(domain + url)
                            # response = requests.get(domain + url, headers=headers)
                            # url = f'https://api.example.com/data2?param={parameter}'
                            url = domain + url
                            # print(url)
                            tasks.append(fetch(session, url, headers))
                            # print(fetch(session, url, headers));
                        

                        # responses = await asyncio.gather(*tasks)
                        # print("-----------------------");
                        # print(responses);
                        # return responses

                # Run the asynchronous function
                loop = asyncio.get_event_loop()
                data2 = loop.run_until_complete(fetch_all())
                # print(data2);
                aggregated_data['data2'] = data2

                # print(aggregated_data);
                
            else:
                return jsonify({'error': 'No parameters met the condition'}), 400

            # print(aggregated_data)
            return jsonify(aggregated_data), 200
        except requests.exceptions.RequestException as e:
            return jsonify({'error': 'Failed to fetch data: ' + str(e)}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500


nested_api()
if __name__ == '__main__':
    app.run(debug=True)
