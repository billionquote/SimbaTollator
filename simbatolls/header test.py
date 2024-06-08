import pandas as pd 
    
path="/Users/Bardia/Desktop/My Toll Test Files/The one with status 4 june/no touchReport_Daily Activity - 2024-06-08T104305.452.xlsx"
rcm_df = pd.read_excel(path, header=None)
rcm_df.columns=rcm_df.iloc[2]
rcm_df=rcm_df.iloc[3:]
rcm_df.reset_index(drop=True, inplace=True)
    
rcm_df['RCM_Rego'] = rcm_df.apply(
        lambda row: row['Vehicle'].split(str(row['Pickup']))[0].strip()
        if pd.notna(row['Pickup']) and pd.notna(row['Vehicle']) and str(row['Pickup']) in str(row['Vehicle'])
        else str(row['Vehicle']).strip(),
        axis=1
    )
rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split(' ', n=1).str.get(1)
rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split('.').str.get(0)
rcm_df['Vehicle'] = rcm_df['Vehicle'].str.lstrip('0')
rcm_df['Vehicle']= rcm_df['Vehicle'].astype(str)
rcm_df['Vehicle'] =  rcm_df['Vehicle'].astype(str).str.replace(r'\.0$', '', regex=True)
    # Fill NaN values with an empty string (or any other placeholder if needed)
print("Before filling NaN values in 'Status':")
rcm_df['Status'] = rcm_df['Status'].fillna('PlaceHolder')
rcm_df['Status'] =  rcm_df['Status'].str.strip().str.upper()
print(f'this is before filtering" {rcm_df['Status']}') 
rcm_df['Status'] = rcm_df['Status'].str.replace(r'.*RETURNED.*', 'RETURNED', regex=True)
rcm_df['Status'] = rcm_df['Status'].str.strip().str.upper()
print(f"After filtering 'Status' for 'RETURNED': {rcm_df['Status']}")
rcm_df = rcm_df[rcm_df['Status'] == 'RETURNED']
print("After filtering 'Status' for 'RETURNED':")
print(rcm_df['Status'])
rcm_df = rcm_df.rename(columns={rcm_df.columns[0]: '#'})
col_to_dedup=['Dropoff', 'Ref.','Update', '#', 'Notes', 'Day', '# Days', 'Category', 'Items', 'Insurance', 'Next Rental', 'Rental Value', 'Daily Rate', 'Departure', 'Balance' ]
rcm_df[col_to_dedup] = rcm_df[col_to_dedup].fillna('PlaceHolder')
print("After filling NaN values in columns to dedup:")
rcm_df[col_to_dedup]='A'
rcm_df = rcm_df.drop_duplicates()
print("After setting columns to dedup to 'A':")
print(f'deleting all that does not exist')
rcm_df.dropna(how='all', inplace=True)

    #try:
        #rcm_df['Vehicle'] = rcm_df['Vehicle'].astype(int)
    #except ValueError:
         #print('Could not handle formatting rcm file vehicle column file')
try:
    rcm_df['Pickup Date'] = pd.to_datetime(rcm_df['Pickup Date'], format='%d/%b/%Y').dt.strftime('%Y-%m-%d')
    rcm_df['Pickup Date Time'] = pd.to_datetime(rcm_df['Pickup Date'] + ' ' + rcm_df['Time_c13']).dt.strftime('%Y-%m-%d %H:%M:%S')
    print('fixed pick up date time')
    rcm_df['Dropoff Date'] = pd.to_datetime(rcm_df['Dropoff Date'], format='%d/%b/%Y').dt.strftime('%Y-%m-%d')
    print(f' my drop off date: {rcm_df['Dropoff Date']}')
    print('drop off date fixed ')                                       
    rcm_df['Dropoff Date Time'] = pd.to_datetime(rcm_df['Dropoff Date'] + ' ' + rcm_df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    print('fixed drop off date time')
    print(f' my drop off date time : {rcm_df['Dropoff Date Time']}')
    rcm_df.drop(['Customer', 'Mobile', 'Daily Rate', 'Rental Value', 'Balance'], inplace=True, axis=1)
    rcm_df = rcm_df.drop_duplicates()
except ValueError:
    print('Could not handle formatting rcm date and time file')
    #drop duplicates
rcm_df.drop_duplicates(subset=['Res.', 'Vehicle', 'Pickup Date Time', 'Dropoff Date Time'], inplace=True)
rcm_df.to_csv('check end results.csv')