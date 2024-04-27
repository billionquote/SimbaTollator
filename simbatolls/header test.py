import pandas as pd 
    
path="/Users/Bardia/Desktop/My Toll Test Files/Testing.xlsx"
rcm_df = pd.read_excel(path, header=None)
rcm_df.columns=rcm_df.iloc[2]
rcm_df=rcm_df.iloc[3:]
rcm_df.reset_index(drop=True, inplace=True)

rcm_df['RCM_Rego'] = rcm_df.apply(
    lambda row: row['Vehicle'].split(str(row['Pickup']))[0].strip()
    if pd.notna(row['Pickup']) and pd.notna(row['Vehicle']) and str(row['Pickup']) in str(row['Vehicle'])
    else row['Vehicle'],
    axis=1
)

rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split(' ', n=1).str.get(1)
rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split('.').str.get(0)
rcm_df['Vehicle'] = rcm_df['Vehicle'].str.lstrip('0')

print(rcm_df['RCM_Rego'])