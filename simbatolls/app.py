import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, g,session, json
from werkzeug.utils import secure_filename
import pandas as pd
import pandasql as ps
import openpyxl
import os
import time
import sqlite3
import tempfile
from sqlalchemy.sql import text
import traceback2
from sqlalchemy.orm import Session
from sqlalchemy import select, column, create_engine, Table, MetaData
from io import StringIO
from simbatolls.cleaner import cleaner,summary_cleaner
from celery import Celery
from flask_migrate import Migrate
#login fixes 
from flask_login import login_user, LoginManager
from flask_bcrypt import Bcrypt
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from flask_login import login_required
from datetime import timedelta
from rq import Queue
from simbatolls.worker import conn  # Make sure worker.py is accessible as a module
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, inspect
from sqlalchemy.types import Integer, String, Float, DateTime
from sqlalchemy.dialects.postgresql import NUMERIC  # Use NUMERIC for more precise financial data
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sqlalchemy import create_engine, select, func
from sqlalchemy import cast, Date
from sqlalchemy import select, func, cast, Date, tuple_, and_, distinct, text
from sqlalchemy import select, func, distinct, and_, cast, Date, tuple_, extract
import plotly.graph_objs as go
import json
from flask import Flask, render_template, jsonify, request
from flask_login import login_required
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, cast, Date, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, and_
import plotly
#from flask import current_app as app


app = Flask(__name__, template_folder='templates')

app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)

@app.route('/')
def home():
    return render_template('home.html')
#use ful comand for flask upgrade poetry run python -m flask db init 

# Get the DATABASE_URL, replace "postgres://" with "postgresql://"
database_url =os.getenv('DATABASE_URL')
#database_url='postgresql://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr'
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

#create Celery
#app.config['CELERY_BROKER_URL'] = os.environ['REDIS_URL']
#app.config['CELERY_RESULT_BACKEND'] = os.environ['REDIS_URL']

#def make_celery(app):
    #celery = Celery(
        #app.import_name,
        #backend=app.config['CELERY_RESULT_BACKEND'],
        #broker=app.config['CELERY_BROKER_URL']
    #)
    #celery.conf.update(app.config)
    #return celery

# Initialize Celery
#celery = make_celery(app)
#intiialize RQ
q = Queue(connection=conn)
# Assuming 'db' is your SQLAlchemy database instance from 'app.db'
migrate = Migrate(app, db)

bcrypt = Bcrypt(app)
# Initialize LoginManager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'validate'

#run vaccum cleaner to clean the database 
#cleaner()

class User(db.Model, UserMixin):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)

    def __repr__(self):
        return f"User('{self.username}')" 
  
class Summary(db.Model):
    __tablename__ = 'summary'

    contract_number = db.Column(db.Integer, primary_key=True)
    num_of_rows = db.Column(db.Integer)
    sum_of_toll_cost = db.Column(db.Float)
    total_toll_contract_cost = db.Column(db.Float)
    pickup_date_time = db.Column(db.String)
    dropoff_date_time = db.Column(db.String)
    admin_fee = db.Column(db.Float)

    def __repr__(self):
        return f"<Summary contract_number={self.contract_number} num_of_rows={self.num_of_rows}>"

class RawData(db.Model):
    __tablename__ = 'rawdata'

    id = db.Column(db.Integer, primary_key=True)
    start_date = db.Column(db.String)
    details = db.Column(db.String)
    lpn_tag_number = db.Column(db.String)
    vehicle_class = db.Column(db.String)
    trip_cost = db.Column(db.String)
    fleet_id = db.Column(db.String)
    end_date = db.Column(db.String)
    date = db.Column(db.String)
    rego = db.Column(db.String)
    res = db.Column(db.String)
    ref = db.Column(db.String)
    update = db.Column(db.String)
    notes = db.Column(db.String)
    status = db.Column(db.String)
    dropoff = db.Column(db.String)
    day = db.Column(db.String)
    dropoff_date = db.Column(db.String)
    time = db.Column(db.String)
    pickup = db.Column(db.String)
    pickup_date = db.Column(db.String)
    time_c13 = db.Column(db.String)
    num_days = db.Column(db.String, name='# Days')
    category = db.Column(db.String)
    vehicle = db.Column(db.String)
    colour = db.Column(db.String)
    items = db.Column(db.String)
    insurance = db.Column(db.String)
    departure = db.Column(db.String)
    next_rental = db.Column(db.String)
    pickup_date_time = db.Column(db.String)
    dropoff_date_time = db.Column(db.String)
    rcm_rego = db.Column(db.String)
    
    def __repr__(self):
        return f"<RawData id={self.id}>"

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))
#end of login fixxes 



@app.route('/validate', methods=['GET', 'POST'])  # Changed from /login to /validate
def validate_license():
    if request.method == 'POST':
        username_r = request.form.get('username')
        password_r = request.form.get('password')

        if not username_r or not password_r:
            return jsonify({'status': 'invalid', 'message': 'Username or password not provided'}), 400

        user = User.query.filter_by(username=username_r).first()
        if user:
            if bcrypt.check_password_hash(user.password_hash, password_r):
                login_user(user, remember=True)
                return redirect(url_for('home'))
            else:
                return jsonify({'status': 'invalid', 'message': 'Password is incorrect'}), 401
        else:
            return jsonify({'status': 'invalid', 'message': 'Username does not exist'}), 404

    return render_template('login.html')



@app.route('/upload', methods=['POST','GET'])
@login_required
def upload_file():
    # Ensure there are files in the request
    if 'rcmFile' not in request.files or 'tollsFile' not in request.files:
        return jsonify({'message': 'No file part'}), 400

    rcm_file = request.files['rcmFile']
    tolls_file = request.files['tollsFile']

    if rcm_file.filename == '' or tolls_file.filename == '':
        return jsonify({'message': 'No selected file'}), 400

    # Process RCM File
    rcm_df = pd.read_excel(rcm_file, header=None)
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
    #rcm_df = rcm_df[rcm_df['Status'] == 'Returned']
    columns_to_null = [ 'Ref.', 'Update', 'Notes', 'Status', 'Dropoff', 'Day', 'Next Rental', 'Daily Rate', 'Rental Value', 'Balance', 'items', 'insurance']
    rcm_df[columns_to_null] = 'DELETE'
    #try:
        #rcm_df['Vehicle'] = rcm_df['Vehicle'].astype(int)
    #except ValueError:
         #print('Could not handle formatting rcm file vehicle column file')
    try:
        rcm_df['Pickup Date Time'] = pd.to_datetime(rcm_df['Pickup Date'] + ' ' + rcm_df['Time_c13']).dt.strftime('%Y-%m-%d %H:%M:%S')
        rcm_df['Dropoff Date Time'] = pd.to_datetime(rcm_df['Dropoff Date'] + ' ' + rcm_df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        rcm_df.drop(['Customer', 'Mobile', 'Daily Rate', 'Rental Value', 'Balance'], inplace=True, axis=1)
        rcm_df = rcm_df.drop_duplicates()
    except ValueError:
         print('Could not handle formatting rcm date and time file')
    #drop duplicates
    rcm_df.drop_duplicates(subset=['Res.', 'Vehicle', 'Pickup Date Time', 'Dropoff Date Time'], inplace=True)
    # Process Toll File
    tolls_df = pd.read_excel(tolls_file)
    tolls_df['Start Date'] = pd.to_datetime(tolls_df['Start Date'], format="%d %b %Y %I:%M%p")
    tolls_df['Start Date'] = tolls_df['Start Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['Start Date'] = pd.to_datetime(tolls_df['Start Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['End Date'] = pd.to_datetime(tolls_df['End Date'], format="%d %b %Y %I:%M%p")
    tolls_df['End Date'] = tolls_df['End Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['End Date'] = pd.to_datetime(tolls_df['End Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['Trip Cost'] = tolls_df['Trip Cost'].astype(str).str.replace(r'[^0-9.]', '', regex=True)
    #try:
        #tolls_df['LPN/Tag number'] = tolls_df['LPN/Tag number'].astype(int)
    #except ValueError:
        #print('Could not handle formatting toll file') # Handle the case where conversion to int is not possible
    tolls_df = tolls_df.drop_duplicates()
    tolls_df['Trip Cost'] = tolls_df['Trip Cost'].astype(float, errors='ignore')
    tolls_df['Trip Cost'] = tolls_df['Trip Cost'].astype(str).str.replace(r'[^0-9.]', '', regex=True)
    tolls_df['LPN/Tag number'] = tolls_df['LPN/Tag number'].astype(str)
    #drop duplicates in the table 
    tolls_df.drop_duplicates(inplace=True)
    # Convert the first 3 rows of each DataFrame to HTML
    rcm_html = rcm_df.head(3).to_html()
    tolls_html = tolls_df.head(3).to_html()


    if rcm_df.empty or tolls_df.empty:
        return jsonify({'message': 'Uploaded files are empty'}), 400

    rcm_json = rcm_df.to_json()
    tolls_json = tolls_df.to_json()

    # Logging data to ensure it's correct before queuing
    #print(f"RCM JSON: {rcm_json}")
    #print(f"Tolls JSON: {tolls_json}")

    job = q.enqueue(confirm_upload_task, rcm_json, tolls_json)

    return jsonify({
        'rcmPreview': rcm_df.head(3).to_html(),
        'tollsPreview': tolls_df.head(3).to_html(),
        'message': 'Files are being processed',
        'job_id': job.get_id()
    }), 202

def load_dataframes(rcm_df_path, tolls_df_path):
    try:
        # Check if RCM file exists and is not empty
        if os.path.exists(rcm_df_path) and os.path.getsize(rcm_df_path) > 0:
            with open(rcm_df_path, 'r') as file:
                rcm_data = file.read()
                if rcm_data:
                    rcm_df = pd.read_json(StringIO(rcm_data))
                else:
                    print("RCM file has no data.")
                    return None, None
        else:
            print("RCM file is empty or missing.")
            return None, None

        # Check if Tolls file exists and is not empty
        if os.path.exists(tolls_df_path) and os.path.getsize(tolls_df_path) > 0:
            with open(tolls_df_path, 'r') as file:
                tolls_data = file.read()
                if tolls_data:
                    tolls_df = pd.read_json(StringIO(tolls_data))
                else:
                    print("Tolls file has no data.")
                    return None, None
        else:
            print("Tolls file is empty or missing.")
            return None, None

        return rcm_df, tolls_df
    except Exception as e:
        print(f"Failed to load dataframes: {e}")
        return None, None


def populate_summary_table():
    engine = db.engine
    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Fetch data from rawdata table into a DataFrame
        query = "SELECT * FROM rawdata"
        df = pd.read_sql(query, session.bind)
    print("Original DataFrame:", df.head())  # Display initial data for debugging

    # Ensure 'Res.' column is a string and remove any trailing ".0"
    print(f'i am populating summary table with column names: {df.columns}')
    df['res'] = df['res'].astype(str).str.replace(r'\.0$', '', regex=True)
    df['pickup_date_time'] = pd.to_datetime(df['pickup_date_time'])
    df['dropoff_date_time'] = pd.to_datetime(df['dropoff_date_time'])
        # Filtering to show only rows with res 7791 for debugging
    debug_df = df[df['res'] == '7791']
    print(f"Debugging DataFrame for res 7791: {debug_df[['pickup_date_time', 'dropoff_date_time']]}")

    df = df[df['res'].notnull()]
    df = df.drop_duplicates()
    df['trip_cost'] = df['trip_cost'].astype(float)
    # Debug output to see DataFrame before aggregation
    print("DataFrame before aggregation:", df.head())

    # Group by the 'Res.' column and perform aggregations
    summary = df.groupby('res').agg(
        Num_of_Rows=('res', 'size'),
        Sum_of_Toll_Cost=('trip_cost', 'sum')
    ).reset_index()

    # Debug output to see how aggregation results look
    print("Aggregated DataFrame:", summary.head())

    # Calculate grand total and admin fee total
    grand_total = summary['Sum_of_Toll_Cost'].sum()
    admin_fee_total = (summary['Num_of_Rows'] * 2.95).sum()
    summary['admin_fee'] = summary['Num_of_Rows'] * 2.95  # This should be a scalar for each group

    # Debug output to check the admin_fee column
    print("DataFrame after adding admin_fee:", summary.head())

    summary['Total Toll Contract cost'] = summary['admin_fee'] + summary['Sum_of_Toll_Cost']
    summary['Pickup Date Time'] = df['pickup_date_time'].astype(str)
    summary['Dropoff Date Time'] = df['dropoff_date_time'].astype(str)
    
    summary['Sum_of_Toll_Cost'] = summary['Sum_of_Toll_Cost'].round(2)
    summary['Total Toll Contract cost'] = summary['Total Toll Contract cost'].round(2)
    summary['admin_fee'] = summary['admin_fee'].astype(float).round(2).map('{:,.2f}'.format)
    summary['Sum of Toll Cost'] = summary['Sum_of_Toll_Cost'].astype(float).map('{:,.2f}'.format)
    summary['Total Toll Contract cost'] =  summary['Total Toll Contract cost'].astype(float).map('{:,.2f}'.format)

    summary = summary.rename(columns={
        'res': 'Contract Number'
    })
    
    summary['Contract Number'] = summary['Contract Number'].astype(int)
    summary = summary.sort_values(by='Contract Number', ascending=False)

    # Final DataFrame to be returned
    print("Final DataFrame for SQL operations:", summary.head())
    summary=summary.rename(columns={
        'res': 'Contract Number'
    })
    summary = summary.sort_values(by='Contract Number', ascending=False)
        # Rename the columns for clarity
    summary = summary.rename(columns={
        'Contract Number': 'contract_number',  # Ensuring this matches the model's field name
        'Num_of_Rows': 'num_of_rows',
        'Sum of Toll Cost': 'sum_of_toll_cost',
        'Total Toll Contract cost': 'total_toll_contract_cost',
        'Pickup Date Time': 'pickup_date_time',
        'Dropoff Date Time': 'dropoff_date_time',
        'admin_fee': 'admin_fee'
    })
        
    return summary, grand_total, admin_fee_total

def convert_df_types(df):
    # Convert dates to datetime objects if not already
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['End Date'] = pd.to_datetime(df['End Date'])
    df['Pickup Date Time'] = pd.to_datetime(df['Pickup Date Time'])
    df['Dropoff Date Time'] = pd.to_datetime(df['Dropoff Date Time'])

    # Ensure all other fields are treated as strings or their specific type
    string_fields = ['Details', 'LPN/Tag number', 'Vehicle Class', 'Trip Cost',
                     'Fleet ID', 'Date', 'Rego', 'Res.', 'Ref.', 'Update', 'Notes',
                     'Status', 'Dropoff', 'Day', 'Dropoff Date', 'Time', 'Pickup',
                     'Pickup Date', 'Time_c13', 'Category', 'Vehicle', 'Colour',
                     'Items', 'Insurance', 'Departure', 'Next Rental', 'RCM_Rego']
    
    for field in string_fields:
        df[field] = df[field].astype(str)

    # Convert integer fields
    df['# Days'] = df['# Days'].astype(int)

    return df

def create_new_raw_data_record(row):
    # Function to create a new RawData instance from row data
    return RawData(
                start_date=row['Start Date'],
                details=row['Details'],
                lpn_tag_number=row['LPN/Tag number'],
                vehicle_class=row['Vehicle Class'],
                fleet_id=row['Fleet ID'],
                end_date=row['End Date'],
                date=row['Date'],
                rego=row['Rego'],
                res=row['Res.'],
                ref=row['Ref.'],
                update=row['Update'],
                notes=row['Notes'],
                status=row['Status'],
                dropoff=row['Dropoff'],
                day=row['Day'],
                dropoff_date=row['Dropoff Date'],
                time=row['Time'],
                pickup=row['Pickup'],
                pickup_date=row['Pickup Date'],
                time_c13=row['Time_c13'],
                num_days=row['# Days'],
                category=row['Category'],
                vehicle=row['Vehicle'],
                trip_cost = row['Trip Cost'],
                colour=row['Colour'],
                items=row['Items'],
                insurance=row['Insurance'],
                departure=row['Departure'],
                next_rental=row['Next Rental'],
                pickup_date_time=row['Pickup Date Time'],
                dropoff_date_time=row['Dropoff Date Time'],
                rcm_rego=row['RCM_Rego']
        # Initialize other fields similarly...
    )

#change approach
def populate_rawdata_from_df(result_df):
    # Convert DataFrame types
    result_df = convert_df_types(result_df)
    result_df['Res.'] = result_df['Res.'].astype(str).str.replace(r'\.0$', '', regex=True)

    try:
        for _, row in result_df.iterrows():
            # Always create a new record for each row, regardless of existing 'Res.' values
            new_record = create_new_raw_data_record(row)
            db.session.add(new_record)

        # Commit all new records to the database
        db.session.commit()
        print("All data added successfully.")
    except Exception as e:
        db.session.rollback()  # Rollback if any error occurs
        print(f"Error populating rawdata table: {e}")
        raise

# Usage in your application would not change other than ensuring the DataFrame is passed
def confirm_upload_task(rcm_data_json, tolls_data_json):
    try: 
        rcm_df = pd.read_json(StringIO(rcm_data_json))
        tolls_df = pd.read_json(StringIO(tolls_data_json))
        print(f'RCM_DF FROM CONFIRM UPLOAD: {rcm_df.head(3)}')
        print(f'tolls_DF FROM CONFIRM UPLOAD: {tolls_df.head(3)}')
    except ValueError as e:
        print("Error parsing JSON data: We are in Confirm_upload_task", e)
        return {'error': 'Invalid JSON data', 'details': str(e)}, 500
        
    if rcm_df.empty or tolls_df.empty:
        print("Debug: DataFrames are empty")
        return {'error': 'DataFrames are empty'}, 400
    
    rcm_df['Vehicle'] = rcm_df['Vehicle'].astype(str)
    rcm_df['Vehicle'] =  rcm_df['Vehicle'].astype(str).str.replace(r'\.0$', '', regex=True)
    tolls_df['LPN/Tag number'] = tolls_df['LPN/Tag number'].astype(str)

    # SQL queries remain the same
    query_tag = """
        SELECT DISTINCT * 
        FROM tolls_df
        INNER JOIN rcm_df 
        ON CAST(tolls_df.[LPN/Tag number] as VARCHAR) = CAST(rcm_df.[Vehicle] as VARCHAR)
        WHERE tolls_df.[Start Date] BETWEEN rcm_df.[Pickup Date Time] AND rcm_df.[Dropoff Date Time]
    """
    print(f'MY OUTPUT TO CHECK RCM DATA_RCMMMM: {rcm_df[['Vehicle', 'Pickup Date Time', 'Dropoff Date Time']].head(5)}')
    print(f'MY OUTPUT TO CHECK TOOOOOLLLLL DATA: {tolls_df[['LPN/Tag number', 'Start Date']].head(5)}')
  

    result_tag = ps.sqldf(query_tag, locals())

    query_rego = """
        SELECT DISTINCT * 
        FROM tolls_df
        INNER JOIN rcm_df 
        ON tolls_df.Rego= rcm_df.RCM_Rego
        WHERE tolls_df.[Start Date] BETWEEN rcm_df.[Pickup Date Time] AND rcm_df.[Dropoff Date Time]
    """
    result_rego = ps.sqldf(query_rego, locals())
    print(f'result tag I AM RESULT TAG: {result_tag.head(5)}') 
    print(f'result Rego_____: {result_rego.head(5)}') 
    if result_rego.empty:
        result_df=result_tag
    else:
        result_df = pd.concat([result_tag, result_rego], ignore_index=True)
    print(f'result df_____ HERE: {result_df.head(5)}')
    result_df.drop_duplicates(inplace=True)

    if result_df.empty:
        print("Debug: Resultant DataFrame is empty")
        return {'error': 'Processed data is empty'}, 400
    with app.app_context(): 
        try:
            engine = db.engine
            with engine.connect() as conn:
                print(f'STARTED DOING CREATE OR UPDATE TABLE')
                populate_rawdata_from_df(result_df)
                print(f'FINISHED DOING CREATE OR UPDATE TABLE')
                #result_df.to_sql('rawdata', conn, if_exists='append', index=False, method='multi')
                print(f'STARTED DOING Populate summary')
                cleaner()
                summary, grand_total, admin_fee_total = populate_summary_table()
                update_or_insert_summary(summary)
                update_existing_res_values()
                delete_null_trip_cost_records()
                print(f'FINISHED DOING Populate summary table')
                summary_cleaner()
                
        except Exception as e:
            print(f"Debug: Exception in database operations - {e}")
            return {'error': 'Database operation failed', 'details': str(e)}, 500

        return {'message': 'Upload and processing successful'}, 200


@app.route('/confirm-upload', methods=['POST'])
@login_required
def confirm_upload():
    job_id = request.args.get('job_id')
    if not job_id:
        return jsonify({'error': 'No job ID provided'}), 400

    job = q.fetch_job(job_id)
    if job is None:
        return jsonify({'error': 'Job not found or expired'}), 404

    try:
        if job.is_finished:
            if job.result and 'error' in job.result:
                return jsonify({'status': 'failed', 'message': job.result}), 500
            return jsonify({'status': 'completed', 'result': job.result}), 200
        elif job.is_failed:
            return jsonify({'status': 'failed', 'message': str(job.exc_info)}), 500
        else:
            return jsonify({'status': 'in progress'}), 202
    except Exception as e:
        return jsonify({'error': 'Failed to retrieve job status', 'details': str(e)}), 500

@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    job = q.fetch_job(job_id)
    if not job:
        return jsonify({'status': 'not found'}), 404
    elif job.is_finished:
        return jsonify({'status': 'finished', 'result': job.result}), 200
    elif job.is_failed:
        return jsonify({'status': 'failed', 'message': str(job.exc_info)}), 500
    else:
        return jsonify({'status': 'in progress'}), 202


def update_or_insert_summary(summary):
    try:
        engine = db.engine
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                for index, row in summary.iterrows():
                    admin_fee = float(row['admin_fee'].replace('$', '').replace(',', ''))

                    # Fetch the corresponding date times from rawData table
                    date_time_query = """
                    SELECT rd.pickup_date_time, rd.dropoff_date_time
                    FROM rawData rd
                    WHERE rd.res = :res
                    LIMIT 1
                    """
                    # Make sure to cast res to string if necessary or ensure it matches the expected data type
                    date_time_data = conn.execute(text(date_time_query), {'res': str(row['contract_number'])}).fetchone()
                    if date_time_data:
                        pickup_date_time = date_time_data[0]
                        dropoff_date_time = date_time_data[1]

                    params = {
                        'contract_number': int(row['contract_number']),  # Assuming this is already an integer
                        'num_of_rows': int(row['num_of_rows']),
                        'sum_of_toll_cost': float(row['sum_of_toll_cost'].replace('$', '').replace(',', '')),
                        'total_toll_contract_cost': float(row['total_toll_contract_cost'].replace('$', '').replace(',', '')),
                        'pickup_date_time': pickup_date_time,
                        'dropoff_date_time': dropoff_date_time,
                        'admin_fee': admin_fee
                    }

                    existing = conn.execute(text("SELECT 1 FROM summary WHERE contract_number = :contract_number"), {'contract_number': params['contract_number']}).scalar()
                    if row['contract_number']==7791:
                        print(f'WE ARE IN THE UPDATE FUNCTION AND FOR CONTRACT 7791: the pickupdate date time is {pickup_date_time}')
                    if existing:
                        conn.execute(text("""
                            UPDATE summary SET
                            num_of_rows = :num_of_rows,
                            sum_of_toll_cost = :sum_of_toll_cost,
                            total_toll_contract_cost = :total_toll_contract_cost,
                            pickup_date_time = :pickup_date_time,
                            dropoff_date_time = :dropoff_date_time,
                            admin_fee = :admin_fee
                            WHERE contract_number = :contract_number
                        """), params)
                    else:
                        conn.execute(text("""
                            INSERT INTO summary (contract_number, num_of_rows, sum_of_toll_cost, 
                                                 total_toll_contract_cost, pickup_date_time, dropoff_date_time, admin_fee)
                            VALUES (:contract_number, :num_of_rows, :sum_of_toll_cost, :total_toll_contract_cost, 
                                    :pickup_date_time, :dropoff_date_time, :admin_fee)
                        """), params)
                transaction.commit()
            except Exception as e:
                print("Transaction failed:", e)
                transaction.rollback()
                raise
    except Exception as e:
        print("Failed to update or insert summary:", e)
        raise


#subsidiary functions for cleaning 
def update_existing_res_values():
    with db.engine.connect() as connection:
        result = connection.execute(text("""
        UPDATE rawdata
        SET res = TRIM(TRAILING '.0' FROM res)
        WHERE res LIKE '%.0'
        """))
        print(f"Updated {result.rowcount} rows.")
def delete_null_trip_cost_records():
    try:
        # Begin a transaction
        db.session.begin()

        # Query to find all records where 'trip_cost' is NULL
        records_to_delete = RawData.query.filter(RawData.trip_cost.is_(None))

        # Delete these records
        records_to_delete.delete(synchronize_session=False)

        # Commit changes to the database
        db.session.commit()
        print("Deleted all records with null 'trip_cost'.")
    except Exception as e:
        db.session.rollback()  # Roll back on error
        print(f"Failed to delete records: {e}")

def fetch_summary_data():
    # Create a session object
    session = Session(bind=db.engine)
    try:
        # Use ORM style query to fetch data
        result = session.execute(select(Summary).order_by(Summary.contract_number.desc()))
        # Extract models directly from result using scalars().all()
        summary_data = result.scalars().all()
        # Convert each ORM model to dictionary if needed
        summary_dicts = [{column.name: getattr(summary, column.name) for column in Summary.__table__.columns} for summary in summary_data]
        app.logger.debug(f"Fetched summary data: {summary_dicts}")
        return summary_dicts
    except Exception as e:
        app.logger.error(f"Error fetching summary data: {e}")
        return None
    finally:
        session.close()

@app.route('/summary')
@login_required
def summary():
    summary_data = fetch_summary_data()
    if not summary_data:
        app.logger.warning("No summary data found or error occurred.")
        return render_template('summary.html', error="No data available.")

    try:
        # Safely calculate totals, ensuring all values are available and properly formatted
        total_admin_fee = sum(
            float(row.get('admin_fee', 0) if isinstance(row.get('admin_fee'), float) else float(row.get('admin_fee', '0').strip('$').replace(',', '')))
            for row in summary_data
        )
        total_sum_of_toll_cost = sum(
            float(row.get('sum_of_toll_cost', 0) if isinstance(row.get('sum_of_toll_cost'), float) else float(row.get('sum_of_toll_cost', '0').strip('$').replace(',', '')))
            for row in summary_data
        )
        total_contract_toll_cost = sum(
            float(row.get('total_toll_contract_cost', 0) if isinstance(row.get('total_toll_contract_cost'), float) else float(row.get('total_toll_contract_cost', '0').strip('$').replace(',', '')))
            for row in summary_data
        )
    except Exception as e:
        app.logger.error(f"Error calculating totals: {e}")
        return render_template('summary.html', error="Error calculating totals.")

    return render_template('summary.html', summary=summary_data, total_admin_fee=total_admin_fee,
                           total_sum_of_toll_cost=total_sum_of_toll_cost,
                           total_contract_toll_cost=total_contract_toll_cost)

# Define a custom filter
@app.template_filter('compact_number')
def compact_number_format(value):
    try:
        value = float(value)
        if value < 1000:
            return f"{value:.2f}"
        elif value < 1000000:
            return f"{value/1000:.2f}K"
        else:
            return f"{value/1000000:.2f}M"
    except (ValueError, TypeError):
        return value

app.jinja_env.filters['compact_number'] = compact_number_format

def get_last_5_contracts():
    # Assuming 'Summary' is your ORM model and 'contract_number' is a column in it
    session = Session(bind=db.engine)
    try:
        # Using the ORM approach to execute a query
        result = session.execute(
            select(Summary.contract_number)
            .distinct()
            .order_by(Summary.contract_number.desc())
            .limit(5)
        )
        # Fetch the results as a list of tuples and extract the contract numbers
        last_5_contracts = [row for row in result.scalars().all()]
        return last_5_contracts
    except Exception as e:
        app.logger.error(f"Error fetching the last 5 contracts: {e}")
        return []
    finally:
        session.close()

@app.route('/search', methods=['POST', 'GET'])
@login_required
def search():
    last_5_contracts = get_last_5_contracts()
    search_query = request.form.get('search_query') if request.method == 'POST' else request.args.get('search_query', None)

    if search_query:
        session = Session(bind=db.engine)
        try:
            # Fetch summary record for the contract using ORM approach
            summary_result = session.execute(
                select(Summary).where(Summary.contract_number == search_query)
            )
            summary_record = [{column.name: getattr(row, column.name) for column in Summary.__table__.columns} for row in summary_result.scalars().all()]

            # Fetch raw records for the contract using literal text for proper SQL execution
             # Fetch raw records for the contract using ORM
            #we need to do str conversion as res are stored in different ways in RawData and Summary (in summary it is int and in RawData it is str)
            search_query = str(search_query)
            raw_records = RawData.query.filter(RawData.res == search_query).order_by(RawData.start_date).all()
            print(f'Printing search Query: {search_query}')
            print(f'Printing search records: {raw_records}')
            raw_records_dicts = [
                {
                    'Toll Date/Time': record.start_date,
                    'Details': record.details,
                    'Tag Number': record.lpn_tag_number,
                    'Vehicle Class': record.vehicle_class,
                    'Trip Cost': f"${record.trip_cost}",
                    'Rego': record.rego
                }
                for record in raw_records
            ]
            print(f'RAW RECORDS FOR SUMMARY{raw_records_dicts}')
        finally:
            session.close()

        # Pass the converted records to your template
        return render_template('search_results.html', summary_record=summary_record, raw_records=raw_records_dicts, search_query=search_query, last_5_contracts=last_5_contracts)

    else:
        # Initial page load, no search performed
        return render_template('search_results.html', last_5_contracts=last_5_contracts, search_query=search_query)

@app.route('/dashboard', methods=['GET'])
@login_required
def dashboard():
    # Only render the page initially with default dates
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    return render_template('dashboard.html', start_date=start_date, end_date=end_date)

@app.route('/dashboard/tolls_data', methods=['GET'])
@login_required
def dashboard_data():
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    tolls_chart_json = fetch_tolls_data(start_date, end_date)
    #return tolls_chart_json  # Ensure this returns JSON formatted for Plotly
    return tolls_chart_json

@app.route('/dashboard/admin_fees_data', methods=['GET'])
@login_required
def dashboard_data_admin():
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    admin_chart_json = fetch_admin_fees_data(start_date, end_date)
    #return tolls_chart_json  # Ensure this returns JSON formatted for Plotly
    return admin_chart_json

@app.route('/dashboard/toll_fees_actual_data', methods=['GET'])
@login_required
def dashboard_data_toll_fees_actual():
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    toll_chart_json = fetch_toll_actual_fees_data(start_date, end_date)
    return toll_chart_json

@app.route('/dashboard/toll_sum_data', methods=['GET'])
@login_required
def dashboard_data_sum_toll_fees():
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    toll_sum_chart_json = fetch_sum_toll_fees_data(start_date, end_date)
    return toll_sum_chart_json
 
def fetch_tolls_data(start_date, end_date):
    session = Session(bind=db.engine)
    try:
        sql = """
        SELECT
            EXTRACT(MONTH FROM CAST(start_date AS DATE)) AS month,
            EXTRACT(YEAR FROM CAST(start_date AS DATE)) AS year,
            COUNT(DISTINCT (start_date, res, details, lpn_tag_number, end_date, trip_cost)) AS unique_toll_count
        FROM
            rawdata
        WHERE
            CAST(start_date AS DATE) BETWEEN :start_date AND :end_date
        GROUP BY
            EXTRACT(MONTH FROM CAST(start_date AS DATE)),
            EXTRACT(YEAR FROM CAST(start_date AS DATE))
        ORDER BY
            year, month;
        """
        result = session.execute(text(sql), {'start_date': start_date, 'end_date': end_date}).fetchall()
        # Using _asdict() if available or converting manually
        result_dicts = [row._asdict() if hasattr(row, '_asdict') else dict(row.items()) for row in result]
        months = [f"{row['year']}-{int(row['month']):02d}" for row in result_dicts]
        counts = [row['unique_toll_count'] for row in result_dicts]

        fig = go.Figure(data=[go.Bar(
            x=months,
            y=counts,
            marker_color='#007bff',  # Bootstrap "primary" blue
            text=counts,
            textposition='auto'
        )])
        fig.update_layout(
            title='Toll Usage Count',
            xaxis_title='Month',
            yaxis_title='Toll Usage Count',
            plot_bgcolor='white'
        )
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
    finally:
        session.close()

def format_currency(value):
    if value >= 1000000:  # Value is in millions or more
        return f"${int(value / 1000000)}M"
    elif value >= 1000:  # Value is in thousands or more
        return f"${int(value / 1000)}K"
    else:
        return f"${int(value)}"
    
def fetch_admin_fees_data(start_date, end_date):
    session = Session(bind=db.engine)
    try:
        sql = """
        SELECT
            EXTRACT(MONTH FROM CAST(dropoff_date_time AS DATE)) AS month,
            EXTRACT(YEAR FROM CAST(dropoff_date_time AS DATE)) AS year,
            SUM(admin_fee) AS total_admin_fee
        FROM
            summary
        WHERE
            CAST(dropoff_date_time AS DATE) BETWEEN :start_date AND :end_date
        GROUP BY
            EXTRACT(MONTH FROM CAST(dropoff_date_time AS DATE)),
            EXTRACT(YEAR FROM CAST(dropoff_date_time AS DATE))
        ORDER BY
            year, month;
        """
        result = session.execute(text(sql), {'start_date': start_date, 'end_date': end_date}).fetchall()
        months = [f"{row.year}-{int(row.month):02d}" for row in result]
        fees = [row.total_admin_fee for row in result]
        formatted_fees = [format_currency(fee) for fee in fees]  # Format for display
        
        fig = go.Figure(data=[go.Bar(
            x=months,
            y=fees,
            marker_color='#F72585',
            text=formatted_fees,
            textposition='auto'
        )])
        fig.update_layout(
            title='Admin Fee',
            xaxis_title='Month',
            yaxis_title='Admin Fee Total ($)',
            plot_bgcolor='white', 
            yaxis_tickprefix = '$', 
            yaxis_tickformat = ',.'
        )
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    finally:
        session.close()

#sum_of_toll_cost
def fetch_toll_actual_fees_data(start_date, end_date):
    session = Session(bind=db.engine)
    try:
        sql = """
        SELECT
            EXTRACT(MONTH FROM CAST(dropoff_date_time AS DATE)) AS month,
            EXTRACT(YEAR FROM CAST(dropoff_date_time AS DATE)) AS year,
            SUM(sum_of_toll_cost) AS total_toll_fee
        FROM
            summary
        WHERE
            CAST(dropoff_date_time AS DATE) BETWEEN :start_date AND :end_date
        GROUP BY
            EXTRACT(MONTH FROM CAST(dropoff_date_time AS DATE)),
            EXTRACT(YEAR FROM CAST(dropoff_date_time AS DATE))
        ORDER BY
            year, month;
        """
        result = session.execute(text(sql), {'start_date': start_date, 'end_date': end_date}).fetchall()
        months = [f"{row.year}-{int(row.month):02d}" for row in result]
        fees = [row.total_toll_fee for row in result]
        formatted_fees = [format_currency(fee) for fee in fees]  # Apply formatting
        fig = go.Figure(data=[go.Bar(
            x=months,
            y=fees,
            marker_color='#7209B7',
            text=formatted_fees,
            textposition='auto'
        )])
        fig.update_layout(
            title='Actual Toll Fee',
            xaxis_title='Month',
            yaxis_title='Toll Actual Fee Total',
            plot_bgcolor='white',
            yaxis_tickprefix = '$', 
            yaxis_tickformat = ',.'
        )
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    finally:
        session.close()

#sum_of_toll_cost
def fetch_sum_toll_fees_data(start_date, end_date):
    session = Session(bind=db.engine)
    try:
        sql = """
        SELECT
            EXTRACT(MONTH FROM CAST(dropoff_date_time AS DATE)) AS month,
            EXTRACT(YEAR FROM CAST(dropoff_date_time AS DATE)) AS year,
            SUM(total_toll_contract_cost) AS total_toll_fee
        FROM
            summary
        WHERE
            CAST(dropoff_date_time AS DATE) BETWEEN :start_date AND :end_date
        GROUP BY
            EXTRACT(MONTH FROM CAST(dropoff_date_time AS DATE)),
            EXTRACT(YEAR FROM CAST(dropoff_date_time AS DATE))
        ORDER BY
            year, month;
        """
        result = session.execute(text(sql), {'start_date': start_date, 'end_date': end_date}).fetchall()
        months = [f"{row.year}-{int(row.month):02d}" for row in result]
        fees = [row.total_toll_fee for row in result]
        formatted_fees = [format_currency(fee) for fee in fees]  # Apply formatting
        fig = go.Figure(data=[go.Bar(
            x=months,
            y=fees,
            marker_color='#4361EE',
            text=formatted_fees,
            textposition='auto'
        )])
        fig.update_layout(
            title='Total Toll Contract Cost (Admin fee + Toll fee)',
            xaxis_title='Month',
            yaxis_title='Total Toll Contract Cost',
            plot_bgcolor='white',
            yaxis_tickprefix = '$', 
            yaxis_tickformat = ',.'
        )
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    finally:
        session.close()



if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
