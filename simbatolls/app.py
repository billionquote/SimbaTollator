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
from simbatolls.cleaner import cleaner
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
#database_url="postgres://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr"
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
cleaner()

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
    pickup_date_time = db.Column(db.DateTime)
    dropoff_date_time = db.Column(db.DateTime)
    admin_fee = db.Column(db.Float)

    def __repr__(self):
        return f"<Summary contract_number={self.contract_number} num_of_rows={self.num_of_rows}>"

class RawData(db.Model):
    __tablename__ = 'rawdata'

    id = db.Column(db.Integer, primary_key=True)
    start_date = db.Column(db.DateTime)
    details = db.Column(db.String)
    lpn_tag_number = db.Column(db.String)
    vehicle_class = db.Column(db.String)
    trip_cost = db.Column(db.String)
    fleet_id = db.Column(db.String)
    end_date = db.Column(db.DateTime)
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
    num_days = db.Column(db.Integer, name='# Days')
    category = db.Column(db.String)
    vehicle = db.Column(db.String)
    colour = db.Column(db.String)
    items = db.Column(db.String)
    insurance = db.Column(db.String)
    departure = db.Column(db.String)
    next_rental = db.Column(db.String)
    pickup_date_time = db.Column(db.DateTime)
    dropoff_date_time = db.Column(db.DateTime)
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
    
    # Process Toll File
    tolls_df = pd.read_excel(tolls_file)
    tolls_df['Start Date'] = pd.to_datetime(tolls_df['Start Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
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
    # Convert the first 3 rows of each DataFrame to HTML
    rcm_html = rcm_df.head(3).to_html()
    tolls_html = tolls_df.head(3).to_html()


    if rcm_df.empty or tolls_df.empty:
        return jsonify({'message': 'Uploaded files are empty'}), 400

    rcm_json = rcm_df.to_json()
    tolls_json = tolls_df.to_json()

    # Logging data to ensure it's correct before queuing
    print(f"RCM JSON: {rcm_json}")
    print(f"Tolls JSON: {tolls_json}")

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


def populate_summary_table(df):
    print("Original DataFrame:", df.head())  # Display initial data for debugging

    # Ensure 'Res.' column is a string and remove any trailing ".0"
    df['Res.'] = df['Res.'].astype(str).str.replace(r'\.0$', '', regex=True)
    df['Pickup Date Time'] = pd.to_datetime(df['Pickup Date Time'])
    df['Dropoff Date Time'] = pd.to_datetime(df['Dropoff Date Time'])
    df = df[df['Res.'].notnull()]
    df = df.drop_duplicates()
    df['Trip Cost'] = df['Trip Cost'].astype(float)
    # Debug output to see DataFrame before aggregation
    print("DataFrame before aggregation:", df.head())

    # Group by the 'Res.' column and perform aggregations
    summary = df.groupby('Res.').agg(
        Num_of_Rows=('Res.', 'size'),
        Sum_of_Toll_Cost=('Trip Cost', 'sum')
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
    summary['Pickup Date Time'] = df['Pickup Date Time'].dt.strftime('%Y-%m-%d %H:%M')
    summary['Dropoff Date Time'] = df['Dropoff Date Time'].dt.strftime('%Y-%m-%d %H:%M')
    
    summary['Sum_of_Toll_Cost'] = summary['Sum_of_Toll_Cost'].round(2)
    summary['Total Toll Contract cost'] = summary['Total Toll Contract cost'].round(2)
    summary['admin_fee'] = summary['admin_fee'].astype(float).round(2).map('{:,.2f}'.format)
    summary['Sum of Toll Cost'] = summary['Sum_of_Toll_Cost'].astype(float).map('{:,.2f}'.format)
    summary['Total Toll Contract cost'] =  summary['Total Toll Contract cost'].astype(float).map('{:,.2f}'.format)

    summary = summary.rename(columns={
        'Res.': 'Contract Number'
    })
    
    summary['Contract Number'] = summary['Contract Number'].astype(int)
    summary = summary.sort_values(by='Contract Number', ascending=False)

    # Final DataFrame to be returned
    print("Final DataFrame for SQL operations:", summary.head())
    summary=summary.rename(columns={
        'Res.': 'Contract Number'
    })
    
    summary['Contract Number'] = summary['Contract Number'].astype(int)
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

def populate_rawdata_from_df(result_df):
    result_df = convert_df_types(result_df)
    result_df['Res.'] = result_df['Res.'].astype(str).str.replace(r'\.0$', '', regex=True)
    try:
        for _, row in result_df.iterrows():
            existing_record = RawData.query.filter_by(
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
                colour=row['Colour'],
                items=row['Items'],
                insurance=row['Insurance'],
                departure=row['Departure'],
                next_rental=row['Next Rental'],
                pickup_date_time=row['Pickup Date Time'],
                dropoff_date_time=row['Dropoff Date Time'],
                rcm_rego=row['RCM_Rego']
            ).first()

            if existing_record:
                # Update fields that may change
                #existing_record.trip_cost = row['Trip Cost']
                # Add other fields if there are more that can change
                pass
            else:
                # Create a new record if it does not exist
                new_record = RawData(
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
                colour=row['Colour'],
                items=row['Items'],
                insurance=row['Insurance'],
                departure=row['Departure'],
                next_rental=row['Next Rental'],
                pickup_date_time=row['Pickup Date Time'],
                dropoff_date_time=row['Dropoff Date Time'],
                rcm_rego=row['RCM_Rego']
                )
                db.session.add(new_record)

        db.session.commit()  # Commit once all records processed
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
                summary, grand_total, admin_fee_total = populate_summary_table(result_df)
                update_or_insert_summary(summary)
                print(f'FINISHED DOING Populate summary table')
                update_existing_res_values()
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
                    print(f'update_or_insert_summary is working: row: {row}')
                    #print(f'row: {row['admin_fee']}')
                    admin_fee = float(row['admin_fee'].replace('$', '').replace(',', ''))
                    pickup_date_time = row['pickup_date_time'].to_pydatetime() if isinstance(row['pickup_date_time'], pd.Timestamp) else row['pickup_date_time']
                    dropoff_date_time = row['dropoff_date_time'].to_pydatetime() if isinstance(row['dropoff_date_time'], pd.Timestamp) else row['dropoff_date_time']
                    
                    params = {
                        'contract_number': int(row['contract_number']),
                        'num_of_rows': int(row['num_of_rows']),
                        'sum_of_toll_cost': float(row['sum_of_toll_cost'].replace('$', '').replace(',', '')),
                        'total_toll_contract_cost': float(row['total_toll_contract_cost'].replace('$', '').replace(',', '')),
                        'pickup_date_time': pickup_date_time,
                        'dropoff_date_time': dropoff_date_time,
                        'admin_fee': admin_fee
                    }
                    
                    #print("SQL Params:", params)  # Debugging output

                    existing = conn.execute(text("SELECT 1 FROM summary WHERE contract_number = :contract_number"), {'contract_number': params['contract_number']}).scalar()
                    
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
        #traceback2.print_exc()  # This will print stack trace to debug the error
        raise



def update_existing_res_values():
    with db.engine.connect() as connection:
        result = connection.execute("""
        UPDATE rawdata
        SET res = TRIM(TRAILING '.0' FROM res)
        WHERE res LIKE '%.0'
        """)
        print(f"Updated {result.rowcount} rows.")

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
            raw_records = RawData.query.filter(RawData.res == search_query).all()
            print(f'Printing search Query: {search_query}')
            print(f'Printing search records: {raw_records}')
            raw_records_dicts = [
                {
                    'Toll Date/Time': record.start_date.strftime('%Y-%m-%d %H:%M:%S') if record.start_date else '',
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


if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
