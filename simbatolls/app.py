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


#from flask import current_app as app


app = Flask(__name__, template_folder='templates')

app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)

@app.route('/')
def home():
    return render_template('home.html')

import os
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

class RawData(db.Model):
    __tablename__ = 'rawdata'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    start_date = db.Column(db.DateTime, nullable=True)  # Assuming dates can be null
    details = db.Column(db.String(255), nullable=True)
    lpn_tag_number = db.Column(db.String(100), nullable=True)
    vehicle_class = db.Column(db.String(50), nullable=True)
    trip_cost = db.Column(db.Float, nullable=True)
    fleet_id = db.Column(db.String(50), nullable=True)
    end_date = db.Column(db.DateTime, nullable=True)
    date = db.Column(db.Date, nullable=True)
    rego = db.Column(db.String(50), nullable=True)
    hash_ = db.Column(db.String(50), nullable=True)  # Using 'hash_' because '#' is not a valid variable name
    res = db.Column(db.String(50), nullable=True)
    ref = db.Column(db.String(50), nullable=True)
    update = db.Column(db.DateTime, nullable=True)  # Assuming 'Update' is a date-time column
    notes = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(50), nullable=True)
    dropoff = db.Column(db.String(50), nullable=True)
    day = db.Column(db.String(50), nullable=True)
    dropoff_date = db.Column(db.Date, nullable=True)
    time = db.Column(db.Time, nullable=True)
    pickup = db.Column(db.String(50), nullable=True)
    pickup_date = db.Column(db.Date, nullable=True)
    time_c13 = db.Column(db.Time, nullable=True)
    days = db.Column(db.Integer, nullable=True)  # Assuming '# Days' is an integer
    category = db.Column(db.String(50), nullable=True)
    vehicle = db.Column(db.String(50), nullable=True)
    colour = db.Column(db.String(50), nullable=True)
    items = db.Column(db.String(255), nullable=True)
    insurance = db.Column(db.String(50), nullable=True)
    departure = db.Column(db.DateTime, nullable=True)
    next_rental = db.Column(db.DateTime, nullable=True)
    pickup_date_time = db.Column(db.DateTime, nullable=True)
    dropoff_date_time = db.Column(db.DateTime, nullable=True)
    
    def __repr__(self):
        return f"User('{self.res}')" 
    
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
                                        else row['Vehicle'],
                                        axis=1
                                    )
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split(' ', n=1).str.get(1)
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split('.').str.get(0)
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.lstrip('0')
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
   
    # Convert the first 3 rows of each DataFrame to HTML
    rcm_html = rcm_df.head(3).to_html()
    tolls_html = tolls_df.head(3).to_html()


    job = q.enqueue(confirm_upload_task, rcm_df.to_json(), tolls_df.to_json())

    return jsonify({'rcmPreview': rcm_html, 'tollsPreview': tolls_html, 'message': 'Files are being processed', 'job_id': job.get_id()}), 202

#data base management 

#DATABASE = 'database.db'

#def get_db():
    #db = getattr(g, '_database', None)
    #if db is None:
        #db = g._database = sqlite3.connect(DATABASE)
    #return db

#@app.teardown_appcontext
#def close_connection(exception):
    #db = getattr(g, '_database', None)
    #if db is not None:
        #db.close()

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
    summary['admin_fee'] = '$' + summary['admin_fee'].astype(float).round(2).map('{:,.2f}'.format)
    summary['Sum of Toll Cost'] = '$' + summary['Sum_of_Toll_Cost'].astype(float).map('{:,.2f}'.format)
    summary['Total Toll Contract cost'] = '$' + summary['Total Toll Contract cost'].astype(float).map('{:,.2f}'.format)

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


def create_rawdata_table(result_df):
    from sqlalchemy import Table, Column, Integer, String, MetaData, Float
    metadata = MetaData()

    columns = [
        Column('id', Integer, primary_key=True)
    ]
    # Dynamically add columns based on DataFrame dtypes
    for col_name, dtype in result_df.dtypes.items():  # Changed from iteritems() to items()
        if dtype == 'int64':
            col_type = Integer()
        elif dtype == 'float64':
            col_type = Float()
        elif dtype == 'object':
            col_type = String()
        else:
            col_type = String()  # Default type
        columns.append(Column(col_name, col_type))
    
    # Create table dynamically
    rawdata_table = Table('rawdata', metadata, *columns, extend_existing=True)
    engine = db.engine
    
    rawdata_table.create(engine, checkfirst=True)

# Usage in your application would not change other than ensuring the DataFrame is passed
def confirm_upload_task(rcm_data_json, tolls_data_json):
    try: 
        rcm_df = pd.read_json(rcm_data_json)
        tolls_df = pd.read_json(tolls_data_json)
    
    except ValueError as e:
        print("Error parsing JSON data: We are in Confirm_upload_task", e)
        return {'error': 'Invalid JSON data', 'details': str(e)}, 500
        
    if rcm_df.empty or tolls_df.empty:
        print("Debug: DataFrames are empty")
        return {'error': 'DataFrames are empty'}, 400
    
    rcm_df['Vehicle'] = rcm_df['Vehicle'].astype(str)
    tolls_df['LPN/Tag number'] = tolls_df['LPN/Tag number'].astype(str)

    # SQL queries remain the same
    query_tag = """
        SELECT DISTINCT * 
        FROM tolls_df
        INNER JOIN rcm_df 
        ON tolls_df.[LPN/Tag number] = rcm_df.[Vehicle]
        WHERE tolls_df.[Start Date] BETWEEN rcm_df.[Pickup Date Time] AND rcm_df.[Dropoff Date Time]
    """
    result_tag = ps.sqldf(query_tag, locals())

    query_rego = """
        SELECT DISTINCT * 
        FROM tolls_df
        INNER JOIN rcm_df 
        ON tolls_df.Rego= rcm_df.RCM_Rego
        WHERE tolls_df.[Start Date] BETWEEN rcm_df.[Pickup Date Time] AND rcm_df.[Dropoff Date Time]
    """
    result_rego = ps.sqldf(query_rego, locals())
    
    result_df = pd.concat([result_tag, result_rego], ignore_index=True).drop_duplicates()
    result_df.drop_duplicates(inplace=True)

    if result_df.empty:
        print("Debug: Resultant DataFrame is empty")
        return {'error': 'Processed data is empty'}, 400

    try:
        engine = db.engine
        with engine.connect() as conn:
            create_rawdata_table(result_df)
            result_df.to_sql('rawdata', conn, if_exists='append', index=False, method='multi')
            summary, grand_total, admin_fee_total = populate_summary_table(result_df)
            update_or_insert_summary(summary)
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
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    if job.is_finished:
        if job.result and 'error' in job.result:
            return jsonify({'status': 'failed', 'message': job.result}), 500
        return jsonify({'status': 'completed', 'result': job.result}), 200
    elif job.is_failed:
        return jsonify({'status': 'failed', 'message': str(job.exc_info)}), 500
    else:
        return jsonify({'status': 'in progress'}), 202


@app.route('/job-status/<job_id>')
def job_status(job_id):
    job = q.fetch_job(job_id)
    if job.is_finished:
        return jsonify({'status': 'finished'}), 200
    elif job.is_failed:
        return jsonify({'status': 'failed', 'message': str(job.exc_info)}), 500
    else:
        return jsonify({'status': 'pending'}), 202


def update_or_insert_summary(summary):
    try:
        engine = db.engine
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                for index, row in summary.iterrows():
                    #print(f'row: {row}')
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
            raw_query = text("""
                SELECT distinct "Start Date" AS start_date, "Details" AS details,
                       "LPN/Tag number" AS lpn_tag_number, "Vehicle Class" AS vehicle_class,
                       "Trip Cost" AS trip_cost, "Rego" AS rego
                FROM rawdata
                WHERE "Res." = :res_value
            """)
            raw_result = session.execute(raw_query, {'res_value': search_query}).fetchall()

            # Extract the data into a list of dictionaries
            
            # Convert each RowProxy to a dictionary manually

            raw_records = []
            for row in raw_result:
                record = {
                    'Start Date': row[0],  # Assuming 'Start Date' is the first column
                    'Details': row[1],     # Assuming 'Details' is the second column
                    'LPN/Tag number': row[2],  # and so forth
                    'Vehicle Class': row[3],
                    'Trip Cost': f"${float(row[4]):,.2f}",  # Assuming 'Trip Cost' is the fifth column
                    'Rego': row[5]         # Assuming 'Rego' is the sixth column
                }
                raw_records.append(record)
            #print(raw_records)
        finally:
            session.close()

        # Pass the converted records to your template
        return render_template('search_results.html', summary_record=summary_record, raw_records=raw_records, search_query=search_query, last_5_contracts=last_5_contracts)

    else:
        # Initial page load, no search performed
        return render_template('search_results.html', last_5_contracts=last_5_contracts, search_query=search_query)


if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
