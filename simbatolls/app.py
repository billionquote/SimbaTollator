from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, g,session, json
from werkzeug.utils import secure_filename
import pandas as pd
import pandasql as ps
import openpyxl
import os
import time
import sqlite3
import tempfile
from flask import current_app as app
 
 
app = Flask(__name__, template_folder='templates')
 
app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
 
 
@app.route('/')
def home():
    return render_template('home.html')
 
#login fixes
from flask_login import login_user, LoginManager
from flask_bcrypt import Bcrypt
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from flask_login import login_required
 
#adding postgre
 
 
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///your_database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
 
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
# Initialize LoginManager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'validate'
 
class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
 
    def __repr__(self):
        return f"User('{self.username}')"
 
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
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split(' ', n=1).str.get(1)
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split('.').str.get(0)
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.lstrip('0')
    try:
        rcm_df['Vehicle'] = rcm_df['Vehicle'].astype(int)
    except ValueError:
        pass  # Handle the case where conversion to int is not possible
    rcm_df['Pickup Date Time'] = pd.to_datetime(rcm_df['Pickup Date'] + ' ' + rcm_df['Time_c13']).dt.strftime('%Y-%m-%d %H:%M:%S')
    rcm_df['Dropoff Date Time'] = pd.to_datetime(rcm_df['Dropoff Date'] + ' ' + rcm_df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    rcm_df.drop(['Customer', 'Mobile', 'Daily Rate', 'Rental Value', 'Balance'], inplace=True, axis=1)
    rcm_df = rcm_df.drop_duplicates()
   
    # Process Toll File
    tolls_df = pd.read_excel(tolls_file)
    tolls_df['Start Date'] = pd.to_datetime(tolls_df['Start Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['End Date'] = pd.to_datetime(tolls_df['End Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['Trip Cost'] = tolls_df['Trip Cost'].astype(str).str.replace(r'[^0-9.]', '', regex=True)
    try:
        tolls_df['LPN/Tag number'] = tolls_df['LPN/Tag number'].astype(int)
    except ValueError:
        pass  # Handle the case where conversion to int is not possible
    tolls_df = tolls_df.drop_duplicates()
    tolls_df['Trip Cost'] = tolls_df['Trip Cost'].astype(float, errors='ignore')
  
    # Convert the first 3 rows of each DataFrame to HTML
    rcm_html = rcm_df.head(3).to_html()
    tolls_html = tolls_df.head(3).to_html()
 
  
    # Create temporary files to save DataFrame JSON
    rcm_temp_file = tempfile.NamedTemporaryFile(delete=False)
    tolls_temp_file = tempfile.NamedTemporaryFile(delete=False)
 
    rcm_df.to_json(rcm_temp_file.name)
    tolls_df.to_json(tolls_temp_file.name)
 
    # Store the paths of temporary files in session
    session['rcm_df_path'] = rcm_temp_file.name
    session['tolls_df_path'] = tolls_temp_file.name
 
    # Ensure to close the files
    rcm_temp_file.close()
    tolls_temp_file.close()
 
    return jsonify({'rcmPreview': rcm_html, 'tollsPreview': tolls_html})
 
#data base management
 
DATABASE = 'database.db'
 
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db
 
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()
 
# Load DataFrames from session paths
def load_dataframes(rcm_df_path, tolls_df_path):
    rcm_df = pd.read_json(rcm_df_path)
    tolls_df = pd.read_json(tolls_df_path)
    return rcm_df, tolls_df
 
# Populate summary table
def populate_summary_table(df):
    # Ensure 'Res.' column is a string and remove any trailing ".0"
    df['Res.'] = df['Res.'].astype(str).str.replace(r'\.0$', '', regex=True)
    df['Pickup Date Time'] = pd.to_datetime(df['Pickup Date Time'])
    df['Dropoff Date Time'] = pd.to_datetime(df['Dropoff Date Time'])
    df = df[df['Res.'].notnull()]
    df=df.drop_duplicates()
    # Group by the 'Res.' column and perform aggregations
    summary = df.groupby('Res.').agg(
        Num_of_Rows=('Res.', 'size'),
        Sum_of_Toll_Cost=('Trip Cost', 'sum')
    ).reset_index()
 
    # Calculate grand total and admin fee total
    grand_total = summary['Sum_of_Toll_Cost'].sum()
    admin_fee_total = (summary['Num_of_Rows'] * 2.95).sum()
    summary['admin_fee'] = summary['Num_of_Rows'] * 2.95
    summary['Total Toll Contract cost'] = summary['admin_fee'] + summary['Sum_of_Toll_Cost']
   
    summary['Pickup Date Time'] = df['Pickup Date Time'].dt.strftime('%Y-%m-%d %H:%M')
    summary['Dropoff Date Time'] = df['Dropoff Date Time'].dt.strftime('%Y-%m-%d %H:%M')
   
    # Round 'Sum of Toll Cost' and 'Total Toll Contract cost' to 2 decimal points
    summary['Sum_of_Toll_Cost'] = summary['Sum_of_Toll_Cost'].round(2)
    summary['Total Toll Contract cost'] = summary['Total Toll Contract cost'].round(2)
 
    # Format 'Admin Fee', 'Sum_of_Toll_Cost', and 'Total Toll Contract cost' as currency
    summary['Admin Fee'] = '$' + summary['admin_fee'].astype(float).round(2).map('{:,.2f}'.format)
    summary['Sum of Toll Cost'] = '$' + summary['Sum_of_Toll_Cost'].astype(float).map('{:,.2f}'.format)
    summary['Total Toll Contract cost'] = '$' + summary['Total Toll Contract cost'].astype(float).map('{:,.2f}'.format)
 
    # Drop the 'admin_fee' column as it's now redundant
    #summary.drop(columns=['admin_fee'], inplace=True)
 
    # Rename the columns for clarity
    summary.rename(columns={'Res.': 'Contract Number', 'Num_of_Rows': 'Num of Rows'}, inplace=True)
        # Order by 'Contract Number'
    # Ensure that 'Contract Number' is treated as an integer for proper sorting
    summary['Contract Number'] = summary['Contract Number'].astype(int)
    summary = summary.sort_values(by='Contract Number', ascending=False)
   
    return summary, grand_total, admin_fee_total
 
def create_rawdata_table(result_df, conn):
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    #c.execute("DROP TABLE IF EXISTS rawdata")
    # Basic type mapping, extend this based on your actual data types
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'REAL',
        'object': 'TEXT'  # Assuming 'object' dtype in pandas is textual content
    }
   
    # Generate column definitions for SQL
    column_defs = ', '.join([f'"{col}" {type_mapping[str(result_df[col].dtype)]}' for col in result_df.columns])
    #print(f'i have created columns {column_defs}')
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS rawdata (
        id INTEGER PRIMARY KEY,
        {column_defs}
    );"""
   
    c.execute(create_table_sql)
    conn.commit()
    conn.close()
@app.route('/confirm-upload', methods=['POST'])
@login_required
def confirm_upload():
    rcm_df_path = session.get('rcm_df_path')
    tolls_df_path = session.get('tolls_df_path')
 
    if rcm_df_path is None or tolls_df_path is None:
        return jsonify({'error': 'Session expired or data not found'}), 400
 
    # Load DataFrames from stored paths
    rcm_df, tolls_df = load_dataframes(rcm_df_path, tolls_df_path)
 
    # SQL Query to join rcm_df and tolls_df
    query = """
        SELECT DISTINCT *
        FROM tolls_df
        INNER JOIN rcm_df
        ON tolls_df.[LPN/Tag number] = rcm_df.[Vehicle]
        WHERE tolls_df.[Start Date] BETWEEN rcm_df.[Pickup Date Time] AND rcm_df.[Dropoff Date Time]
    """
    result_df = ps.sqldf(query, locals())
    result_df.drop_duplicates(inplace=True)
 
    try:
        with sqlite3.connect(DATABASE) as conn:
            # Ensure the rawdata table exists and add new raw data
            create_rawdata_table(result_df, conn)
            result_df.to_sql('rawdata', conn, if_exists='append', index=False)
 
            # Update or insert summary data
            summary, grand_total, admin_fee_total = populate_summary_table(result_df)
            update_or_insert_summary(conn, summary)
    except Exception as e:
        return jsonify({'error': 'Database operation failed', 'details': str(e)}), 500
    finally:
        session.pop('rcm_df_path', None)
        session.pop('tolls_df_path', None)
 
    return redirect(url_for('summary'))
 
def update_or_insert_summary(conn, summary):
    cursor = conn.cursor()
    for index, row in summary.iterrows():
        # Check if record exists
        cursor.execute("SELECT * FROM summary WHERE \"Contract Number\" = ?", (row['Contract Number'],))
        existing = cursor.fetchone()
        if existing:
            # Update existing record
            cursor.execute("""
                UPDATE summary SET
                "Num of Rows" = ?,
                "Sum of Toll Cost" = ?,
                "Total Toll Contract cost" = ?,
                "Pickup Date Time" = ?,
                "Dropoff Date Time" = ?,
                "Admin Fee" = ?
                WHERE "Contract Number" = ?
            """, (row['Num of Rows'], row['Sum of Toll Cost'], row['Total Toll Contract cost'],
                  row['Pickup Date Time'], row['Dropoff Date Time'], row['Admin Fee'], row['Contract Number']))
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO summary ("Contract Number", "Num of Rows", "Sum of Toll Cost",
                                     "Total Toll Contract cost", "Pickup Date Time", "Dropoff Date Time", "Admin Fee")
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (row['Contract Number'], row['Num of Rows'], row['Sum of Toll Cost'],
                  row['Total Toll Contract cost'], row['Pickup Date Time'], row['Dropoff Date Time'], row['Admin Fee']))
    conn.commit()
 
 
 
 
def fetch_summary_data():
    try:
        con = sqlite3.connect(DATABASE)
        con.row_factory = sqlite3.Row  # This makes rows fetch as dictionaries
        cur = con.cursor()
 
        # Correct the ORDER BY clause by removing the single quotes around the column name
        cur.execute("SELECT * FROM summary ORDER BY \"Contract Number\" DESC")
        summary_data = [dict(row) for row in cur.fetchall()]
 
        con.close()
        return summary_data
    except Exception as e:
        print("Error fetching summary data:", e)
        return None
 
 
@app.route('/summary')
@login_required
def summary():
    # Fetch summary data from the database
    summary_data = fetch_summary_data()
 
    # Calculate totals
    total_admin_fee = sum(float(row['Admin Fee'].strip('$').replace(',', '')) if row['Admin Fee'] else 0 for row in summary_data)
    total_sum_of_toll_cost = sum(float(row['Sum of Toll Cost'].strip('$').replace(',', '')) if row['Sum of Toll Cost'] else 0 for row in summary_data)
    total_contract_toll_cost = sum(float(row['Total Toll Contract cost'].strip('$').replace(',', '')) if row['Total Toll Contract cost'] else 0 for row in summary_data)
 
    if summary_data:
        app.logger.info("Summary data fetched successfully: %s", summary_data)
    else:
        app.logger.warning("No summary data found.")
 
    # Render the summary template with the fetched data and totals
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
    try:
        con = sqlite3.connect(DATABASE)
        con.row_factory = sqlite3.Row  # Fetch rows as dictionaries
        cur = con.cursor()
 
        # Assuming 'Contract Number' is stored as a numerical value and you want the 5 largest values
        cur.execute("SELECT DISTINCT `Contract Number` FROM summary ORDER BY `Contract Number` DESC LIMIT 5")
        last_5_contracts = [row['Contract Number'] for row in cur.fetchall()]
 
        con.close()
        return last_5_contracts
    except Exception as e:
        print("Error fetching last 5 contracts:", e)
        return []
 
@app.route('/search', methods=['POST','GET'])
@login_required
def search():
    last_5_contracts = get_last_5_contracts()
    if request.method == 'POST':
        search_query = request.form.get('search_query')
    else:
        search_query = request.args.get('search_query', None)
    if search_query:
        # Connect to the database
        con = sqlite3.connect(DATABASE)
        con.row_factory = sqlite3.Row  # This ensures rows are fetched as dictionary-like objects
       
        # Fetch summary record for the contract
        summary_cursor = con.execute("SELECT * FROM summary WHERE `Contract Number` = ?", (search_query,))
        summary_record = [dict(row) for row in summary_cursor.fetchall()]
 
        # Fetch raw records for the contract with specific columns
        raw_query = """
        SELECT distinct "Start Date" as "Toll Date/Time", "Details", "LPN/Tag number", "Vehicle Class", "Trip Cost",
                "Rego"
        FROM rawdata
        WHERE "Res." = ?
        """
        raw_cursor = con.execute(raw_query, (search_query,))
        raw_records = [dict(row) for row in raw_cursor.fetchall()]
       
        # Formatting 'Trip Cost' to include a dollar sign
        for record in raw_records:
            # Ensure Trip Cost is a float before formatting, this is a safeguard.
            try:
                record['Trip Cost'] = f"${float(record['Trip Cost']):,.2f}"
            except ValueError:
                # In case 'Trip Cost' is not a valid float, keep it as is or handle appropriately.
                pass
       
        con.close()
 
        # Pass the converted records to your template
        return render_template('search_results.html', summary_record=summary_record, raw_records=raw_records, search_query=search_query, last_5_contracts=last_5_contracts)
    else:
        # Initial page load, no search performed
        return render_template('search_results.html', last_5_contracts=last_5_contracts, search_query=search_query)
 
 
if __name__ == '__main__':
    app.run(debug=True)from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, g,session, json
from werkzeug.utils import secure_filename
import pandas as pd
import pandasql as ps
import openpyxl
import os
import time
import sqlite3
import tempfile
#from flask import current_app as app


app = Flask(__name__, template_folder='templates')

app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}


@app.route('/')
def home():
    return render_template('home.html')

#login fixes 
from flask_login import login_user, LoginManager
from flask_bcrypt import Bcrypt
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from flask_login import login_required

#adding postgre
#from dotenv import load_dotenv
#load_dotenv() 

#updated 22 April
import os
# Get the DATABASE_URL, replace "postgres://" with "postgresql://"
database_url = os.getenv('DATABASE_URL')
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

from flask_migrate import Migrate

# Assuming 'db' is your SQLAlchemy database instance from 'app.db'
migrate = Migrate(app, db)

bcrypt = Bcrypt(app)
# Initialize LoginManager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'validate'

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
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split(' ', n=1).str.get(1)
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.split('.').str.get(0)
    rcm_df['Vehicle'] = rcm_df['Vehicle'].str.lstrip('0')
    try:
        rcm_df['Vehicle'] = rcm_df['Vehicle'].astype(int)
    except ValueError:
        pass  # Handle the case where conversion to int is not possible
    rcm_df['Pickup Date Time'] = pd.to_datetime(rcm_df['Pickup Date'] + ' ' + rcm_df['Time_c13']).dt.strftime('%Y-%m-%d %H:%M:%S')
    rcm_df['Dropoff Date Time'] = pd.to_datetime(rcm_df['Dropoff Date'] + ' ' + rcm_df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    rcm_df.drop(['Customer', 'Mobile', 'Daily Rate', 'Rental Value', 'Balance'], inplace=True, axis=1)
    rcm_df = rcm_df.drop_duplicates()
    
    # Process Toll File
    tolls_df = pd.read_excel(tolls_file)
    tolls_df['Start Date'] = pd.to_datetime(tolls_df['Start Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['End Date'] = pd.to_datetime(tolls_df['End Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tolls_df['Trip Cost'] = tolls_df['Trip Cost'].astype(str).str.replace(r'[^0-9.]', '', regex=True)
    try:
        tolls_df['LPN/Tag number'] = tolls_df['LPN/Tag number'].astype(int)
    except ValueError:
        pass  # Handle the case where conversion to int is not possible
    tolls_df = tolls_df.drop_duplicates()
    tolls_df['Trip Cost'] = tolls_df['Trip Cost'].astype(float, errors='ignore')
   
    # Convert the first 3 rows of each DataFrame to HTML
    rcm_html = rcm_df.head(3).to_html()
    tolls_html = tolls_df.head(3).to_html()

   
    # Create temporary files to save DataFrame JSON
    rcm_temp_file = tempfile.NamedTemporaryFile(delete=False)
    tolls_temp_file = tempfile.NamedTemporaryFile(delete=False)

    rcm_df.to_json(rcm_temp_file.name)
    tolls_df.to_json(tolls_temp_file.name)

    # Store the paths of temporary files in session
    session['rcm_df_path'] = rcm_temp_file.name
    session['tolls_df_path'] = tolls_temp_file.name

    # Ensure to close the files
    rcm_temp_file.close()
    tolls_temp_file.close()

    return jsonify({'rcmPreview': rcm_html, 'tollsPreview': tolls_html})

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

# Load DataFrames from session paths
def load_dataframes(rcm_df_path, tolls_df_path):
    rcm_df = pd.read_json(rcm_df_path)
    tolls_df = pd.read_json(tolls_df_path)
    return rcm_df, tolls_df

# Populate summary table
def populate_summary_table(df):
    # Ensure 'Res.' column is a string and remove any trailing ".0"
    df['Res.'] = df['Res.'].astype(str).str.replace(r'\.0$', '', regex=True)
    df['Pickup Date Time'] = pd.to_datetime(df['Pickup Date Time'])
    df['Dropoff Date Time'] = pd.to_datetime(df['Dropoff Date Time'])
    df = df[df['Res.'].notnull()]
    df=df.drop_duplicates()
    # Group by the 'Res.' column and perform aggregations
    summary = df.groupby('Res.').agg(
        Num_of_Rows=('Res.', 'size'),
        Sum_of_Toll_Cost=('Trip Cost', 'sum')
    ).reset_index()

    # Calculate grand total and admin fee total
    grand_total = summary['Sum_of_Toll_Cost'].sum()
    admin_fee_total = (summary['Num_of_Rows'] * 2.95).sum()
    summary['admin_fee'] = summary['Num_of_Rows'] * 2.95
    summary['Total Toll Contract cost'] = summary['admin_fee'] + summary['Sum_of_Toll_Cost']
    
    summary['Pickup Date Time'] = df['Pickup Date Time'].dt.strftime('%Y-%m-%d %H:%M')
    summary['Dropoff Date Time'] = df['Dropoff Date Time'].dt.strftime('%Y-%m-%d %H:%M')
    
    # Round 'Sum of Toll Cost' and 'Total Toll Contract cost' to 2 decimal points
    summary['Sum_of_Toll_Cost'] = summary['Sum_of_Toll_Cost'].round(2)
    summary['Total Toll Contract cost'] = summary['Total Toll Contract cost'].round(2)

    # Format 'Admin Fee', 'Sum_of_Toll_Cost', and 'Total Toll Contract cost' as currency
    summary['Admin Fee'] = '$' + summary['admin_fee'].astype(float).round(2).map('{:,.2f}'.format)
    summary['Sum of Toll Cost'] = '$' + summary['Sum_of_Toll_Cost'].astype(float).map('{:,.2f}'.format)
    summary['Total Toll Contract cost'] = '$' + summary['Total Toll Contract cost'].astype(float).map('{:,.2f}'.format)

    # Drop the 'admin_fee' column as it's now redundant
    #summary.drop(columns=['admin_fee'], inplace=True)

    # Rename the columns for clarity
    summary.rename(columns={'Res.': 'Contract Number', 'Num_of_Rows': 'Num of Rows'}, inplace=True)
        # Order by 'Contract Number'
    # Ensure that 'Contract Number' is treated as an integer for proper sorting
    summary['Contract Number'] = summary['Contract Number'].astype(int)
    summary = summary.sort_values(by='Contract Number', ascending=False)
    
    return summary, grand_total, admin_fee_total


#update 22
#from flask import current_app as app
def create_rawdata_table(result_df):
    # Obtain a connection from SQLAlchemy
    engine = app.db.engine  # Assuming db is the SQLAlchemy object
    connection = engine.connect()
    transaction = connection.begin()

    # Map pandas data types to PostgreSQL data types
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'REAL',
        'object': 'TEXT'  # Textual content in PostgreSQL
    }

    # Generate column definitions for SQL
    column_defs = ', '.join([f'"{col}" {type_mapping[str(result_df[col].dtype)]}' for col in result_df.columns])

    # SQL for creating table, ensuring it uses PostgreSQL syntax
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS rawdata (
        id SERIAL PRIMARY KEY,
        {column_defs}
    );"""

    # Execute the create table SQL
    try:
        connection.execute(create_table_sql)
        transaction.commit()  # Commit the transaction if successful
    except Exception as e:
        app.logger.error(f"Error creating table: {e}")
        transaction.rollback()  # Rollback the transaction on failure
    finally:
        connection.close()  # Always close the connection

# Usage in your application would not change other than ensuring the DataFrame is passed

@app.route('/confirm-upload', methods=['POST'])
@login_required
def confirm_upload():
    
    with app.app_context():
    
        rcm_df_path = session.get('rcm_df_path')
        tolls_df_path = session.get('tolls_df_path')

        if rcm_df_path is None or tolls_df_path is None:
            return jsonify({'error': 'Session expired or data not found'}), 400

        # Load DataFrames from stored paths
        rcm_df, tolls_df = load_dataframes(rcm_df_path, tolls_df_path)

        # Using pandasql to perform the join operation
        query = """
            SELECT DISTINCT * 
            FROM tolls_df
            INNER JOIN rcm_df 
            ON tolls_df.[LPN/Tag number] = rcm_df.[Vehicle]
            WHERE tolls_df.[Start Date] BETWEEN rcm_df.[Pickup Date Time] AND rcm_df.[Dropoff Date Time]
        """
        result_df = ps.sqldf(query, locals())
        result_df.drop_duplicates(inplace=True)

        try:
            # Use SQLAlchemy to handle database connection
            engine = db.engine
            with engine.connect() as conn:
                # Create table and insert data using SQLAlchemy methods
                create_rawdata_table(result_df)  # Updated for use with PostgreSQL
                result_df.to_sql('rawdata', conn, if_exists='append', index=False, method='multi')

                # Update or insert summary data
                summary, grand_total, admin_fee_total = populate_summary_table(result_df)
                update_or_insert_summary(summary)
        except Exception as e:
            return jsonify({'error': 'Database operation failed', 'details': str(e)}), 500
        finally:
            session.pop('rcm_df_path', None)
            session.pop('tolls_df_path', None)

        return redirect(url_for('summary'))

def update_or_insert_summary(summary):
    try:
        # Using SQLAlchemy connection and transaction
        engine = db.engine
        with engine.connect() as conn:
            transaction = conn.begin()
            for index, row in summary.iterrows():
                # Check if record exists
                result = conn.execute("SELECT * FROM summary WHERE \"Contract Number\" = %s", row['Contract Number'])
                existing = result.fetchone()
                if existing:
                    # Update existing record
                    conn.execute("""
                        UPDATE summary SET
                        "Num of Rows" = %s,
                        "Sum of Toll Cost" = %s,
                        "Total Toll Contract cost" = %s,
                        "Pickup Date Time" = %s,
                        "Dropoff Date Time" = %s,
                        "Admin Fee" = %s
                        WHERE "Contract Number" = %s
                    """, (row['Num of Rows'], row['Sum of Toll Cost'], row['Total Toll Contract cost'],
                          row['Pickup Date Time'], row['Dropoff Date Time'], row['Admin Fee'], row['Contract Number']))
                else:
                    # Insert new record
                    conn.execute("""
                        INSERT INTO summary ("Contract Number", "Num of Rows", "Sum of Toll Cost", 
                                             "Total Toll Contract cost", "Pickup Date Time", "Dropoff Date Time", "Admin Fee")
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (row['Contract Number'], row['Num of Rows'], row['Sum of Toll Cost'],
                          row['Total Toll Contract cost'], row['Pickup Date Time'], row['Dropoff Date Time'], row['Admin Fee']))
            transaction.commit()
    except Exception as e:
        transaction.rollback()
        app.logger.error(f"Failed to update or insert summary: {e}")
        raise

#from flask import current_app as app

def fetch_summary_data():
    try:
        # Use SQLAlchemy's session to execute SQL
        engine = app.db.engine  # Assuming db is the SQLAlchemy object
        with engine.connect() as connection:
            result = connection.execute("SELECT * FROM summary ORDER BY \"Contract Number\" DESC")
            summary_data = [dict(row) for row in result.fetchall()]
        return summary_data
    except Exception as e:
        app.logger.error(f"Error fetching summary data: {e}")
        return None



@app.route('/summary')
@login_required
def summary():
    # Fetch summary data from the database
    summary_data = fetch_summary_data()

    # Calculate totals
    total_admin_fee = sum(float(row['Admin Fee'].strip('$').replace(',', '')) if row['Admin Fee'] else 0 for row in summary_data)
    total_sum_of_toll_cost = sum(float(row['Sum of Toll Cost'].strip('$').replace(',', '')) if row['Sum of Toll Cost'] else 0 for row in summary_data)
    total_contract_toll_cost = sum(float(row['Total Toll Contract cost'].strip('$').replace(',', '')) if row['Total Toll Contract cost'] else 0 for row in summary_data)

    if summary_data:
        app.logger.info("Summary data fetched successfully: %s", summary_data)
    else:
        app.logger.warning("No summary data found.")

    # Render the summary template with the fetched data and totals
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
    engine = app.db.engine  # Ensure you have this setup to access the db engine
    with engine.connect() as connection:
        query = """
            SELECT DISTINCT "Contract Number" 
            FROM summary 
            ORDER BY "Contract Number" DESC 
            LIMIT 5
        """
        result = connection.execute(query)
        last_5_contracts = [row['Contract Number'] for row in result.fetchall()]
    return last_5_contracts


#from flask import current_app as app, request, render_template
from flask_login import login_required
from sqlalchemy import text

@app.route('/search', methods=['POST', 'GET'])
@login_required
def search():
    last_5_contracts = get_last_5_contracts()
    search_query = request.form.get('search_query') if request.method == 'POST' else request.args.get('search_query', None)
    
    if search_query:
        # Use SQLAlchemy to handle the database connection and querying
        engine = app.db.engine  # Make sure you have configured your db instance with SQLAlchemy
        with engine.connect() as connection:
            # Fetch summary record for the contract
            summary_result = connection.execute(
                text("SELECT * FROM summary WHERE \"Contract Number\" = :cn"),
                {'cn': search_query}
            )
            summary_record = [{column: value for column, value in row.items()} for row in summary_result]

            # Fetch raw records for the contract with specific columns
            raw_result = connection.execute(
                text("""
                    SELECT DISTINCT "Start Date" as "Toll Date/Time", "Details", "LPN/Tag number", "Vehicle Class", "Trip Cost", "Rego"
                    FROM rawdata
                    WHERE "Res." = :res
                """),
                {'res': search_query}
            )
            raw_records = [{column: value for column, value in row.items()} for row in raw_result]

            # Format 'Trip Cost' to include a dollar sign
            for record in raw_records:
                try:
                    record['Trip Cost'] = f"${float(record['Trip Cost']):,.2f}"
                except ValueError:
                    pass  # In case 'Trip Cost' is not a valid float, keep it as is or handle appropriately.

        # Pass the converted records to your template
        return render_template('search_results.html', summary_record=summary_record, raw_records=raw_records, search_query=search_query, last_5_contracts=last_5_contracts)

    else:
        # Initial page load, no search performed
        return render_template('search_results.html', last_5_contracts=last_5_contracts, search_query=search_query)



if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
