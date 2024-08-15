#use code below for adding new users 
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_login import UserMixin

app = Flask(__name__)
# to get URI go to Heroku >> settings >> Config Var and update the below
# app.config['SQLALCHEMY_DATABASE_URI'] ='postgresql://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr'
# app.config['SQLALCHEMY_DATABASE_URI'] ='postgres://u8o7lasmharbq1:p671fb6b9ee7752b360f06d7b5cdc0c781427b938d1e3601862a2aeb6a3ea9b2f@cb4l59cdg4fg1k.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d99nb7lr00tna7'
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# app.secret_key = 'replace_with_a_secret_key'

# db = SQLAlchemy(app)
# bcrypt = Bcrypt(app)

# database_url ='postgres://u8o7lasmharbq1:p671fb6b9ee7752b360f06d7b5cdc0c781427b938d1e3601862a2aeb6a3ea9b2f@cb4l59cdg4fg1k.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d99nb7lr00tna7'

database_url ='postgres://uc0bhdfpdneiu3:pe6e0da6fb8b0bbed3d6f7a5a92746f179552c78a947cf3d3b7e1ca62b9d9da99@c11ai4tgvdcf54.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d6e265e9ds5t4e'

# database_url='postgresql://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr'
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)

class User(db.Model, UserMixin):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)

    def __repr__(self):
        return f"User('{self.username}')"

def add_user(username, password):
    with app.app_context():
        # This line ensures that all tables are created based on the models defined
        db.create_all()

        hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
        user = User(username=username, password_hash=hashed_password)
        db.session.add(user)
        db.session.commit()

# Example usage
# add_user (username, password)
# updatae the password below from ### to whatever 
add_user('bz@simbacarhire.com.au', 'bzsimba1368?')

def update_password(username, new_password):
    with app.app_context():
        user = User.query.filter_by(username=username).first()
        if user:
            user.password_hash = bcrypt.generate_password_hash(new_password).decode('utf-8')
            db.session.commit()
            return True
        else:
            return False
###update_password('bz@simbacarhire.com.au', '####')