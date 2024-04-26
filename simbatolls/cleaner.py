from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

def main():
    # Assume DATABASE_URL is set in your environment variables,
    # typically through Heroku's config vars
    database_url="postgres://ktbzjfczfdhzls:894a3004b174c857f5188cc7148b20e9a660ae6b9c70ce8071287bd7700689de@ec2-35-169-9-79.compute-1.amazonaws.com:5432/d2jinffuso3col"
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    # Set up the database connection
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Start a transaction
        session.begin()
        
        # Create a temporary table with distinct records
        session.execute(text("""
            CREATE TEMPORARY TABLE temp_rawdata AS 
            SELECT distinct 
                            "Start Date",
                            "Details",
                            "LPN/Tag number",
                            "Vehicle Class",
                            "Trip Cost",
                            "Fleet ID",
                            "End Date",
                            "Date",
                            "Rego",
                            "#",
                            "Res.",
                            "Ref.",
                            "Update",
                            "Notes",
                            "Status",
                            "Dropoff",
                            "Day",
                            "Dropoff Date",
                            "Time",
                            "Pickup",
                            "Pickup Date",
                            "Time_c13",
                            "# Days",
                            "Category",
                            "Vehicle",
                            "Colour",
                            "Items",
                            "Insurance",
                            "Departure",
                            "Next Rental",
                            "Pickup Date Time",
                            "Dropoff Date Time",
                    Count(*)
            FROM rawdata 

                    "Start Date",
                    "Details",
                    "LPN/Tag number",
                    "Vehicle Class",
                    "Trip Cost",
                    "Fleet ID",
                    "End Date",
                    "Date",
                    "Rego",
                    "#",
                    "Res.",
                    "Ref.",
                    "Update",
                    "Notes",
                    "Status",
                    "Dropoff",
                    "Day",
                    "Dropoff Date",
                    "Time",
                    "Pickup",
                    "Pickup Date",
                    "Time_c13",
                    "# Days",
                    "Category",
                    "Vehicle",
                    "Colour",
                    "Items",
                    "Insurance",
                    "Departure",
                    "Next Rental",
                    "Pickup Date Time",
                    "Dropoff Date Time"

                    Having count(*)>1;
                            """))
        
        # Delete all data from the original table
        session.execute(text("""
            DELETE FROM rawdata;
        """))
        
        # Copy back unique records to the original table
        session.execute(text("""
            INSERT INTO rawdata
            SELECT * FROM temp_rawdata;
        """))
        
        # Commit changes
        session.commit()
        print("Duplicates removed successfully.")
    except Exception as e:
        session.rollback()
        print(f"Failed to remove duplicates: {e}")
    finally:
        session.close()

if __name__ == '__main__':
    main()
