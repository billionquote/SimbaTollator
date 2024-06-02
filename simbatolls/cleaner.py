from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from io import StringIO

def cleaner():
    database_url = os.getenv('DATABASE_URL', 'your_hardcoded_database_url')
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    #database_url='postgresql://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr'
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        try:
            session.begin()
            count_before = session.execute(text("SELECT COUNT(*) FROM rawdata;")).scalar()
            #print("Number of records before deduplication:", count_before)
            #session.execute(text("DELETE FROM rawdata;"))
            #print('I HAVE NOW DELETED EVERYTHING')
            
            #session.execute(text("DELETE FROM summary;"))
            #print('I HAVE NOW DELETED EVERYTHING')
            # Ensuring start_date is treated as a timestamp
            session.execute(text("""
                CREATE TEMPORARY TABLE temp_rawdata AS 
                SELECT 
                    distinct 
                    "# Days",
                    start_date,
                    details,
                    lpn_tag_number,
                    vehicle_class,
                    trip_cost,
                    fleet_id,
                    end_date,
                    date,
                    rego,
                    res,
                    ref,
                    update,
                    notes,
                    status,
                    
                    day,
                    dropoff_date,
                    time,
                    pickup,
                    pickup_date,
                    time_c13,
                    category,
                    vehicle,
                    colour,
                    items,
                    insurance,
                    departure,
                    next_rental,
                    pickup_date_time,
                    dropoff_date_time,
                    rcm_rego
                FROM rawData
;
            """))

            session.execute(text("DELETE FROM rawdata;"))

            session.execute(text("""
                INSERT INTO rawdata
                SELECT
                    CAST(row_number() OVER () AS INTEGER) AS id, 
                    "# Days",
                    start_date,
                    details,
                    lpn_tag_number,
                    vehicle_class,
                    trip_cost,
                    fleet_id,
                    end_date,
                    date,
                    rego,
                    res,
                    ref,
                    update,
                    notes,
                    status,
                   
                    day,
                    dropoff_date,
                    time,
                    pickup,
                    pickup_date,
                    time_c13,
                    category,
                    vehicle,
                    colour,
                    items,
                    insurance,
                    departure,
                    next_rental,
                    pickup_date_time,
                    dropoff_date_time,
                    rcm_rego
                FROM temp_rawdata;
            """))
            count_after = session.execute(text("SELECT COUNT(*) FROM rawdata;")).scalar()
            print("Number of records after deduplication:", count_after)

            # Calculate and print the number of duplicates removed
            duplicates_removed = count_before - count_after
            print("Duplicates removed:", duplicates_removed)
            session.commit()
            print("Duplicates removed successfully.")
        except Exception as e:
            session.rollback()
            print(f"Failed to remove duplicates: {e}")
def summary_cleaner():
    database_url = os.getenv('DATABASE_URL', 'your_hardcoded_database_url')
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    #database_url='postgresql://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr'
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        try:
            session.begin()
            count_before = session.execute(text("SELECT COUNT(*) FROM summary;")).scalar()
            print("Number of records before deduplication:", count_before)

            # Create a temporary table with unique records
            session.execute(text("""
                CREATE TEMPORARY TABLE temp_summary AS 
                SELECT DISTINCT ON (dropoff_date_time, pickup_date_time, total_toll_contract_cost, admin_fee)
                    contract_number,
                    num_of_rows,
                    sum_of_toll_cost,
                    total_toll_contract_cost,
                    pickup_date_time,
                    dropoff_date_time,
                    admin_fee
                FROM summary
                ORDER BY dropoff_date_time, pickup_date_time, total_toll_contract_cost, admin_fee, contract_number;
            """))

            # Clear the original summary table
            session.execute(text("DELETE FROM summary;"))

            # Insert unique records back into the summary table
            session.execute(text("""
                INSERT INTO summary
                SELECT
                    contract_number,
                    num_of_rows,
                    sum_of_toll_cost,
                    total_toll_contract_cost,
                    pickup_date_time,
                    dropoff_date_time,
                    admin_fee
                FROM temp_summary;
            """))

            count_after = session.execute(text("SELECT COUNT(*) FROM summary;")).scalar()
            print("Number of records after deduplication:", count_after)

            # Calculate and print the number of duplicates removed
            duplicates_removed = count_before - count_after
            print("Duplicates removed:", duplicates_removed)

            session.commit()
            print("Duplicates removed successfully.")
        except Exception as e:
            session.rollback()
            print(f"Failed to remove duplicates: {e}")

if __name__ == '__main__':
    cleaner()
    summary_cleaner()