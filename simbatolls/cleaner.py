from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from io import StringIO

def cleaner():
    database_url =os.getenv('DATABASE_URL')

    # database_url ='postgres://uc0bhdfpdneiu3:p5e86cded25c6249ededeee783d0a3c4c77d689a3a1cc772ac7f415072685e2a9@cbib4a865d7s88.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/dbjuisj2f768p7'
    
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    #database_url='postgresql://u8o7lasmharbq1:p671fb6b9ee7752b360f06d7b5cdc0c781427b938d1e3601862a2aeb6a3ea9b2f@cb4l59cdg4fg1k.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d99nb7lr00tna7'
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        try:
            session.begin()
            count_before = session.execute(text("SELECT COUNT(*) FROM rawdata;")).scalar()
            print("Number of records before deduplication:", count_before)
            #session.execute(text("DELETE FROM rawdata;"))
            #print('I HAVE NOW DELETED EVERYTHING')
            
            #session.execute(text("DELETE FROM summary;"))
            #print('I HAVE NOW DELETED EVERYTHING')
            # Ensuring start_date is treated as a timestamp
            
            session.execute(text("""
                CREATE TEMPORARY TABLE temp_rawdata AS 
                SELECT 
                    distinct ON( "# Days",
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
                    dropoff,
                    day,
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
                    rcm_rego,
                    adminfeeamt)  "# Days",
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
                    dropoff,
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
                    rcm_rego,
                    adminfeeamt
                FROM rawData
;
            """))

            # Define the SQL query
            # query = text("""
            #     CREATE TEMPORARY TABLE temp_rawdata AS 
            #     SELECT DISTINCT ON (
            #         "# Days",
            #         start_date,
            #         details,
            #         CASE 
            #             WHEN lpn_tag_number LIKE '%\.0' THEN TRIM(TRAILING '.0' FROM lpn_tag_number)
            #             ELSE lpn_tag_number
            #         END,
            #         vehicle_class,
            #         trip_cost,
            #         fleet_id,
            #         end_date,
            #         date,
            #         rego,
            #         res,
            #         ref,
            #         update,
            #         notes,
            #         dropoff,
            #         day,
            #         pickup,
            #         pickup_date,
            #         time_c13,
            #         category,
            #         vehicle,
            #         colour,
            #         items,
            #         insurance,
            #         departure,
            #         next_rental,
            #         pickup_date_time,
            #         rcm_rego,
            #         adminfeeamt
            #     )
            #     "# Days",
            #     start_date,
            #     details,
            #     CASE 
            #         WHEN lpn_tag_number LIKE '%\.0' THEN TRIM(TRAILING '.0' FROM lpn_tag_number)
            #         ELSE lpn_tag_number
            #     END AS lpn_tag_number,
            #     vehicle_class,
            #     trip_cost,
            #     fleet_id,
            #     end_date,
            #     date,
            #     rego,
            #     res,
            #     ref,
            #     update,
            #     notes,
            #     status,
            #     dropoff,
            #     day,
            #     dropoff_date,
            #     time,
            #     pickup,
            #     pickup_date,
            #     time_c13,
            #     category,
            #     vehicle,
            #     colour,
            #     items,
            #     insurance,
            #     departure,
            #     next_rental,
            #     pickup_date_time,
            #     dropoff_date_time,
            #     rcm_rego,
            #     adminfeeamt
            #     FROM rawData;
            # """)

            # # Execute the query
            # session.execute(query)

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
                    dropoff,
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
                    rcm_rego,
                    adminfeeamt
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
    database_url =os.getenv('DATABASE_URL')
    
    # database_url ='postgres://uc0bhdfpdneiu3:p5e86cded25c6249ededeee783d0a3c4c77d689a3a1cc772ac7f415072685e2a9@cbib4a865d7s88.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/dbjuisj2f768p7'
    
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    #database_url='postgresql://u8o7lasmharbq1:p671fb6b9ee7752b360f06d7b5cdc0c781427b938d1e3601862a2aeb6a3ea9b2f@cb4l59cdg4fg1k.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d99nb7lr00tna7'
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
