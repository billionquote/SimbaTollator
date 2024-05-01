from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from io import StringIO

def cleaner():
    #database_url = os.getenv('DATABASE_URL', 'your_hardcoded_database_url')
    #if database_url.startswith("postgres://"):
        #database_url = database_url.replace("postgres://", "postgresql://", 1)
    database_url='postgresql://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr'
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        try:
            session.begin()
            result = session.execute(text("SELECT distinct pg_typeof(start_date) FROM rawdata;"))
            for row in result:
                print("Data type of start_date in rawdata:", row[0])

            # Ensuring start_date is treated as a timestamp
            session.execute(text("""
                CREATE TEMPORARY TABLE temp_rawdata AS 
                SELECT 
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
                    "# Days",
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
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY 
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
                               "# Days",
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
                           ) AS row_num
                    FROM rawdata
                ) subquery
                WHERE row_num = 1;
            """))

            session.execute(text("DELETE FROM rawdata;"))

            session.execute(text("""
                INSERT INTO rawdata
                SELECT
                    CAST(row_number() OVER () AS INTEGER) AS id, 
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
                    "# Days",
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

            session.commit()
            print("Duplicates removed successfully.")
        except Exception as e:
            session.rollback()
            print(f"Failed to remove duplicates: {e}")

if __name__ == '__main__':
    cleaner()