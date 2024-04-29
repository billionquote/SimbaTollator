from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

def cleaner():
   # database_url = os.getenv('DATABASE_URL')
    database_url ='postgresql://jvkhatepulwmsq:4db6729008abc739d7bfdeefd19c6a6459e38f9b7dbd1b3bda2e95de5eb3d01c@ec2-54-83-138-228.compute-1.amazonaws.com:5432/d33ktsaohkqdr'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Start a transaction
        session.begin()

        # Create a temporary table with distinct records
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
                CAST("# Days" AS INTEGER) "# Days",  -- Corrected CAST syntax
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
                SELECT 
                    *,
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
                        CAST('# Days' AS INTEGER) '# Days', 
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
            ) AS subquery
            WHERE row_num = 1;
        """))

        # Delete all data from the original table
        session.execute(text("DELETE FROM rawdata;"))

        # Copy back unique records to the original table
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
                CAST("# Days" AS INTEGER) AS "# Days", 
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

        # Commit changes
        session.commit()
        print("Duplicates removed successfully.")
    except Exception as e:
        session.rollback()
        print(f"Failed to remove duplicates: {e}")
    finally:
        session.close()

if __name__ == '__main__':
    cleaner()
