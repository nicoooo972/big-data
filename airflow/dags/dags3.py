from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sqlalchemy
import polars as pl
import os
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connexions
SRC_CONN = 'postgresql://postgres:admin@data-warehouse:5432/taxi'
DST_CONN = 'postgresql://postgres:admin@data-mart:5432/taxi_olap'


def migrate_dim_vendor(**kwargs):
    logger.info("Starting dim_vendor migration.")
    query = 'SELECT DISTINCT vendorid as vendor_id FROM yellow_tripdata WHERE vendorid IS NOT NULL'
    try:
        logger.info(f"Executing query on SRC_CONN: {query}")
        df = pl.read_database_uri(query=query, uri=SRC_CONN)
        logger.info(f"Read {df.height} rows for new vendors.")
    except Exception as e:
        logger.error(f"Error reading from source for dim_vendor: {e}")
        raise

    # Add vendor names
    df = df.with_columns([
        pl.col('vendor_id').map_elements(lambda x: {
            1: 'Creative Mobile Technologies, LLC',
            2: 'Curb Mobility, LLC',
            6: 'Myle Technologies Inc',
            7: 'Helix'
        }.get(x, 'Unknown')).alias('vendor_name')
    ])
    
    existing_query = 'SELECT vendor_id FROM dim_vendor'
    try:
        logger.info(f"Executing query on DST_CONN: {existing_query}")
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('vendor_id').is_in(existing['vendor_id']))
        logger.info(f"Found {df.height} new vendors after filtering existing ones.")
    except Exception as e:
         logger.warning(f"Could not read existing dim_vendor or table is empty, proceeding with all new rows: {e}")
         pass # Assuming if table doesn't exist, all df rows are new

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            logger.info(f"Writing {df.height} new rows to dim_vendor.")
            df.write_database(table_name='dim_vendor', connection=engine, if_table_exists='append')
            with engine.connect() as conn:
                logger.info("Executing ANALYZE on dim_vendor.")
                conn.execute(sqlalchemy.text('ANALYZE dim_vendor'))
            logger.info("Successfully wrote to dim_vendor and analyzed.")
        except Exception as e:
            logger.error(f"Error writing to dim_vendor: {e}")
            raise
        finally:
            engine.dispose()
            logger.info("dim_vendor engine disposed.")
    else:
        logger.info("No new vendor data to write.")
    logger.info("Finished dim_vendor migration.")


def migrate_dim_rate_code(**kwargs):
    logger.info("Starting dim_rate_code migration.")
    query = 'SELECT DISTINCT ratecodeid as rate_code_id FROM yellow_tripdata WHERE ratecodeid IS NOT NULL'
    try:
        logger.info(f"Executing query on SRC_CONN: {query}")
        df = pl.read_database_uri(query=query, uri=SRC_CONN)
        logger.info(f"Read {df.height} rows for new rate codes.")
    except Exception as e:
        logger.error(f"Error reading from source for dim_rate_code: {e}")
        raise
    
    # Add rate code names
    df = df.with_columns([
        pl.col('rate_code_id').map_elements(lambda x: {
            1: 'Standard rate',
            2: 'JFK',
            3: 'Newark',
            4: 'Nassau or Westchester',
            5: 'Negotiated fare',
            6: 'Group ride',
            99: 'Null/unknown'
        }.get(x, 'Other')).alias('rate_code_name')
    ])
    
    existing_query = 'SELECT rate_code_id FROM dim_rate_code'
    try:
        logger.info(f"Executing query on DST_CONN: {existing_query}")
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('rate_code_id').is_in(existing['rate_code_id']))
        logger.info(f"Found {df.height} new rate codes after filtering existing ones.")
    except Exception as e:
        logger.warning(f"Could not read existing dim_rate_code or table is empty, proceeding with all new rows: {e}")
        pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            logger.info(f"Writing {df.height} new rows to dim_rate_code.")
            df.write_database(table_name='dim_rate_code', connection=engine, if_table_exists='append')
            with engine.connect() as conn:
                logger.info("Executing ANALYZE on dim_rate_code.")
                conn.execute(sqlalchemy.text('ANALYZE dim_rate_code'))
            logger.info("Successfully wrote to dim_rate_code and analyzed.")
        except Exception as e:
            logger.error(f"Error writing to dim_rate_code: {e}")
            raise
        finally:
            engine.dispose()
            logger.info("dim_rate_code engine disposed.")
    else:
        logger.info("No new rate code data to write.")
    logger.info("Finished dim_rate_code migration.")


def migrate_dim_payment_type(**kwargs):
    logger.info("Starting dim_payment_type migration.")
    query = 'SELECT DISTINCT payment_type as payment_type_id FROM yellow_tripdata WHERE payment_type IS NOT NULL'
    try:
        logger.info(f"Executing query on SRC_CONN: {query}")
        df = pl.read_database_uri(query=query, uri=SRC_CONN)
        logger.info(f"Read {df.height} rows for new payment types.")
    except Exception as e:
        logger.error(f"Error reading from source for dim_payment_type: {e}")
        raise
    
    # Add payment type names
    df = df.with_columns([
        pl.col('payment_type_id').map_elements(lambda x: {
            0: 'Flex Fare trip',
            1: 'Credit card',
            2: 'Cash',
            3: 'No charge',
            4: 'Dispute',
            5: 'Unknown',
            6: 'Voided trip'
        }.get(x, 'Other')).alias('payment_type_name')
    ])
    
    existing_query = 'SELECT payment_type_id FROM dim_payment_type'
    try:
        logger.info(f"Executing query on DST_CONN: {existing_query}")
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('payment_type_id').is_in(existing['payment_type_id']))
        logger.info(f"Found {df.height} new payment types after filtering existing ones.")
    except Exception as e:
        logger.warning(f"Could not read existing dim_payment_type or table is empty, proceeding with all new rows: {e}")
        pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            logger.info(f"Writing {df.height} new rows to dim_payment_type.")
            df.write_database(table_name='dim_payment_type', connection=engine, if_table_exists='append')
            with engine.connect() as conn:
                logger.info("Executing ANALYZE on dim_payment_type.")
                conn.execute(sqlalchemy.text('ANALYZE dim_payment_type'))
            logger.info("Successfully wrote to dim_payment_type and analyzed.")
        except Exception as e:
            logger.error(f"Error writing to dim_payment_type: {e}")
            raise
        finally:
            engine.dispose()
            logger.info("dim_payment_type engine disposed.")
    else:
        logger.info("No new payment type data to write.")
    logger.info("Finished dim_payment_type migration.")


def migrate_dim_location(**kwargs):
    logger.info("Starting dim_location migration.")
    query1 = 'SELECT DISTINCT pulocationid as location_id FROM yellow_tripdata WHERE pulocationid IS NOT NULL'
    query2 = 'SELECT DISTINCT dolocationid as location_id FROM yellow_tripdata WHERE dolocationid IS NOT NULL'
    try:
        logger.info(f"Executing query1 on SRC_CONN: {query1}")
        df1 = pl.read_database_uri(query=query1, uri=SRC_CONN)
        logger.info(f"Read {df1.height} rows for pickup locations.")
        logger.info(f"Executing query2 on SRC_CONN: {query2}")
        df2 = pl.read_database_uri(query=query2, uri=SRC_CONN)
        logger.info(f"Read {df2.height} rows for dropoff locations.")
        df = pl.concat([df1, df2]).unique(subset=['location_id'])
        logger.info(f"Combined and found {df.height} unique locations.")
    except Exception as e:
        logger.error(f"Error reading from source for dim_location: {e}")
        raise
    
    # Add location details
    location_details = {
        1: ('EWR', 'Newark Airport', 'EWR'),
        2: ('Queens', 'Jamaica Bay', 'Boro Zone'),
        3: ('Bronx', 'Allerton/Pelham Gardens', 'Boro Zone'),
        4: ('Manhattan', 'Alphabet City', 'Yellow Zone'),
        5: ('Staten Island', 'Arden Heights', 'Boro Zone'),
        6: ('Staten Island', 'Arrochar/Fort Wadsworth', 'Boro Zone'),
        7: ('Queens', 'Astoria', 'Boro Zone'),
        8: ('Queens', 'Astoria Park', 'Boro Zone'),
        9: ('Queens', 'Auburndale', 'Boro Zone'),
        10: ('Queens', 'Baisley Park', 'Boro Zone'),
        11: ('Brooklyn', 'Bath Beach', 'Boro Zone'),
        12: ('Manhattan', 'Battery Park', 'Yellow Zone'),
        13: ('Manhattan', 'Battery Park City', 'Yellow Zone'),
        14: ('Brooklyn', 'Bay Ridge', 'Boro Zone'),
        15: ('Queens', 'Bay Terrace/Fort Totten', 'Boro Zone'),
        16: ('Queens', 'Bayside', 'Boro Zone'),
        17: ('Brooklyn', 'Bedford', 'Boro Zone'),
        18: ('Bronx', 'Bedford Park', 'Boro Zone'),
        19: ('Queens', 'Bellerose', 'Boro Zone'),
        20: ('Bronx', 'Belmont', 'Boro Zone'),
        21: ('Brooklyn', 'Bensonhurst East', 'Boro Zone'),
        22: ('Brooklyn', 'Bensonhurst West', 'Boro Zone'),
        23: ('Staten Island', 'Bloomfield/Emerson Hill', 'Boro Zone'),
        24: ('Manhattan', 'Bloomingdale', 'Yellow Zone'),
        25: ('Brooklyn', 'Boerum Hill', 'Boro Zone'),
        26: ('Brooklyn', 'Borough Park', 'Boro Zone'),
        27: ('Queens', 'Breezy Point/Fort Tilden/Riis Beach', 'Boro Zone'),
        28: ('Queens', 'Briarwood/Jamaica Hills', 'Boro Zone'),
        29: ('Brooklyn', 'Brighton Beach', 'Boro Zone'),
        30: ('Queens', 'Broad Channel', 'Boro Zone'),
        31: ('Bronx', 'Bronx Park', 'Boro Zone'),
        32: ('Bronx', 'Bronxdale', 'Boro Zone'),
        33: ('Brooklyn', 'Brooklyn Heights', 'Boro Zone'),
        34: ('Brooklyn', 'Brooklyn Navy Yard', 'Boro Zone'),
        35: ('Brooklyn', 'Brownsville', 'Boro Zone'),
        36: ('Brooklyn', 'Bushwick North', 'Boro Zone'),
        37: ('Brooklyn', 'Bushwick South', 'Boro Zone'),
        38: ('Queens', 'Cambria Heights', 'Boro Zone'),
        39: ('Brooklyn', 'Canarsie', 'Boro Zone'),
        40: ('Brooklyn', 'Carroll Gardens', 'Boro Zone'),
        41: ('Manhattan', 'Central Harlem', 'Boro Zone'),
        42: ('Manhattan', 'Central Harlem North', 'Boro Zone'),
        43: ('Manhattan', 'Central Park', 'Yellow Zone'),
        44: ('Staten Island', 'Charleston/Tottenville', 'Boro Zone'),
        45: ('Manhattan', 'Chinatown', 'Yellow Zone'),
        46: ('Bronx', 'City Island', 'Boro Zone'),
        47: ('Bronx', 'Claremont/Bathgate', 'Boro Zone'),
        48: ('Manhattan', 'Clinton East', 'Yellow Zone'),
        49: ('Brooklyn', 'Clinton Hill', 'Boro Zone'),
        50: ('Manhattan', 'Clinton West', 'Yellow Zone'),
        51: ('Bronx', 'Co-Op City', 'Boro Zone'),
        52: ('Brooklyn', 'Cobble Hill', 'Boro Zone'),
        53: ('Queens', 'College Point', 'Boro Zone'),
        54: ('Brooklyn', 'Columbia Street', 'Boro Zone'),
        55: ('Brooklyn', 'Coney Island', 'Boro Zone'),
        56: ('Queens', 'Corona', 'Boro Zone'),
        57: ('Queens', 'Corona', 'Boro Zone'),
        58: ('Bronx', 'Country Club', 'Boro Zone'),
        59: ('Bronx', 'Crotona Park', 'Boro Zone'),
        60: ('Bronx', 'Crotona Park East', 'Boro Zone'),
        61: ('Brooklyn', 'Crown Heights North', 'Boro Zone'),
        62: ('Brooklyn', 'Crown Heights South', 'Boro Zone'),
        63: ('Brooklyn', 'Cypress Hills', 'Boro Zone'),
        64: ('Queens', 'Douglaston', 'Boro Zone'),
        65: ('Brooklyn', 'Downtown Brooklyn/MetroTech', 'Boro Zone'),
        66: ('Brooklyn', 'DUMBO/Vinegar Hill', 'Boro Zone'),
        67: ('Brooklyn', 'Dyker Heights', 'Boro Zone'),
        68: ('Manhattan', 'East Chelsea', 'Yellow Zone'),
        69: ('Bronx', 'East Concourse/Concourse Village', 'Boro Zone'),
        70: ('Queens', 'East Elmhurst', 'Boro Zone'),
        71: ('Brooklyn', 'East Flatbush/Farragut', 'Boro Zone'),
        72: ('Brooklyn', 'East Flatbush/Remsen Village', 'Boro Zone'),
        73: ('Queens', 'East Flushing', 'Boro Zone'),
        74: ('Manhattan', 'East Harlem North', 'Boro Zone'),
        75: ('Manhattan', 'East Harlem South', 'Boro Zone'),
        76: ('Brooklyn', 'East New York', 'Boro Zone'),
        77: ('Brooklyn', 'East New York/Pennsylvania Avenue', 'Boro Zone'),
        78: ('Bronx', 'East Tremont', 'Boro Zone'),
        79: ('Manhattan', 'East Village', 'Yellow Zone'),
        80: ('Brooklyn', 'East Williamsburg', 'Boro Zone'),
        81: ('Bronx', 'Eastchester', 'Boro Zone'),
        82: ('Queens', 'Elmhurst', 'Boro Zone'),
        83: ('Queens', 'Elmhurst/Maspeth', 'Boro Zone'),
        84: ('Staten Island', 'Eltingville/Annadale/Prince\'s Bay', 'Boro Zone'),
        85: ('Brooklyn', 'Erasmus', 'Boro Zone'),
        86: ('Queens', 'Far Rockaway', 'Boro Zone'),
        87: ('Manhattan', 'Financial District North', 'Yellow Zone'),
        88: ('Manhattan', 'Financial District South', 'Yellow Zone'),
        89: ('Brooklyn', 'Flatbush/Ditmas Park', 'Boro Zone'),
        90: ('Manhattan', 'Flatiron', 'Yellow Zone'),
        91: ('Brooklyn', 'Flatlands', 'Boro Zone'),
        92: ('Queens', 'Flushing', 'Boro Zone'),
        93: ('Queens', 'Flushing Meadows-Corona Park', 'Boro Zone'),
        94: ('Bronx', 'Fordham South', 'Boro Zone'),
        95: ('Queens', 'Forest Hills', 'Boro Zone'),
        96: ('Queens', 'Forest Park/Highland Park', 'Boro Zone'),
        97: ('Brooklyn', 'Fort Greene', 'Boro Zone'),
        98: ('Queens', 'Fresh Meadows', 'Boro Zone'),
        99: ('Staten Island', 'Freshkills Park', 'Boro Zone'),
        100: ('Manhattan', 'Garment District', 'Yellow Zone'),
        101: ('Queens', 'Glen Oaks', 'Boro Zone'),
        102: ('Queens', 'Glendale', 'Boro Zone'),
        103: ('Manhattan', 'Governor\'s Island/Ellis Island/Liberty Island', 'Yellow Zone'),
        104: ('Manhattan', 'Governor\'s Island/Ellis Island/Liberty Island', 'Yellow Zone'),
        105: ('Manhattan', 'Governor\'s Island/Ellis Island/Liberty Island', 'Yellow Zone'),
        106: ('Brooklyn', 'Gowanus', 'Boro Zone'),
        107: ('Manhattan', 'Gramercy', 'Yellow Zone'),
        108: ('Brooklyn', 'Gravesend', 'Boro Zone'),
        109: ('Staten Island', 'Great Kills', 'Boro Zone'),
        110: ('Staten Island', 'Great Kills Park', 'Boro Zone'),
        111: ('Brooklyn', 'Green-Wood Cemetery', 'Boro Zone'),
        112: ('Brooklyn', 'Greenpoint', 'Boro Zone'),
        113: ('Manhattan', 'Greenwich Village North', 'Yellow Zone'),
        114: ('Manhattan', 'Greenwich Village South', 'Yellow Zone'),
        115: ('Staten Island', 'Grymes Hill/Clifton', 'Boro Zone'),
        116: ('Manhattan', 'Hamilton Heights', 'Boro Zone'),
        117: ('Queens', 'Hammels/Arverne', 'Boro Zone'),
        118: ('Staten Island', 'Heartland Village/Todt Hill', 'Boro Zone'),
        119: ('Bronx', 'Highbridge', 'Boro Zone'),
        120: ('Manhattan', 'Highbridge Park', 'Boro Zone'),
        121: ('Queens', 'Hillcrest/Pomonok', 'Boro Zone'),
        122: ('Queens', 'Hollis', 'Boro Zone'),
        123: ('Brooklyn', 'Homecrest', 'Boro Zone'),
        124: ('Queens', 'Howard Beach', 'Boro Zone'),
        125: ('Manhattan', 'Hudson Sq', 'Yellow Zone'),
        126: ('Bronx', 'Hunts Point', 'Boro Zone'),
        127: ('Manhattan', 'Inwood', 'Boro Zone'),
        128: ('Manhattan', 'Inwood Hill Park', 'Boro Zone'),
        129: ('Queens', 'Jackson Heights', 'Boro Zone'),
        130: ('Queens', 'Jamaica', 'Boro Zone'),
        131: ('Queens', 'Jamaica Estates', 'Boro Zone'),
        132: ('Queens', 'JFK Airport', 'Airports'),
        133: ('Brooklyn', 'Kensington', 'Boro Zone'),
        134: ('Queens', 'Kew Gardens', 'Boro Zone'),
        135: ('Queens', 'Kew Gardens Hills', 'Boro Zone'),
        136: ('Bronx', 'Kingsbridge Heights', 'Boro Zone'),
        137: ('Manhattan', 'Kips Bay', 'Yellow Zone'),
        138: ('Queens', 'LaGuardia Airport', 'Airports'),
        139: ('Queens', 'Laurelton', 'Boro Zone'),
        140: ('Manhattan', 'Lenox Hill East', 'Yellow Zone'),
        141: ('Manhattan', 'Lenox Hill West', 'Yellow Zone'),
        142: ('Manhattan', 'Lincoln Square East', 'Yellow Zone'),
        143: ('Manhattan', 'Lincoln Square West', 'Yellow Zone'),
        144: ('Manhattan', 'Little Italy/NoLiTa', 'Yellow Zone'),
        145: ('Queens', 'Long Island City/Hunters Point', 'Boro Zone'),
        146: ('Queens', 'Long Island City/Queens Plaza', 'Boro Zone'),
        147: ('Bronx', 'Longwood', 'Boro Zone'),
        148: ('Manhattan', 'Lower East Side', 'Yellow Zone'),
        149: ('Brooklyn', 'Madison', 'Boro Zone'),
        150: ('Brooklyn', 'Manhattan Beach', 'Boro Zone'),
        151: ('Manhattan', 'Manhattan Valley', 'Yellow Zone'),
        152: ('Manhattan', 'Manhattanville', 'Boro Zone'),
        153: ('Manhattan', 'Marble Hill', 'Boro Zone'),
        154: ('Brooklyn', 'Marine Park/Floyd Bennett Field', 'Boro Zone'),
        155: ('Brooklyn', 'Marine Park/Mill Basin', 'Boro Zone'),
        156: ('Staten Island', 'Mariners Harbor', 'Boro Zone'),
        157: ('Queens', 'Maspeth', 'Boro Zone'),
        158: ('Manhattan', 'Meatpacking/West Village West', 'Yellow Zone'),
        159: ('Bronx', 'Melrose South', 'Boro Zone'),
        160: ('Queens', 'Middle Village', 'Boro Zone'),
        161: ('Manhattan', 'Midtown Center', 'Yellow Zone'),
        162: ('Manhattan', 'Midtown East', 'Yellow Zone'),
        163: ('Manhattan', 'Midtown North', 'Yellow Zone'),
        164: ('Manhattan', 'Midtown South', 'Yellow Zone'),
        165: ('Brooklyn', 'Midwood', 'Boro Zone'),
        166: ('Manhattan', 'Morningside Heights', 'Boro Zone'),
        167: ('Bronx', 'Morrisania/Melrose', 'Boro Zone'),
        168: ('Bronx', 'Mott Haven/Port Morris', 'Boro Zone'),
        169: ('Bronx', 'Mount Hope', 'Boro Zone'),
        170: ('Manhattan', 'Murray Hill', 'Yellow Zone'),
        171: ('Queens', 'Murray Hill-Queens', 'Boro Zone'),
        172: ('Staten Island', 'New Dorp/Midland Beach', 'Boro Zone'),
        173: ('Queens', 'North Corona', 'Boro Zone'),
        174: ('Bronx', 'Norwood', 'Boro Zone'),
        175: ('Queens', 'Oakland Gardens', 'Boro Zone'),
        176: ('Staten Island', 'Oakwood', 'Boro Zone'),
        177: ('Brooklyn', 'Ocean Hill', 'Boro Zone'),
        178: ('Brooklyn', 'Ocean Parkway South', 'Boro Zone'),
        179: ('Queens', 'Old Astoria', 'Boro Zone'),
        180: ('Queens', 'Ozone Park', 'Boro Zone'),
        181: ('Brooklyn', 'Park Slope', 'Boro Zone'),
        182: ('Bronx', 'Parkchester', 'Boro Zone'),
        183: ('Bronx', 'Pelham Bay', 'Boro Zone'),
        184: ('Bronx', 'Pelham Bay Park', 'Boro Zone'),
        185: ('Bronx', 'Pelham Parkway', 'Boro Zone'),
        186: ('Manhattan', 'Penn Station/Madison Sq West', 'Yellow Zone'),
        187: ('Staten Island', 'Port Richmond', 'Boro Zone'),
        188: ('Brooklyn', 'Prospect-Lefferts Gardens', 'Boro Zone'),
        189: ('Brooklyn', 'Prospect Heights', 'Boro Zone'),
        190: ('Brooklyn', 'Prospect Park', 'Boro Zone'),
        191: ('Queens', 'Queens Village', 'Boro Zone'),
        192: ('Queens', 'Queensboro Hill', 'Boro Zone'),
        193: ('Queens', 'Queensbridge/Ravenswood', 'Boro Zone'),
        194: ('Manhattan', 'Randalls Island', 'Yellow Zone'),
        195: ('Brooklyn', 'Red Hook', 'Boro Zone'),
        196: ('Queens', 'Rego Park', 'Boro Zone'),
        197: ('Queens', 'Richmond Hill', 'Boro Zone'),
        198: ('Queens', 'Ridgewood', 'Boro Zone'),
        199: ('Bronx', 'Rikers Island', 'Boro Zone'),
        200: ('Bronx', 'Riverdale/North Riverdale/Fieldston', 'Boro Zone'),
        201: ('Queens', 'Rockaway Park', 'Boro Zone'),
        202: ('Manhattan', 'Roosevelt Island', 'Boro Zone'),
        203: ('Queens', 'Rosedale', 'Boro Zone'),
        204: ('Staten Island', 'Rossville/Woodrow', 'Boro Zone'),
        205: ('Queens', 'Saint Albans', 'Boro Zone'),
        206: ('Staten Island', 'Saint George/New Brighton', 'Boro Zone'),
        207: ('Queens', 'Saint Michaels Cemetery/Woodside', 'Boro Zone'),
        208: ('Bronx', 'Schuylerville/Edgewater Park', 'Boro Zone'),
        209: ('Manhattan', 'Seaport', 'Yellow Zone'),
        210: ('Brooklyn', 'Sheepshead Bay', 'Boro Zone'),
        211: ('Manhattan', 'SoHo', 'Yellow Zone'),
        212: ('Bronx', 'Soundview/Bruckner', 'Boro Zone'),
        213: ('Bronx', 'Soundview/Castle Hill', 'Boro Zone'),
        214: ('Staten Island', 'South Beach/Dongan Hills', 'Boro Zone'),
        215: ('Queens', 'South Jamaica', 'Boro Zone'),
        216: ('Queens', 'South Ozone Park', 'Boro Zone'),
        217: ('Brooklyn', 'South Williamsburg', 'Boro Zone'),
        218: ('Queens', 'Springfield Gardens North', 'Boro Zone'),
        219: ('Queens', 'Springfield Gardens South', 'Boro Zone'),
        220: ('Bronx', 'Spuyten Duyvil/Kingsbridge', 'Boro Zone'),
        221: ('Staten Island', 'Stapleton', 'Boro Zone'),
        222: ('Brooklyn', 'Starrett City', 'Boro Zone'),
        223: ('Queens', 'Steinway', 'Boro Zone'),
        224: ('Manhattan', 'Stuy Town/Peter Cooper Village', 'Yellow Zone'),
        225: ('Brooklyn', 'Stuyvesant Heights', 'Boro Zone'),
        226: ('Queens', 'Sunnyside', 'Boro Zone'),
        227: ('Brooklyn', 'Sunset Park East', 'Boro Zone'),
        228: ('Brooklyn', 'Sunset Park West', 'Boro Zone'),
        229: ('Manhattan', 'Sutton Place/Turtle Bay North', 'Yellow Zone'),
        230: ('Manhattan', 'Times Sq/Theatre District', 'Yellow Zone'),
        231: ('Manhattan', 'TriBeCa/Civic Center', 'Yellow Zone'),
        232: ('Manhattan', 'Two Bridges/Seward Park', 'Yellow Zone'),
        233: ('Manhattan', 'UN/Turtle Bay South', 'Yellow Zone'),
        234: ('Manhattan', 'Union Sq', 'Yellow Zone'),
        235: ('Bronx', 'University Heights/Morris Heights', 'Boro Zone'),
        236: ('Manhattan', 'Upper East Side North', 'Yellow Zone'),
        237: ('Manhattan', 'Upper East Side South', 'Yellow Zone'),
        238: ('Manhattan', 'Upper West Side North', 'Yellow Zone'),
        239: ('Manhattan', 'Upper West Side South', 'Yellow Zone'),
        240: ('Bronx', 'Van Cortlandt Park', 'Boro Zone'),
        241: ('Bronx', 'Van Cortlandt Village', 'Boro Zone'),
        242: ('Bronx', 'Van Nest/Morris Park', 'Boro Zone'),
        243: ('Manhattan', 'Washington Heights North', 'Boro Zone'),
        244: ('Manhattan', 'Washington Heights South', 'Boro Zone'),
        245: ('Staten Island', 'West Brighton', 'Boro Zone'),
        246: ('Manhattan', 'West Chelsea/Hudson Yards', 'Yellow Zone'),
        247: ('Bronx', 'West Concourse', 'Boro Zone'),
        248: ('Bronx', 'West Farms/Bronx River', 'Boro Zone'),
        249: ('Manhattan', 'West Village', 'Yellow Zone'),
        250: ('Bronx', 'Westchester Village/Unionport', 'Boro Zone'),
        251: ('Staten Island', 'Westerleigh', 'Boro Zone'),
        252: ('Queens', 'Whitestone', 'Boro Zone'),
        253: ('Queens', 'Willets Point', 'Boro Zone'),
        254: ('Bronx', 'Williamsbridge/Olinville', 'Boro Zone'),
        255: ('Brooklyn', 'Williamsburg (North Side)', 'Boro Zone'),
        256: ('Brooklyn', 'Williamsburg (South Side)', 'Boro Zone'),
        257: ('Brooklyn', 'Windsor Terrace', 'Boro Zone'),
        258: ('Queens', 'Woodhaven', 'Boro Zone'),
        259: ('Bronx', 'Woodlawn/Wakefield', 'Boro Zone'),
        260: ('Queens', 'Woodside', 'Boro Zone'),
        261: ('Manhattan', 'World Trade Center', 'Yellow Zone'),
        262: ('Manhattan', 'Yorkville East', 'Yellow Zone'),
        263: ('Manhattan', 'Yorkville West', 'Yellow Zone'),
        264: ('Unknown', 'N/A', 'N/A'),
        265: ('N/A', 'Outside of NYC', 'N/A')
    }
    
    df = df.with_columns([
        pl.col('location_id').map_elements(lambda x: location_details.get(x, ('Unknown', 'Unknown', 'Unknown'))[0]).alias('borough'),
        pl.col('location_id').map_elements(lambda x: location_details.get(x, ('Unknown', 'Unknown', 'Unknown'))[1]).alias('zone'),
        pl.col('location_id').map_elements(lambda x: location_details.get(x, ('Unknown', 'Unknown', 'Unknown'))[2]).alias('service_zone')
    ])
    
    existing_query = 'SELECT location_id FROM dim_location'
    try:
        logger.info(f"Executing query on DST_CONN: {existing_query}")
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('location_id').is_in(existing['location_id']))
        logger.info(f"Found {df.height} new locations after filtering existing ones.")
    except Exception as e:
        logger.warning(f"Could not read existing dim_location or table is empty, proceeding with all new rows: {e}")
        pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            logger.info(f"Writing {df.height} new rows to dim_location.")
            df.write_database(table_name='dim_location', connection=engine, if_table_exists='append')
            with engine.connect() as conn:
                logger.info("Executing ANALYZE on dim_location.")
                conn.execute(sqlalchemy.text('ANALYZE dim_location'))
        except Exception as e:
            logger.error(f"Error writing to dim_location: {e}")
            raise
        finally:
            engine.dispose()
            logger.info("dim_location engine disposed.")
    else:
        logger.info("No new location data to write.")
    logger.info("Finished dim_location migration.")


def migrate_dim_date(**kwargs):
    logger.info("Starting dim_date migration.")
    query1 = 'SELECT DISTINCT CAST(tpep_pickup_datetime AS DATE) as full_date FROM yellow_tripdata WHERE tpep_pickup_datetime IS NOT NULL'
    query2 = 'SELECT DISTINCT CAST(tpep_dropoff_datetime AS DATE) as full_date FROM yellow_tripdata WHERE tpep_dropoff_datetime IS NOT NULL'
    try:
        logger.info(f"Executing query1 on SRC_CONN: {query1}")
        df1 = pl.read_database_uri(query=query1, uri=SRC_CONN)
        logger.info(f"Read {df1.height} pickup dates.")
        logger.info(f"Executing query2 on SRC_CONN: {query2}")
        df2 = pl.read_database_uri(query=query2, uri=SRC_CONN)
        logger.info(f"Read {df2.height} dropoff dates.")
        df = pl.concat([df1, df2]).unique(subset=['full_date'])
        logger.info(f"Combined and found {df.height} unique dates.")
    except Exception as e:
        logger.error(f"Error reading from source for dim_date: {e}")
        raise

    df = df.with_columns([
        pl.col('full_date').dt.year().alias('year'),
        pl.col('full_date').dt.month().alias('month'),
        pl.col('full_date').dt.day().alias('day'),
        pl.col('full_date').dt.weekday().alias('day_of_week'),
        pl.col('full_date').dt.strftime('%A').alias('day_name'),
        pl.col('full_date').dt.strftime('%B').alias('month_name'),
        pl.col('full_date').dt.quarter().alias('quarter'),
    ]).with_columns(
        pl.col('day_of_week').is_in([6, 7]).alias('is_weekend')
    )

    existing_query = 'SELECT full_date FROM dim_date'
    try:
        logger.info(f"Executing query on DST_CONN: {existing_query}")
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN).with_columns(
            pl.col('full_date').cast(pl.Date)
        )
        df = df.filter(~pl.col('full_date').is_in(existing['full_date']))
        logger.info(f"Found {df.height} new dates after filtering existing ones.")
    except Exception as e:
        logger.warning(f"Could not read existing dim_date or table is empty, proceeding with all new rows: {e}")
        # print(f"Could not read existing dim_date or table is empty: {e}") # Replaced by logger
        pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            logger.info(f"Writing {df.height} new rows to dim_date.")
            df.write_database(table_name='dim_date', connection=engine, if_table_exists='append')
            with engine.connect() as conn:
                logger.info("Executing ANALYZE on dim_date.")
                conn.execute(sqlalchemy.text('ANALYZE dim_date'))
        except Exception as e:
            logger.error(f"Error writing to dim_date: {e}")
            raise
        finally:
            engine.dispose()
            logger.info("dim_date engine disposed.")
    else:
        logger.info("No new date data to write.")
    logger.info("Finished dim_date migration.")


def migrate_fact_trips(**kwargs):
    logger.info("=== Starting migrate_fact_trips ===")
    logger.info("Initializing migration process...")
    
    fact_cols = [
        'vendor_key', 'pickup_date_key', 'dropoff_date_key', 'pickup_location_key',
        'dropoff_location_key', 'rate_code_key', 'payment_type_key', 'store_and_fwd_flag',
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance',
        'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'trip_duration',
        'airport_fee'
    ]
    
    logger.info("🔧 Setting up database connection and configurations...")
    engine = sqlalchemy.create_engine(DST_CONN)
    
    altered_fact_trips_settings = False
    total_rows_processed = 0
    start_time = time.time()

    try:
        logger.info("📚 Loading dimension tables from data mart (DST_CONN)...")
        dim_vendor = pl.read_database_uri(query='SELECT vendor_key, vendor_id FROM dim_vendor', uri=DST_CONN)
        dim_rate_code = pl.read_database_uri(query='SELECT rate_code_key, rate_code_id FROM dim_rate_code', uri=DST_CONN)
        dim_payment_type = pl.read_database_uri(query='SELECT payment_type_key, payment_type_id FROM dim_payment_type', uri=DST_CONN)
        dim_location = pl.read_database_uri(query='SELECT location_key, location_id FROM dim_location', uri=DST_CONN)
        dim_date = pl.read_database_uri(query='SELECT date_key, full_date FROM dim_date', uri=DST_CONN).with_columns(
            pl.col('full_date').cast(pl.Date)
        )
        logger.info("✅ Dimension tables loaded.")

        inspector = sqlalchemy.inspect(engine)
        if inspector.has_table("fact_trips"):
            logger.info("- Table 'fact_trips' exists. Attempting to alter settings...")
            with engine.connect() as conn:
                logger.info("- Disabling table logging for fact_trips...")
                conn.execute(sqlalchemy.text("ALTER TABLE fact_trips SET UNLOGGED"))
                logger.info("- Disabling triggers for fact_trips...")
                conn.execute(sqlalchemy.text("ALTER TABLE fact_trips DISABLE TRIGGER ALL"))
                altered_fact_trips_settings = True
                logger.info("- Successfully altered 'fact_trips' settings (UNLOGGED, TRIGGERS DISABLED).")
        else:
            logger.info("- Table 'fact_trips' does not exist yet. It will be created by Polars during data write.")
        
        batch_size = 100000
        offset = 0
        
        logger.info("🚀 Starting batch processing for fact_trips...")
        while True:
            batch_start_time = time.time()
            logger.info(f"--- Batch {offset//batch_size + 1} ---")
            logger.info(f"- Processing records from offset {offset}, batch size {batch_size}")
            
            query = f"""
                SELECT *
                FROM yellow_tripdata
                ORDER BY tpep_pickup_datetime
                LIMIT {batch_size} OFFSET {offset}
            """
            
            logger.info(f"- Fetching data from source with query: {query[:200]}...")
            try:
                df = pl.read_database_uri(query=query, uri=SRC_CONN)
            except Exception as e:
                logger.error(f"Error fetching data for fact_trips batch (offset {offset}): {e}")
                raise

            if df.is_empty():
                logger.info("✅ No more data to process for fact_trips!")
                break
            
            logger.info(f"- Retrieved {df.height} rows for current batch.")
            
            logger.info("- Transforming data for fact_trips batch...")
            logger.info("  • Normalizing column names")
            df = df.rename({col: col.lower() for col in df.columns})
            
            logger.info("  • Processing dates for join keys")
            df = df.with_columns([
                pl.col('tpep_pickup_datetime').cast(pl.Datetime).dt.date().alias('pickup_date'),
                pl.col('tpep_dropoff_datetime').cast(pl.Datetime).dt.date().alias('dropoff_date')
            ])

            logger.info("  • Joining with dimension tables...")
            df = df.join(dim_vendor, left_on='vendorid', right_on='vendor_id', how='left')
            df = df.join(dim_rate_code, left_on='ratecodeid', right_on='rate_code_id', how='left')
            df = df.join(dim_payment_type, left_on='payment_type', right_on='payment_type_id', how='left')
            df = df.join(dim_location.rename({'location_key': 'pickup_location_key', 'location_id': 'pu_loc_id'}), 
                         left_on='pulocationid', right_on='pu_loc_id', how='left')
            df = df.join(dim_location.rename({'location_key': 'dropoff_location_key', 'location_id': 'do_loc_id'}), 
                         left_on='dolocationid', right_on='do_loc_id', how='left')
            df = df.join(dim_date.rename({'date_key': 'pickup_date_key', 'full_date': 'pk_full_date'}), 
                         left_on='pickup_date', right_on='pk_full_date', how='left')
            df = df.join(dim_date.rename({'date_key': 'dropoff_date_key', 'full_date': 'do_full_date'}), 
                         left_on='dropoff_date', right_on='do_full_date', how='left')
            logger.info("  ✅ Joins completed.")

            logger.info("  • Calculating trip duration")
            df = df.with_columns([
                (
                    pl.col('tpep_dropoff_datetime') - pl.col('tpep_pickup_datetime')
                )
                .dt.total_seconds()
                .cast(pl.String)
                .add(pl.lit(" seconds"))
                .alias('trip_duration')
            ])
            
            logger.info("  • Selecting final columns for fact_trips")
            current_cols = df.columns
            cols_to_select = [col for col in fact_cols if col in current_cols]
            missing_cols = [col for col in fact_cols if col not in current_cols]
            if missing_cols:
                logger.warning(f"Missing columns for fact_trips, they will not be included: {missing_cols}")

            df_final = df.select(cols_to_select)

            logger.info(f"- Writing batch of {df_final.height} rows to fact_trips database...")
            try:
                rows_written = df_final.write_database(
                    table_name='fact_trips',
                    connection=engine,
                    if_table_exists='append'
                )
                logger.info(f"Successfully wrote batch to fact_trips. Rows affected: {rows_written if rows_written is not None else df_final.height}")
            except Exception as e:
                logger.error(f"Error writing batch to fact_trips (offset {offset}): {e}")
                raise

            total_rows_processed += df.height
            batch_duration = time.time() - batch_start_time
            total_duration = time.time() - start_time
            
            logger.info("📊 Batch Statistics:")
            logger.info(f"- Batch duration: {batch_duration:.2f} seconds")
            if batch_duration > 0:
                 logger.info(f"- Average speed: {df.height/batch_duration:.0f} rows/second")
            logger.info(f"- Total progress: {total_rows_processed:,} rows")
            logger.info(f"- Total duration: {total_duration:.2f} seconds")
            if total_duration > 0:
                logger.info(f"- Overall average speed: {total_rows_processed/total_duration:.0f} rows/second")
            
            offset += batch_size
            
    except Exception as e:
        logger.error(f"A critical error occurred during fact_trips migration: {e}")
        raise
    finally:
        logger.info("🔧 Cleaning up fact_trips configurations...")
        
        inspector_final = sqlalchemy.inspect(engine)
        final_fact_table_exists = inspector_final.has_table("fact_trips")

        if altered_fact_trips_settings:
            try:
                with engine.connect() as conn:
                    logger.info("- Restoring table fact_trips to LOGGED (as it was set to UNLOGGED)...")
                    conn.execute(sqlalchemy.text("ALTER TABLE fact_trips SET LOGGED"))
                    logger.info("- Enabling triggers for fact_trips (as they were disabled)...")
                    conn.execute(sqlalchemy.text("ALTER TABLE fact_trips ENABLE TRIGGER ALL"))
                    logger.info("Successfully restored LOGGED and ENABLED TRIGGERS for fact_trips.")
            except Exception as e:
                logger.error(f"Error restoring fact_trips LOGGED/TRIGGER settings: {e}")
        
        if final_fact_table_exists:
            try:
                with engine.connect() as conn:
                    logger.info("- Running ANALYZE on fact_trips...")
                    conn.execute(sqlalchemy.text("ANALYZE fact_trips"))
                    logger.info("Successfully ran ANALYZE on fact_trips.")
            except Exception as e:
                logger.error(f"Error running ANALYZE on fact_trips: {e}")
        else:
            logger.info("- fact_trips table does not exist at the end, skipping final ANALYZE.")

        engine.dispose()
        logger.info("fact_trips engine disposed.")

        logger.info(f"✨ Migration completed for fact_trips!")
        logger.info(f"- Total rows processed: {total_rows_processed:,}")
        final_total_duration = time.time() - start_time
        logger.info(f"- Total duration: {final_total_duration/60:.2f} minutes")
        logger.info("=====================================")


with DAG(
    dag_id='migrate_datawarehouse_to_datamart',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['migration', 'postgres'],
) as dag:
    t1 = PythonOperator(
        task_id='migrate_dim_vendor',
        python_callable=migrate_dim_vendor
    )
    t2 = PythonOperator(
        task_id='migrate_dim_rate_code',
        python_callable=migrate_dim_rate_code
    )
    t3 = PythonOperator(
        task_id='migrate_dim_payment_type',
        python_callable=migrate_dim_payment_type
    )
    t4 = PythonOperator(
        task_id='migrate_dim_location',
        python_callable=migrate_dim_location
    )
    t5 = PythonOperator(
        task_id='migrate_dim_date',
        python_callable=migrate_dim_date
    )
    t6 = PythonOperator(
        task_id='migrate_fact_trips',
        python_callable=migrate_fact_trips
    )

    [t1, t2, t3, t4, t5] >> t6