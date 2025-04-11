INSERT INTO dim_vendor (vendor_id)
SELECT DISTINCT "VendorID"
FROM dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT "VendorID" FROM yellow_tripdata')
     AS source_data("VendorID" INT)
WHERE "VendorID" IS NOT NULL
ON CONFLICT (vendor_id) DO NOTHING; -- Avoid duplicates if script is run multiple times

-- Populate dim_rate_code
INSERT INTO dim_rate_code (rate_code_id) -- Add rate_code_name manually or from lookup if available
SELECT DISTINCT "RatecodeID"
FROM dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT "RatecodeID" FROM yellow_tripdata')
     AS source_data("RatecodeID" INT)
WHERE "RatecodeID" IS NOT NULL
ON CONFLICT (rate_code_id) DO NOTHING;

-- Populate dim_payment_type
INSERT INTO dim_payment_type (payment_type_id) -- Add payment_type_name manually or from lookup if available
SELECT DISTINCT payment_type
FROM dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT payment_type FROM yellow_tripdata')
     AS source_data(payment_type INT)
WHERE payment_type IS NOT NULL
ON CONFLICT (payment_type_id) DO NOTHING;

-- Populate dim_location (Requires external lookup table for Borough, Zone etc.)
INSERT INTO dim_location (location_id)
SELECT DISTINCT location_id FROM (
    SELECT "PULocationID" as location_id FROM dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT "PULocationID" FROM yellow_tripdata') AS t1("PULocationID" INT) WHERE "PULocationID" IS NOT NULL
    UNION
    SELECT "DOLocationID" as location_id FROM dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT "DOLocationID" FROM yellow_tripdata') AS t2("DOLocationID" INT) WHERE "DOLocationID" IS NOT NULL
) all_locations
ON CONFLICT (location_id) DO NOTHING;
-- NOTE: You would ideally perform an UPDATE here after inserting,
-- joining with your taxi zone lookup table to fill in borough, zone etc.
-- e.g. UPDATE dim_location SET borough = l.borough, zone = l.zone FROM lookup_table l WHERE dim_location.location_id = l.locationid;

-- Populate dim_date (Dynamically for dates present in the data)
INSERT INTO dim_date (full_date, year, month, day, day_of_week, day_name, month_name, quarter, is_weekend)
SELECT
    datum AS full_date,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(DAY FROM datum) AS day,
    EXTRACT(ISODOW FROM datum) AS day_of_week, -- ISO standard: 1 = Monday, 7 = Sunday
    TO_CHAR(datum, 'Day') AS day_name,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(QUARTER FROM datum) AS quarter,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT DISTINCT DATE(tpep_pickup_datetime) AS datum
    FROM dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT tpep_pickup_datetime FROM yellow_tripdata') AS t1(tpep_pickup_datetime TIMESTAMP)
    WHERE tpep_pickup_datetime IS NOT NULL
    UNION
    SELECT DISTINCT DATE(tpep_dropoff_datetime) AS datum
    FROM dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT tpep_dropoff_datetime FROM yellow_tripdata') AS t2(tpep_dropoff_datetime TIMESTAMP)
    WHERE tpep_dropoff_datetime IS NOT NULL
) distinct_dates
ON CONFLICT (full_date) DO NOTHING;


-- Populate Fact Table

INSERT INTO fact_trips (
    vendor_key,
    pickup_date_key,
    dropoff_date_key,
    pickup_location_key,
    dropoff_location_key,
    rate_code_key,
    payment_type_key,
    store_and_fwd_flag,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    trip_duration
)
SELECT
    dv.vendor_key,
    dd_pickup.date_key AS pickup_date_key,
    dd_dropoff.date_key AS dropoff_date_key,
    dl_pickup.location_key AS pickup_location_key,
    dl_dropoff.location_key AS dropoff_location_key,
    drc.rate_code_key,
    dpt.payment_type_key,
    source.store_and_fwd_flag,
    source.tpep_pickup_datetime,
    source.tpep_dropoff_datetime,
    source.passenger_count,
    source.trip_distance,
    source.fare_amount,
    source.extra,
    source.mta_tax,
    source.tip_amount,
    source.tolls_amount,
    source.improvement_surcharge,
    source.total_amount,
    source.congestion_surcharge,
    source."Airport_fee",
    -- Calculate trip duration, handle potential NULLs
    CASE
        WHEN source.tpep_dropoff_datetime IS NOT NULL AND source.tpep_pickup_datetime IS NOT NULL
        THEN source.tpep_dropoff_datetime - source.tpep_pickup_datetime
        ELSE NULL
    END AS trip_duration
FROM
    dblink('postgresql://postgres:admin@data-warehouse:5432/taxi', 'SELECT
        "VendorID", tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, "RatecodeID", store_and_fwd_flag, "PULocationID", "DOLocationID",
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge, "Airport_fee"
        FROM yellow_tripdata')
    AS source(
        "VendorID" INT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count BIGINT, -- Cast later if needed
        trip_distance DOUBLE PRECISION,
        "RatecodeID" BIGINT, -- Cast later if needed
        store_and_fwd_flag TEXT,
        "PULocationID" INT,
        "DOLocationID" INT,
        payment_type BIGINT, -- Cast later if needed
        fare_amount DOUBLE PRECISION,
        extra DOUBLE PRECISION,
        mta_tax DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        tolls_amount DOUBLE PRECISION,
        improvement_surcharge DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        congestion_surcharge DOUBLE PRECISION,
        "Airport_fee" DOUBLE PRECISION
    )
LEFT JOIN dim_vendor dv ON source."VendorID" = dv.vendor_id
LEFT JOIN dim_date dd_pickup ON DATE(source.tpep_pickup_datetime) = dd_pickup.full_date
LEFT JOIN dim_date dd_dropoff ON DATE(source.tpep_dropoff_datetime) = dd_dropoff.full_date
LEFT JOIN dim_location dl_pickup ON source."PULocationID" = dl_pickup.location_id
LEFT JOIN dim_location dl_dropoff ON source."DOLocationID" = dl_dropoff.location_id
LEFT JOIN dim_rate_code drc ON source."RatecodeID" = drc.rate_code_id
LEFT JOIN dim_payment_type dpt ON source.payment_type = dpt.payment_type_id;

CREATE TEMP TABLE staging_locations (
    location_id INT PRIMARY KEY,
    borough VARCHAR(255),
    zone VARCHAR(255),
    service_zone VARCHAR(255)
);

-- Insert the location data into the staging table
INSERT INTO staging_locations (location_id, borough, zone, service_zone) VALUES
(1,'EWR','Newark Airport','EWR'),
(2,'Queens','Jamaica Bay','Boro Zone'),
(3,'Bronx','Allerton/Pelham Gardens','Boro Zone'),
(4,'Manhattan','Alphabet City','Yellow Zone'),
(5,'Staten Island','Arden Heights','Boro Zone'),
(6,'Staten Island','Arrochar/Fort Wadsworth','Boro Zone'),
(7,'Queens','Astoria','Boro Zone'),
(8,'Queens','Astoria Park','Boro Zone'),
(9,'Queens','Auburndale','Boro Zone'),
(10,'Queens','Baisley Park','Boro Zone'),
(11,'Brooklyn','Bath Beach','Boro Zone'),
(12,'Manhattan','Battery Park','Yellow Zone'),
(13,'Manhattan','Battery Park City','Yellow Zone'),
(14,'Brooklyn','Bay Ridge','Boro Zone'),
(15,'Queens','Bay Terrace/Fort Totten','Boro Zone'),
(16,'Queens','Bayside','Boro Zone'),
(17,'Brooklyn','Bedford','Boro Zone'),
(18,'Bronx','Bedford Park','Boro Zone'),
(19,'Queens','Bellerose','Boro Zone'),
(20,'Bronx','Belmont','Boro Zone'),
(21,'Brooklyn','Bensonhurst East','Boro Zone'),
(22,'Brooklyn','Bensonhurst West','Boro Zone'),
(23,'Staten Island','Bloomfield/Emerson Hill','Boro Zone'),
(24,'Manhattan','Bloomingdale','Yellow Zone'),
(25,'Brooklyn','Boerum Hill','Boro Zone'),
(26,'Brooklyn','Borough Park','Boro Zone'),
(27,'Queens','Breezy Point/Fort Tilden/Riis Beach','Boro Zone'),
(28,'Queens','Briarwood/Jamaica Hills','Boro Zone'),
(29,'Brooklyn','Brighton Beach','Boro Zone'),
(30,'Queens','Broad Channel','Boro Zone'),
(31,'Bronx','Bronx Park','Boro Zone'),
(32,'Bronx','Bronxdale','Boro Zone'),
(33,'Brooklyn','Brooklyn Heights','Boro Zone'),
(34,'Brooklyn','Brooklyn Navy Yard','Boro Zone'),
(35,'Brooklyn','Brownsville','Boro Zone'),
(36,'Brooklyn','Bushwick North','Boro Zone'),
(37,'Brooklyn','Bushwick South','Boro Zone'),
(38,'Queens','Cambria Heights','Boro Zone'),
(39,'Brooklyn','Canarsie','Boro Zone'),
(40,'Brooklyn','Carroll Gardens','Boro Zone'),
(41,'Manhattan','Central Harlem','Boro Zone'),
(42,'Manhattan','Central Harlem North','Boro Zone'),
(43,'Manhattan','Central Park','Yellow Zone'),
(44,'Staten Island','Charleston/Tottenville','Boro Zone'),
(45,'Manhattan','Chinatown','Yellow Zone'),
(46,'Bronx','City Island','Boro Zone'),
(47,'Bronx','Claremont/Bathgate','Boro Zone'),
(48,'Manhattan','Clinton East','Yellow Zone'),
(49,'Brooklyn','Clinton Hill','Boro Zone'),
(50,'Manhattan','Clinton West','Yellow Zone'),
(51,'Bronx','Co-Op City','Boro Zone'),
(52,'Brooklyn','Cobble Hill','Boro Zone'),
(53,'Queens','College Point','Boro Zone'),
(54,'Brooklyn','Columbia Street','Boro Zone'),
(55,'Brooklyn','Coney Island','Boro Zone'),
(56,'Queens','Corona','Boro Zone'),
(57,'Queens','Corona','Boro Zone'), -- Note: Duplicate ID 57?
(58,'Bronx','Country Club','Boro Zone'),
(59,'Bronx','Crotona Park','Boro Zone'),
(60,'Bronx','Crotona Park East','Boro Zone'),
(61,'Brooklyn','Crown Heights North','Boro Zone'),
(62,'Brooklyn','Crown Heights South','Boro Zone'),
(63,'Brooklyn','Cypress Hills','Boro Zone'),
(64,'Queens','Douglaston','Boro Zone'),
(65,'Brooklyn','Downtown Brooklyn/MetroTech','Boro Zone'),
(66,'Brooklyn','DUMBO/Vinegar Hill','Boro Zone'),
(67,'Brooklyn','Dyker Heights','Boro Zone'),
(68,'Manhattan','East Chelsea','Yellow Zone'),
(69,'Bronx','East Concourse/Concourse Village','Boro Zone'),
(70,'Queens','East Elmhurst','Boro Zone'),
(71,'Brooklyn','East Flatbush/Farragut','Boro Zone'),
(72,'Brooklyn','East Flatbush/Remsen Village','Boro Zone'),
(73,'Queens','East Flushing','Boro Zone'),
(74,'Manhattan','East Harlem North','Boro Zone'),
(75,'Manhattan','East Harlem South','Boro Zone'),
(76,'Brooklyn','East New York','Boro Zone'),
(77,'Brooklyn','East New York/Pennsylvania Avenue','Boro Zone'),
(78,'Bronx','East Tremont','Boro Zone'),
(79,'Manhattan','East Village','Yellow Zone'),
(80,'Brooklyn','East Williamsburg','Boro Zone'),
(81,'Bronx','Eastchester','Boro Zone'),
(82,'Queens','Elmhurst','Boro Zone'),
(83,'Queens','Elmhurst/Maspeth','Boro Zone'),
(84,'Staten Island','Eltingville/Annadale/Prince''s Bay','Boro Zone'),
(85,'Brooklyn','Erasmus','Boro Zone'),
(86,'Queens','Far Rockaway','Boro Zone'),
(87,'Manhattan','Financial District North','Yellow Zone'),
(88,'Manhattan','Financial District South','Yellow Zone'),
(89,'Brooklyn','Flatbush/Ditmas Park','Boro Zone'),
(90,'Manhattan','Flatiron','Yellow Zone'),
(91,'Brooklyn','Flatlands','Boro Zone'),
(92,'Queens','Flushing','Boro Zone'),
(93,'Queens','Flushing Meadows-Corona Park','Boro Zone'),
(94,'Bronx','Fordham South','Boro Zone'),
(95,'Queens','Forest Hills','Boro Zone'),
(96,'Queens','Forest Park/Highland Park','Boro Zone'),
(97,'Brooklyn','Fort Greene','Boro Zone'),
(98,'Queens','Fresh Meadows','Boro Zone'),
(99,'Staten Island','Freshkills Park','Boro Zone'),
(100,'Manhattan','Garment District','Yellow Zone'),
(101,'Queens','Glen Oaks','Boro Zone'),
(102,'Queens','Glendale','Boro Zone'),
(103,'Manhattan','Governor''s Island/Ellis Island/Liberty Island','Yellow Zone'),
(104,'Manhattan','Governor''s Island/Ellis Island/Liberty Island','Yellow Zone'),
(105,'Manhattan','Governor''s Island/Ellis Island/Liberty Island','Yellow Zone'),
(106,'Brooklyn','Gowanus','Boro Zone'),
(107,'Manhattan','Gramercy','Yellow Zone'),
(108,'Brooklyn','Gravesend','Boro Zone'),
(109,'Staten Island','Great Kills','Boro Zone'),
(110,'Staten Island','Great Kills Park','Boro Zone'),
(111,'Brooklyn','Green-Wood Cemetery','Boro Zone'),
(112,'Brooklyn','Greenpoint','Boro Zone'),
(113,'Manhattan','Greenwich Village North','Yellow Zone'),
(114,'Manhattan','Greenwich Village South','Yellow Zone'),
(115,'Staten Island','Grymes Hill/Clifton','Boro Zone'),
(116,'Manhattan','Hamilton Heights','Boro Zone'),
(117,'Queens','Hammels/Arverne','Boro Zone'),
(118,'Staten Island','Heartland Village/Todt Hill','Boro Zone'),
(119,'Bronx','Highbridge','Boro Zone'),
(120,'Manhattan','Highbridge Park','Boro Zone'),
(121,'Queens','Hillcrest/Pomonok','Boro Zone'),
(122,'Queens','Hollis','Boro Zone'),
(123,'Brooklyn','Homecrest','Boro Zone'),
(124,'Queens','Howard Beach','Boro Zone'),
(125,'Manhattan','Hudson Sq','Yellow Zone'),
(126,'Bronx','Hunts Point','Boro Zone'),
(127,'Manhattan','Inwood','Boro Zone'),
(128,'Manhattan','Inwood Hill Park','Boro Zone'),
(129,'Queens','Jackson Heights','Boro Zone'),
(130,'Queens','Jamaica','Boro Zone'),
(131,'Queens','Jamaica Estates','Boro Zone'),
(132,'Queens','JFK Airport','Airports'),
(133,'Brooklyn','Kensington','Boro Zone'),
(134,'Queens','Kew Gardens','Boro Zone'),
(135,'Queens','Kew Gardens Hills','Boro Zone'),
(136,'Bronx','Kingsbridge Heights','Boro Zone'),
(137,'Manhattan','Kips Bay','Yellow Zone'),
(138,'Queens','LaGuardia Airport','Airports'),
(139,'Queens','Laurelton','Boro Zone'),
(140,'Manhattan','Lenox Hill East','Yellow Zone'),
(141,'Manhattan','Lenox Hill West','Yellow Zone'),
(142,'Manhattan','Lincoln Square East','Yellow Zone'),
(143,'Manhattan','Lincoln Square West','Yellow Zone'),
(144,'Manhattan','Little Italy/NoLiTa','Yellow Zone'),
(145,'Queens','Long Island City/Hunters Point','Boro Zone'),
(146,'Queens','Long Island City/Queens Plaza','Boro Zone'),
(147,'Bronx','Longwood','Boro Zone'),
(148,'Manhattan','Lower East Side','Yellow Zone'),
(149,'Brooklyn','Madison','Boro Zone'),
(150,'Brooklyn','Manhattan Beach','Boro Zone'),
(151,'Manhattan','Manhattan Valley','Yellow Zone'),
(152,'Manhattan','Manhattanville','Boro Zone'),
(153,'Manhattan','Marble Hill','Boro Zone'),
(154,'Brooklyn','Marine Park/Floyd Bennett Field','Boro Zone'),
(155,'Brooklyn','Marine Park/Mill Basin','Boro Zone'),
(156,'Staten Island','Mariners Harbor','Boro Zone'),
(157,'Queens','Maspeth','Boro Zone'),
(158,'Manhattan','Meatpacking/West Village West','Yellow Zone'),
(159,'Bronx','Melrose South','Boro Zone'),
(160,'Queens','Middle Village','Boro Zone'),
(161,'Manhattan','Midtown Center','Yellow Zone'),
(162,'Manhattan','Midtown East','Yellow Zone'),
(163,'Manhattan','Midtown North','Yellow Zone'),
(164,'Manhattan','Midtown South','Yellow Zone'),
(165,'Brooklyn','Midwood','Boro Zone'),
(166,'Manhattan','Morningside Heights','Boro Zone'),
(167,'Bronx','Morrisania/Melrose','Boro Zone'),
(168,'Bronx','Mott Haven/Port Morris','Boro Zone'),
(169,'Bronx','Mount Hope','Boro Zone'),
(170,'Manhattan','Murray Hill','Yellow Zone'),
(171,'Queens','Murray Hill-Queens','Boro Zone'),
(172,'Staten Island','New Dorp/Midland Beach','Boro Zone'),
(173,'Queens','North Corona','Boro Zone'),
(174,'Bronx','Norwood','Boro Zone'),
(175,'Queens','Oakland Gardens','Boro Zone'),
(176,'Staten Island','Oakwood','Boro Zone'),
(177,'Brooklyn','Ocean Hill','Boro Zone'),
(178,'Brooklyn','Ocean Parkway South','Boro Zone'),
(179,'Queens','Old Astoria','Boro Zone'),
(180,'Queens','Ozone Park','Boro Zone'),
(181,'Brooklyn','Park Slope','Boro Zone'),
(182,'Bronx','Parkchester','Boro Zone'),
(183,'Bronx','Pelham Bay','Boro Zone'),
(184,'Bronx','Pelham Bay Park','Boro Zone'),
(185,'Bronx','Pelham Parkway','Boro Zone'),
(186,'Manhattan','Penn Station/Madison Sq West','Yellow Zone'),
(187,'Staten Island','Port Richmond','Boro Zone'),
(188,'Brooklyn','Prospect-Lefferts Gardens','Boro Zone'),
(189,'Brooklyn','Prospect Heights','Boro Zone'),
(190,'Brooklyn','Prospect Park','Boro Zone'),
(191,'Queens','Queens Village','Boro Zone'),
(192,'Queens','Queensboro Hill','Boro Zone'),
(193,'Queens','Queensbridge/Ravenswood','Boro Zone'),
(194,'Manhattan','Randalls Island','Yellow Zone'),
(195,'Brooklyn','Red Hook','Boro Zone'),
(196,'Queens','Rego Park','Boro Zone'),
(197,'Queens','Richmond Hill','Boro Zone'),
(198,'Queens','Ridgewood','Boro Zone'),
(199,'Bronx','Rikers Island','Boro Zone'),
(200,'Bronx','Riverdale/North Riverdale/Fieldston','Boro Zone'),
(201,'Queens','Rockaway Park','Boro Zone'),
(202,'Manhattan','Roosevelt Island','Boro Zone'),
(203,'Queens','Rosedale','Boro Zone'),
(204,'Staten Island','Rossville/Woodrow','Boro Zone'),
(205,'Queens','Saint Albans','Boro Zone'),
(206,'Staten Island','Saint George/New Brighton','Boro Zone'),
(207,'Queens','Saint Michaels Cemetery/Woodside','Boro Zone'),
(208,'Bronx','Schuylerville/Edgewater Park','Boro Zone'),
(209,'Manhattan','Seaport','Yellow Zone'),
(210,'Brooklyn','Sheepshead Bay','Boro Zone'),
(211,'Manhattan','SoHo','Yellow Zone'),
(212,'Bronx','Soundview/Bruckner','Boro Zone'),
(213,'Bronx','Soundview/Castle Hill','Boro Zone'),
(214,'Staten Island','South Beach/Dongan Hills','Boro Zone'),
(215,'Queens','South Jamaica','Boro Zone'),
(216,'Queens','South Ozone Park','Boro Zone'),
(217,'Brooklyn','South Williamsburg','Boro Zone'),
(218,'Queens','Springfield Gardens North','Boro Zone'),
(219,'Queens','Springfield Gardens South','Boro Zone'),
(220,'Bronx','Spuyten Duyvil/Kingsbridge','Boro Zone'),
(221,'Staten Island','Stapleton','Boro Zone'),
(222,'Brooklyn','Starrett City','Boro Zone'),
(223,'Queens','Steinway','Boro Zone'),
(224,'Manhattan','Stuy Town/Peter Cooper Village','Yellow Zone'),
(225,'Brooklyn','Stuyvesant Heights','Boro Zone'),
(226,'Queens','Sunnyside','Boro Zone'),
(227,'Brooklyn','Sunset Park East','Boro Zone'),
(228,'Brooklyn','Sunset Park West','Boro Zone'),
(229,'Manhattan','Sutton Place/Turtle Bay North','Yellow Zone'),
(230,'Manhattan','Times Sq/Theatre District','Yellow Zone'),
(231,'Manhattan','TriBeCa/Civic Center','Yellow Zone'),
(232,'Manhattan','Two Bridges/Seward Park','Yellow Zone'),
(233,'Manhattan','UN/Turtle Bay South','Yellow Zone'),
(234,'Manhattan','Union Sq','Yellow Zone'),
(235,'Bronx','University Heights/Morris Heights','Boro Zone'),
(236,'Manhattan','Upper East Side North','Yellow Zone'),
(237,'Manhattan','Upper East Side South','Yellow Zone'),
(238,'Manhattan','Upper West Side North','Yellow Zone'),
(239,'Manhattan','Upper West Side South','Yellow Zone'),
(240,'Bronx','Van Cortlandt Park','Boro Zone'),
(241,'Bronx','Van Cortlandt Village','Boro Zone'),
(242,'Bronx','Van Nest/Morris Park','Boro Zone'),
(243,'Manhattan','Washington Heights North','Boro Zone'),
(244,'Manhattan','Washington Heights South','Boro Zone'),
(245,'Staten Island','West Brighton','Boro Zone'),
(246,'Manhattan','West Chelsea/Hudson Yards','Yellow Zone'),
(247,'Bronx','West Concourse','Boro Zone'),
(248,'Bronx','West Farms/Bronx River','Boro Zone'),
(249,'Manhattan','West Village','Yellow Zone'),
(250,'Bronx','Westchester Village/Unionport','Boro Zone'),
(251,'Staten Island','Westerleigh','Boro Zone'),
(252,'Queens','Whitestone','Boro Zone'),
(253,'Queens','Willets Point','Boro Zone'),
(254,'Bronx','Williamsbridge/Olinville','Boro Zone'),
(255,'Brooklyn','Williamsburg (North Side)','Boro Zone'),
(256,'Brooklyn','Williamsburg (South Side)','Boro Zone'),
(257,'Brooklyn','Windsor Terrace','Boro Zone'),
(258,'Queens','Woodhaven','Boro Zone'),
(259,'Bronx','Woodlawn/Wakefield','Boro Zone'),
(260,'Queens','Woodside','Boro Zone'),
(261,'Manhattan','World Trade Center','Yellow Zone'),
(262,'Manhattan','Yorkville East','Yellow Zone'),
(263,'Manhattan','Yorkville West','Yellow Zone'),
(264,'Unknown','N/A','N/A'),
(265,'N/A','Outside of NYC','N/A')
ON CONFLICT (location_id) DO UPDATE SET -- Handle potential duplicates in source data
  borough = EXCLUDED.borough,
  zone = EXCLUDED.zone,
  service_zone = EXCLUDED.service_zone;

-- Update the main dim_location table from the staging table
UPDATE dim_location dl
SET
    borough = sl.borough,
    zone = sl.zone,
    service_zone = sl.service_zone
FROM staging_locations sl
WHERE dl.location_id = sl.location_id;

-- Step 2: Enrich other dimension tables

-- Update dim_vendor
UPDATE dim_vendor SET vendor_name =
    CASE vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'Curb Mobility, LLC'
        WHEN 6 THEN 'Myle Technologies Inc'
        WHEN 7 THEN 'Helix'
        ELSE 'Unknown'
    END
WHERE vendor_name IS NULL; -- Only update if not already populated

-- Update dim_rate_code
UPDATE dim_rate_code SET rate_code_name =
    CASE rate_code_id
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        WHEN 99 THEN 'Null/unknown' -- Assuming 99 maps from source
        ELSE 'Other'
    END
WHERE rate_code_name IS NULL;

-- Update dim_payment_type
UPDATE dim_payment_type SET payment_type_name =
    CASE payment_type_id
        WHEN 0 THEN 'Flex Fare trip' -- From description
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END
WHERE payment_type_name IS NULL;

ANALYZE dim_vendor;
ANALYZE dim_rate_code;
ANALYZE dim_payment_type;
ANALYZE dim_location;
ANALYZE dim_date;
ANALYZE fact_trips;
