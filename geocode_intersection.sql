-- prepare tables --
DROP TABLE IF EXISTS intersection_table;
CREATE TABLE intersection_table(
row_seq varchar(255), 
street_1 varchar(255),
street_2 varchar(255),
STATE varchar(20),
city varchar(255),
zip varchar(255) -- some input could be irregular or with leading 0, use string to be safe
);
-- aws version. modify this in debug
COPY intersection_table FROM :input_file WITH DELIMITER '|' NULL 'NA' CSV HEADER; 
-- pc version. 
-- COPY intersection_table FROM 'e:\\Data\\intersection_sample.csv' WITH DELIMITER '|' NULL 'NA' CSV HEADER; 
-- update table, add key, indexing --
ALTER TABLE intersection_table
	ADD addid serial NOT NULL PRIMARY KEY,
	ADD rating integer, -- -1 for no match, -2 for exception of geocode
	ADD lon numeric,
	ADD lat numeric,
	ADD output_address text,
	ADD geomout geometry,	-- a point geometry in NAD 83 long lat. 
	ADD tabblock_id varchar(20), -- sometimes there are value over 15.
	ADD state_fips text,
	ADD county_fips text,
	ADD tractid text;

-- to make the row count estimate more accurate. not really needed
-- ANALYZE intersection_table;

--<< geocode function --
CREATE OR REPLACE FUNCTION geocode_intersection_sample(sample_size integer) 
	RETURNS void AS $$
DECLARE OUTPUT intersection_table%ROWTYPE;
BEGIN
UPDATE intersection_table
  SET (	rating, output_address, lon, lat, geomout)
	= (	COALESCE((g.geo).rating,-1), 
		pprint_addy((g.geo).addy),
	   	ST_X((g.geo).geomout)::numeric(8,5), 
	   	ST_Y((g.geo).geomout)::numeric(8,5), -- using 6 cause problem in census block mapping.
	   	(g.geo).geomout
	  )
FROM (SELECT addid
		FROM intersection_table
		WHERE rating IS NULL
		ORDER BY addid LIMIT sample_size
	 ) AS a
	LEFT JOIN (SELECT sample.addid, 
					  geocode_intersection(
					  	sample.street_1, 
					  	sample.street_2,
					  	sample.STATE, 
					  	sample.city, 
					  	sample.zip,
					  	1
			  ) AS geo
		FROM (SELECT *
				FROM intersection_table WHERE rating IS NULL
				ORDER BY addid LIMIT sample_size
			 ) AS sample
		) AS g ON a.addid = g.addid
WHERE a.addid = intersection_table.addid;

EXCEPTION
	WHEN OTHERS THEN
		SELECT * INTO OUTPUT 
			FROM intersection_table 
			WHERE rating IS NOT NULL ORDER BY addid DESC LIMIT 1;
		RAISE NOTICE 'Error in sample rows started from: %', OUTPUT;
		UPDATE intersection_table
			SET rating = -2
		FROM (SELECT addid
				FROM intersection_table 
				WHERE rating IS NULL
				ORDER BY addid LIMIT sample_size
			 ) AS sample
		WHERE sample.addid = intersection_table.addid;
END;
$$ LANGUAGE plpgsql;


-- census block function ---
CREATE OR REPLACE FUNCTION mapblock_sample(block_sample_size integer) 
	RETURNS void AS $$
DECLARE OUTPUT intersection_table%ROWTYPE;
BEGIN
UPDATE intersection_table
	SET (tabblock_id, state_fips, county_fips, tractid)
	  = (COALESCE(ab.tabblock_id,'FFFF'), 
		 substring(ab.tabblock_id FROM 1 FOR 2),
		 substring(ab.tabblock_id FROM 3 FOR 3),
		 substring(ab.tabblock_id FROM 1 FOR 11)
		)
	FROM (SELECT addid
			FROM intersection_table
			WHERE (geomout IS NOT NULL) AND (tabblock_id IS NULL)
			ORDER BY addid LIMIT block_sample_size
		 ) AS a
		LEFT JOIN (
			SELECT a.addid, b.tabblock_id
				FROM intersection_table AS a, tabblock AS b
				WHERE (geomout IS NOT NULL) AND (a.tabblock_id IS NULL)
					AND ST_Contains(b.the_geom, ST_SetSRID(ST_Point(a.lon, a.lat), 4269)) 
				ORDER BY addid LIMIT block_sample_size 
		 ) AS ab ON a.addid = ab.addid
WHERE ab.addid = intersection_table.addid;

EXCEPTION
	WHEN OTHERS THEN
		SELECT * INTO OUTPUT 
			FROM intersection_table 
			WHERE (geomout IS NOT NULL) AND (tabblock_id IS NULL)  
			ORDER BY addid DESC LIMIT 1;
		RAISE NOTICE '<census block> error in samples started from: %', OUTPUT;
		RAISE notice '-- !!! % % !!!--', SQLERRM, SQLSTATE;
		UPDATE intersection_table
			SET tabblock_id = 'EEEE'
		FROM (SELECT addid
				FROM intersection_table
				WHERE (geomout IS NOT NULL) AND (tabblock_id IS NULL)
				ORDER BY addid LIMIT block_sample_size
			 ) AS a
		WHERE a.addid = intersection_table.addid;
END;
$$ LANGUAGE plpgsql;
-- census block function >>--


--<< main control ---
DROP FUNCTION IF EXISTS geocode_table();
CREATE OR REPLACE FUNCTION geocode_table(
	OUT table_size integer, 
	OUT remaining_rows integer,
	OUT total_time interval(0), 
	OUT time_per_row interval(3)
	) AS $func$
DECLARE sample_size integer;
DECLARE block_sample_size integer;
DECLARE report_address_runs integer;-- sample size * runs, 100 * 1 = 100
DECLARE report_block_runs integer;-- block size * runs, 100 * 10 = 1000
DECLARE starting_time timestamp(0) WITH time ZONE;
DECLARE time_stamp timestamp(0) WITH time ZONE;
DECLARE time_passed interval(1);


BEGIN
	SELECT reltuples::bigint INTO table_size
					FROM   pg_class
					WHERE  oid = 'public.intersection_table'::regclass;
	starting_time := clock_timestamp();	
	time_stamp := clock_timestamp(); 
	RAISE notice '> % : Start on table of %', starting_time, table_size;
	RAISE notice '> time passed | address processed <<<< address left';
	sample_size := 1;
	block_sample_size := 10;
	report_address_runs := 100; -- modify this in debugging with small sample
	report_block_runs := 100; -- modify this in debug with small sample
	FOR i IN 1..(SELECT table_size / sample_size + 1) LOOP
		PERFORM geocode_intersection_sample(sample_size);
		IF i % report_address_runs = 0  THEN
			SELECT count(*) INTO remaining_rows 
				FROM intersection_table WHERE rating IS NULL;
			time_passed := clock_timestamp() - time_stamp;
			RAISE notice E'> %  |\t%\t<<<<\t%', 
				time_passed, i * sample_size, remaining_rows;
			time_stamp := clock_timestamp();
		END IF;		
	END LOOP;
	time_stamp := clock_timestamp();
	RAISE notice '==== start mapping census block ====';
	RAISE notice '# time passed | address to block <<<< address left';	
	FOR i IN 1..(SELECT table_size / block_sample_size + 1) LOOP
		PERFORM mapblock_sample(block_sample_size);
		IF i % report_block_runs = 0 THEN
			SELECT count(*) INTO remaining_rows 
				FROM intersection_table WHERE tabblock_id IS NULL;
			time_passed := clock_timestamp() - time_stamp;
			RAISE notice E'# %  |\t%\t<<<<\t%', 
				time_passed, i * block_sample_size, remaining_rows;
			time_stamp := clock_timestamp();
		END IF;		
	END LOOP;
	-- report table status. if have remaining, run again. may not need this.
	SELECT count(*) INTO remaining_rows 
		FROM intersection_table WHERE rating IS NULL;
	total_time := to_char(clock_timestamp() - starting_time, 'HH24:MI:SS'); 
	time_per_row := to_char(total_time / table_size, 'HH24:MI:SS.MS'); 
END
$func$ LANGUAGE plpgsql;
-- main control >>--

SELECT * FROM geocode_table();


