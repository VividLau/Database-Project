# Database-Project . 
cd to directory where datasets are stored at.   
Make sure that your postgresql DB has superuser ***postgres***.

Run command line: psql -U postgres -f setup.sql   
Run command line: psql -U postgres -d safty -f data.sql
Run command line: psql -U postgres -d safty
Run sql commnd on psql shell:
\COPY motor FROM '/Users/weiliu/Desktop/DB PROJECT/Motor.csv' DELIMITER ',' CSV HEADER;
\COPY complaint FROM '/Users/weiliu/Desktop/DB PROJECT/Complaint.csv' DELIMITER ',' CSV HEADER;
ALTER TABLE complaint ALTER COLUMN fr_dt TYPE DATE using to_date(fr_dt, 'MM/DD/YYYY');
ALTER TABLE complaint ALTER COLUMN to_dt TYPE DATE using to_date(to_dt, 'MM/DD/YYYY');
ALTER TABLE complaint ALTER COLUMN rpt_dt TYPE DATE using to_date(rpt_dt, 'MM/DD/YYYY');
ALTER TABLE motor ALTER COLUMN _date TYPE DATE using to_date(_date, 'MM/DD/YYYY');

DELETE FROM complaint WHERE fr_dt >= '2006-01-01' and fr_dt < '2012-01-01';
ALTER TABLE complaint DROP x_coor, DROP y_coor, DROP hadevelop, DROP juris, DROP lat_long, DROP addr;
ALTER TABLE motor ALTER COLUMN _date TYPE DATE using to_date(_date, 'MM/DD/YYYY');

CREATE TABLE offense_cd ( ky_cd INT, ofns_desc VARCHAR(80) );
CREATE TABLE internal_cd ( pd_cd INT, pd_desc VARCHAR(80) );
INSERT INTO internal_code select distinct pd_cd, pd_desc from complaint where pd_desc is not NULL;
INSERT INTO offense_code select distinct ky_cd, ofns_desc from complaint where ofns_desc is not NULL;

ALTER TABLE complaint DROP ofns_desc, DROP pd_desc;

copy (select lat, long, uni_key from motor) TO '/tmp/motor_locate.csv' DELIMITER ',' CSV HEADER;
copy (select lat, long, cmplnt_num from complaint) TO '/tmp/complaint_locate.csv' DELIMITER ',' CSV HEADER;



***For MongoDB***
make sure that the files you want to import are in the *bin* directory of mongodb.
connect to mongoDB.
**command on terminal:**
./mongoimport -d safty -c complaint --type csv --file complaint_locate.csv --headerline
./mongoimport -d safty -c motor --type csv --file motor_locate.csv --headerline