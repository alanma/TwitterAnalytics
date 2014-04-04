CREATE TABLE twitts (uid VARCHAR(20) NOT NULL, time VARCHAR(40), tid VARCHAR(30) NOT NULL, PRIMARY KEY (uid, tid));
LOAD DATA LOCAL INFILE 'C:\\output.csv' INTO TABLE twitts FIELDS TERMINATED BY ',' enclosed by '"' LINES TERMINATED BY '\n' (uid, time, tid);
SELECT DATE_FORMAT('2014-1-14+00:00:00', '%a %b %d %T +0000 %Y');
CREATE INDEX index_twitts12 ON twitts(uid, time);