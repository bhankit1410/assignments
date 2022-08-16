DATABASE = "Tesla"
TABLE = "earthquakes"

CREATE_DATABASE = f'''CREATE DATABASE IF NOT EXISTS {DATABASE}'''
SELECT_DATABASE = f'''USE {DATABASE}'''


# The schema for table. Could have more tables with normalization.
CREATE_TABLE = """CREATE TABLE  IF NOT EXISTS earthquakes ( 
                             id int(11) NOT NULL AUTO_INCREMENT,
                             event_id VARCHAR(20) NOT NULL,
                             place VARCHAR(100),
                             mag FLOAT,
                             time bigint(20) NOT NULL,
                             longitude FLOAT,
                             latitude FLOAT,
                             depth FLOAT,
                             PRIMARY KEY (id)) """