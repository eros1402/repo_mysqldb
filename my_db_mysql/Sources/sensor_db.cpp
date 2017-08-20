/*******************************************************************************
* FILENAME: sensor_db.c							       
*
* Version V1.1		
* Author: Pham Hoang Chi
*
* An implementation of Lab 7 assignment - System Software Course
* 
*******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <my_global.h>
#include "sensor_db.h"

//install lib: 
//Compile: gcc -Wall -g -D_GNU_SOURCE -o out test.c sensor_db.c $(mysql_config --cflags --libs)

#define DB_SERVER_NAME "localhost"
#define USER_NAME "root"
#define PASSWORD "7777"
#define DB_NAME "testdb"
#define TABLE_NAME "PhamHoangChi"

#ifdef DEBUG
	#define DEBUG_PRINT(...) 															\
	  do {					  															\
		printf("In %s - function %s at line %d: ", __FILE__, __func__, __LINE__);		\
		printf(__VA_ARGS__);															\
	  } while(0)
#else
	#define DEBUG_PRINT(...) 		\
		do {						\
			printf(__VA_ARGS__);	\
		} while(0)
#endif

#define CHECK_INVALID_VALUE(value, msg) 					\
		do {												\
			if(value != 0) {								\
				printf( "%s\n", msg );						\
				return 1;									\
			}												\
		} while (0)


#define CHECK_POINTER(p, msg) 								\
		do {												\
			if(p == NULL) {									\
				printf( "%s\n", msg );						\
				return 1;									\
			}												\
		} while (0)

#define MYSQL_QUERY_EXE(con, q, err_msg)						\
		do {													\
				if(mysql_query(con, q)) {						\
					printf("%s\n", err_msg);					\
					return 1;									\
				}												\
				if(q != NULL) free(q);							\
		} while (0)

void tstamp_to_tstring(sensor_ts_t ts, char *strtime) {
	strftime(strtime, 20, "%F %T",localtime(&ts));  	
}

////////// NEW IMPLEMENTATION ///////////////////////////

/*
 * Make a connection to MySQL database with host name, user name, password and database name
 * return the connection for success, NULL if an error occurs
 */
mysql_conn_pt connect_mysql_db(const char *host, const char *user, const char *passwd, const char *dbName)
{
	// init a mysql object
	mysql_conn_pt con = mysql_init(NULL);
	if(con == NULL) {
		printf( "Error %s: could not initialze mysql pointer!\n", __func__);
		return NULL;
	}

	// establish a connection to the host server
	if(mysql_real_connect(con, host, user, passwd, dbName, 0, NULL, 0) == NULL) {
		printf( "Error %s: could not make a connection to the host server!\n", __func__);
		return NULL;
	}

	return con;
}

/*
 * Disconnect MySQL database
 */
void disconnect_mysql_db(mysql_conn_pt con)
{
	mysql_close(con);
	con = NULL;
	mysql_library_end();
}

/*
 * Reconnect to MySQL database
 * return 0 if database is connected successfully, 1 if there is an error
 */
int reconnect_mysql_db(mysql_conn_pt con) {
	CHECK_POINTER(con, "Error reconnect_mysql_db(): invalid connection pointer!\n");

	// Enable Auto-reconnect option
	// mysql_options(conn, MYSQL_OPT_RECONNECT, &recon);
	if (mysql_real_connect(con, con->host, con->user, con->passwd, con->db, 0, NULL, 0) == NULL) {
		fprintf(stderr, "reconnect_mysql_db(): %s\n", mysql_error(con));
		return 1;
	}

	return 0;
}

/*
 * Checks whether the connection to the server is working
 * return zero for success, and 1 if an error occurs
 */
int check_mysql_connection(mysql_conn_pt con) {
	CHECK_POINTER(con, "Error check_mysql_connection(): invalid connection pointer!\n");
	if(mysql_ping(con)) return 1;

	return 0;
}

/*
 * Make a connection to MySQL database with host name, user name, password
 * Create database with dbName
 * return 0 if database is created successfully, 1 if there is an error
 */
int create_mysql_db(mysql_conn_pt con, const char *host, const char *user, const char *passwd, const char *dbName)
{
	// initialises a MYSQL object
	con = mysql_init(NULL);
	CHECK_POINTER(con, "Error create_mysql_db(): could not initialze mysql pointer!\n");

	// establish a connection to the host server
	CHECK_POINTER(mysql_real_connect(con, host, user, passwd, NULL, 0, NULL, 0), "Error create_mysql_db(): could not make a connection to the host server!\n");

	char *query;
	asprintf(&query, "CREATE DATABASE %s", dbName);
	MYSQL_QUERY_EXE(con, query, "Error create_mysql_db(): could not create database or database name already exists in the host server!");

	return 0;
}

/*
 * Checks whether the database name is existing in the host server
 * return zero if database is already available, and 1 if unavailable or error
 */
int check_mysql_db(mysql_conn_pt con, const char *dbName)
{
	CHECK_POINTER(con, "Error check_mysql_db(): invalid connection pointer!\n");


	return 0;
}

/*
 * Create a table named 'tableName' if the table does not exist
 * Table should have an 'id' column as index which is primary key and auto increment
 * one column of 'itemName' id, one column of  'itemName' value and one column of 'itemName' timestamp
 * If the table existed, clear up the existing data if dropTableIfExists flag is set to 1
 * return 0 if table is created successfully, 1 if there is an error
 */
int create_mysql_table(mysql_conn_pt con, int dropTableIfExists, const char *tableName, const char *itemName)
{
	CHECK_POINTER(con, "Error creat_mysql_table(): invalid connection pointer!\n");

	char *query;
	mysql_res_pt result;

	if (dropTableIfExists) {
		asprintf(&query, "DROP TABLE IF EXISTS %s", tableName);
		MYSQL_QUERY_EXE(con, query, "Error create_mysql_table(): could not drop the existed table in the host server!");
	}
	else {
		// Check if table is already existing
		asprintf(&query, "SHOW TABLES LIKE '%s'", tableName);
		MYSQL_QUERY_EXE(con, query, "Error create_mysql_table(): invalid showing table name!");
		result = mysql_store_result(con);
		unsigned int num_rows = mysql_num_rows(result);
		mysql_free_result(result);
		if(num_rows > 0) return 1;
	}

	asprintf(&query, "CREATE TABLE %s (id INT PRIMARY KEY AUTO_INCREMENT, %s_id INT, %s_value DECIMAL(4,2), timestamp TIMESTAMP)", tableName, itemName, itemName);
	MYSQL_QUERY_EXE(con, query, "Error create_mysql_table(): could not create the table!");

	return 0;
}

/*
 * Checks whether the table name is existing in the database
 * return zero if table is already available, and 1 if unavailable or error
 */
int check_mysql_table(mysql_conn_pt con, const char *tableName)
{
	CHECK_POINTER(con, "Error check_mysql_table(): invalid connection pointer!\n");

	char *query;
	MYSQL_RES *result;

	// Check if table is already existing
	asprintf(&query, "SHOW TABLES LIKE '%s'", tableName);
	MYSQL_QUERY_EXE(con, query, "Error check_mysql_table(): invalid showing table name!");
	result = mysql_store_result(con);
	unsigned int num_rows = mysql_num_rows(result);
	mysql_free_result(result);
	if(num_rows <= 0) return 1;

	return 0;
}

/*
 * Write an INSERT query to insert a single item measurement
 * return zero for success, and 1 if an error occurs
 */
int insert_mysql_item(mysql_conn_pt con, const char *tableName, item_id_t id, item_value_t value, item_timestamp_t ts)
{
	char *query;
	char strtime[20];

	CHECK_POINTER(con, "Error insert_mysql_item(): invalid connection pointer!\n");
	tstamp_to_tstring(ts, (char *)strtime);
	asprintf(&query, "INSERT INTO %s VALUES (null, %d, %g, '%s')", tableName, (int)id,(double)value, strtime);
	MYSQL_QUERY_EXE(con, query, "Error insert_mysql_item(): could not insert the item into the table!");

	return 0;
}

/*
 * Write an INSERT query to insert all item measurements existed in a data file
 * return zero for success, and non-zero if an error occurs
 */
int insert_mysql_item_from_file(mysql_conn_pt con, FILE *data_file, const char *tableName)
{
	item_id_t id;
	item_value_t value;
	item_timestamp_t ts;
	int fsize;
	int i, count, result;

	CHECK_POINTER(con, "Error insert_mysql_item_from_file(): invalid connection pointer!\n");

	CHECK_POINTER(data_file, "Error insert_mysql_item_from_file(): invalid data file pointer!\n");

	CHECK_INVALID_VALUE(check_mysql_table(con, tableName), "Error insert_mysql_item_from_file(): invalid table name!\n");

	// obtain file size:
	fseek (data_file , 0 , SEEK_END);
	fsize = ftell (data_file);
	rewind (data_file);
	count = fsize/(sizeof(uint16_t) + sizeof(double) + sizeof(time_t));

	if(count <= 0) {
		printf("Warning insert_mysql_item_from_file(): There is no data in table %s \n", tableName);
	}
	// Check each data package=<sensor ID><temperature><timestamp>
	for( i = 0; i < count; i++ ) {
		result = fread( &id, sizeof(item_id_t), 1, data_file );
		if ( result < 1 ) {
			fprintf(stderr, "Error insert_mysql_item_from_file(): File read failed\n"); return 1;
		}

		result = fread( &value, sizeof(item_value_t), 1, data_file );
		if ( result < 1 ) {
			fprintf(stderr, "Error insert_mysql_item_from_file(): File read failed\n"); return 1;
		}

		result = fread( &ts, sizeof(item_timestamp_t), 1, data_file );
		if ( result < 1 ) {
			fprintf(stderr, "Error insert_mysql_item_from_file(): File read failed\n"); return 1;
		}

		if(insert_mysql_item(con, tableName, id, value, ts)) {
			fprintf(stderr, "Error insert_mysql_item_from_file(): Insert to database failed\n");
			return 1;
		}
	}

	return 0;
}

////////// OLD IMPLEMENTATION ///////////////////////////
/*
 * Make a connection to MySQL database
 * Create a table named 'yourname' if the table does not exist
 * If the table existed, clear up the existing data if clear_up_flag is set to 1
 * return the connection for success, NULL if an error occurs
 */
MYSQL *init_connection(int clear_up_flag)
{
	if (mysql_library_init(0, NULL, NULL)) {
		fprintf(stderr, "could not initialize MySQL library\n");
		exit(1);
	}
	
	// initialises a MYSQL object	
	MYSQL *con = mysql_init(NULL);
	if(con == NULL) return NULL;
	char *query1, *query2;	
		
	// establish a connection to the database: server_name(studev.groept.be) - user_name(a13_syssoft) - pw(a13_syssoft) - database_name(a13_syssoft) 
	if (mysql_real_connect(con, DB_SERVER_NAME, USER_NAME, PASSWORD, DB_NAME, 0, NULL, 0) == NULL) {
		fprintf(stderr, "%s\n", mysql_error(con));
		return NULL;
	} 
		
	// Check clear_up_flag and then check if the table is already existing	
	if(clear_up_flag) {
		asprintf(&query1, "DROP TABLE IF EXISTS %s", TABLE_NAME);		
		if (mysql_query(con, query1)) {
			fprintf(stderr, "%s\n", mysql_error(con));			
		}

		// Create new table with columns: id (primary_key and auto_increment), sensor_id(INT), sensor_value DECIMAL(4,2), timestamp (TIMESTAMP)
		asprintf(&query2, "CREATE TABLE %s (id INT PRIMARY KEY AUTO_INCREMENT, sensor_id INT, sensor_value DECIMAL(4,2), timestamp TIMESTAMP)", TABLE_NAME);
		if (mysql_query(con, query2)) {      
			fprintf(stderr, "%s\n", mysql_error(con));
		}	
		
		free(query2);
	}	
		
	free(query1);		
	return con;
}

/*
 * Reconnect to MySQL database
 */
int reconnect(MYSQL *conn) {
	// my_bool recon = 1;
	if(conn == NULL) {
		fprintf(stderr, "%s\n", mysql_error(conn));
		return -1;
	} 
	// Enable Auto-reconnect option
	// mysql_options(conn, MYSQL_OPT_RECONNECT, &recon);
	if (mysql_real_connect(conn, DB_SERVER_NAME, USER_NAME, PASSWORD, DB_NAME, 0, NULL, 0) == NULL) {
		fprintf(stderr, "%s\n", mysql_error(conn));
		return -1;
	}
	
	return 1;
}

/*
 * Disconnect MySQL database
 */
void disconnect(MYSQL *conn) {
	mysql_close(conn);
	conn = NULL;
	mysql_library_end();
}

/*
 * Checks whether the connection to the server is working
 * return zero for success, and non-zero if an error occurs
 */
int check_connection(MYSQL *conn) {
	if(mysql_ping(conn)) return 1;
	else return 0;
} 

/*
 * Write an INSERT query to insert a single sensor measurement
 * return zero for success, and non-zero if an error occurs
 */

int insert_sensor(MYSQL *conn, sensor_id_t id, sensor_value_t value, sensor_ts_t ts) {
	char *query;	
	char strtime[20];  
	tstamp_to_tstring(ts, (char *)strtime);
	if(conn == NULL) return 1;
	
	asprintf(&query, "INSERT INTO %s(sensor_id, sensor_value, timestamp) VALUES (%d, %g, '%s')", TABLE_NAME, (int)id,(double)value, strtime);		
	if (mysql_query(conn, query)) {    
		fprintf(stderr, "%s\n", mysql_error(conn));  
		// if(conn == NULL) return -1;
		return 1;
	}
	free(query);
	return 0;
}

/*
 * Write an INSERT query to insert all sensor measurements existed in the file pointed by sensor_data
 * return zero for success, and non-zero if an error occurs
 */

int insert_sensor_from_file(MYSQL *conn, FILE *sensor_data_file) {
	sensor_id_t id;
	sensor_value_t value;
	sensor_ts_t ts;
	int fsize;
	int i, count, result;
	
	if(conn == NULL) {
		fprintf(stderr, "Error in connection\n");  	
		return -1;
	}
	// Check the file
	if ( sensor_data_file ==NULL) {
		fprintf(stderr, "Error: File is not opened\n");  	
		return -1;
	}	
	// obtain file size:
	fseek (sensor_data_file , 0 , SEEK_END);
	fsize = ftell (sensor_data_file);
	rewind (sensor_data_file);		
	count = fsize/(sizeof(uint16_t) + sizeof(double) + sizeof(time_t));	
	
	// Check each data package=<sensor ID><temperature><timestamp>
	for(i=0; i<count; i++) { 
		result = fread( &id, sizeof(sensor_id_t), 1, sensor_data_file );
		if ( result < 1 ) {
			fprintf(stderr, "Error: File read failed\n"); return -1;			
		}
		
		result = fread( &value, sizeof(sensor_value_t), 1, sensor_data_file );
		if ( result < 1 ) {
			fprintf(stderr, "Error: File read failed\n"); return -1;			
		}
		
		result = fread( &ts, sizeof(sensor_ts_t), 1, sensor_data_file );
		if ( result < 1 ) {
			fprintf(stderr, "Error: File read failed\n"); return -1;			
		}
		
		if(insert_sensor(conn, id, value, ts) == -1) {
			fprintf(stderr, "Error: Insert to database failed\n");
			return -1;
		}		
	}
	
	return 0;
}

/*
 * Write a SELECT query to return all sensor measurements existed in the table
 * return MYSQL_RES with all the results
 */

mysql_res_pt find_sensor_all(MYSQL *conn) {
	if(conn == NULL) return NULL;
	
	char *query;	
	asprintf(&query, "SELECT * FROM %s", TABLE_NAME);
	
	if (mysql_query(conn, query)) {    
		fprintf(stderr, "%s\n", mysql_error(conn));  		
	}	
	
	mysql_res_pt result = mysql_store_result(conn);
	if(result == NULL) {
		fprintf(stderr, "%s\n", mysql_error(conn));
	}	
	
	free(query);
	return result;		
}

/*
 * Write a SELECT query to return all sensor measurements containing 'value_t'
 * return MYSQL_RES with all the results
 */

mysql_res_pt find_sensor_by_value(MYSQL *conn, sensor_value_t value_t) {
	char *query;	
	asprintf(&query, "SELECT * FROM %s WHERE sensor_value=%g", TABLE_NAME, (double)value_t);
	
	if(mysql_query(conn, query)) {
		fprintf(stderr, "%s\n", mysql_error(conn));   
		if(conn == NULL) return NULL;
	}	
	mysql_res_pt result = mysql_store_result(conn);
	free(query);
	return result;			
}

/*
 * Write a SELECT query to return all sensor measurement that its value exceeds 'value_t'
 * return MYSQL_RES with all the results
 */
mysql_res_pt find_sensor_exceed_value(MYSQL *conn, sensor_value_t value_t) {
	char *query;	
	asprintf(&query, "SELECT * FROM %s WHERE sensor_value >= %g", TABLE_NAME, (double)value_t);
	
	if(mysql_query(conn, query)) {
		fprintf(stderr, "%s\n", mysql_error(conn));   
		if(conn == NULL) return NULL;
	}	
	
	mysql_res_pt result = mysql_store_result(conn);
	free(query);
	return result;			
}

/*
 * Write a SELECT query to return all sensor measurement containing timestamp 'ts_t'
 * return MYSQL_RES with all the results
 */
mysql_res_pt find_sensor_by_timestamp(MYSQL *conn, sensor_ts_t ts_t) {
	char *query;
	char strtime[20];  
	tstamp_to_tstring(ts_t, (char *)strtime);	
	asprintf(&query, "SELECT * FROM %s WHERE timestamp= '%s'", TABLE_NAME, strtime);
	
	if(mysql_query(conn, query)) {
		fprintf(stderr, "%s\n", mysql_error(conn));   
		if(conn == NULL) return NULL;
	}	
	
	mysql_res_pt result = mysql_store_result(conn);
	free(query);
	return result;			
}

/*
 * Write a SELECT query to return all sensor measurement recorded later than timestamp 'ts_t'
 * return MYSQL_RES with all the results
 */
mysql_res_pt find_sensor_later_timestamp(MYSQL *conn, sensor_ts_t ts_t) {
	char *query;
	char strtime[20];  
	tstamp_to_tstring(ts_t, (char *)strtime);	
	asprintf(&query, "SELECT * FROM %s WHERE timestamp >= '%s'", TABLE_NAME, strtime);
	
	if(mysql_query(conn, query)) {
		fprintf(stderr, "%s\n", mysql_error(conn));   
		if(conn == NULL) return NULL;
	}	
	
	mysql_res_pt result = mysql_store_result(conn);
	free(query);
	return result;			
}

/*
 * Return the number of records contained in the result
 */
int get_result_size(MYSQL_RES *result) {	
	return mysql_num_rows(result);
}

/*
 * Print all the records contained in the result
 */
void print_result(MYSQL_RES *result) {
	int num_fields = mysql_num_fields(result);
	MYSQL_ROW row;
	int i;
  
	while ((row = mysql_fetch_row(result))) { 
		for(i = 0; i < num_fields; i++) 
		{ 
			printf("%s ", row[i] ? row[i] : "NULL"); 
		} 
        printf("\n"); 
	}
}

/*
 * Free the records contained in the result
 */
void free_sensor_data(MYSQL_RES *result) {
	mysql_free_result(result);
}

