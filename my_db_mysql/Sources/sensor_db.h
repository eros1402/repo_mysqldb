#ifndef _SENSOR_DB_H_
#define _SENSOR_DB_H_

#include <mysql.h>
#include <time.h>
#include "config.h"

typedef MYSQL *mysql_conn_pt;
typedef MYSQL_RES *mysql_res_pt;

typedef uint16_t item_id_t;
typedef double   item_value_t;
typedef time_t 	 item_timestamp_t;         // UTC timestamp as returned by time()

typedef struct{
	item_id_t id;
	item_value_t value;
	item_timestamp_t ts;
} mysql_item_t, * mysql_item_pt;

/*
 * Make a connection to MySQL database with host name, user name, password and database name
 * return the connection for success, NULL if an error occurs
 */
mysql_conn_pt connect_mysql_db(const char *host, const char *user, const char *passwd, const char *dbName);

/*
 * Disconnect MySQL database
 */
void disconnect_mysql_db(mysql_conn_pt con);

/*
 * Reconnect to MySQL database
 * return 0 if database is connected successfully, 1 if there is an error
 */
int reconnect_mysql_db(mysql_conn_pt con);

/*
 * Checks whether the connection to the server is working
 * return zero for success, and 1 if an error occurs
 */
int check_mysql_connection(mysql_conn_pt con);

/*
 * Make a connection to MySQL database with host name, user name, password
 * Create database with dbName
 * return 0 if database is created successfully, 1 if there is an error
 */
int create_mysql_db(mysql_conn_pt con, const char *host, const char *user, const char *passwd, const char *dbName);

/*
 * Create a table named 'tableName' if the table does not exist
 * Table should have an 'id' column as index which is primary key and auto increment
 * one column of 'itemName' id, one column of  'itemName' value and one column of 'itemName' timestamp
 * If the table existed, clear up the existing data if dropTableIfExists flag is set to 1
 * return 0 if table is created successfully, 1 if there is an error
 */
int create_mysql_table(mysql_conn_pt con, int dropTableIfExists, const char *tableName, const char *itemName = "Sensor");

/*
 * Checks whether the table name is existing in the database
 * return zero if table is already available, and 1 if unavailable or error
 */
int check_mysql_table(mysql_conn_pt con, const char *tableName);

/*
 * Write an INSERT query to insert a single item measurement
 * return zero for success, and 1 if an error occurs
 */
int insert_mysql_item(mysql_conn_pt con, const char *tableName, item_id_t id, item_value_t value, item_timestamp_t ts) ;

/*
 * Write an INSERT query to insert all item measurements existed in a data file
 * return zero for success, and non-zero if an error occurs
 */
int insert_mysql_item_from_file(mysql_conn_pt con, FILE *data_file, const char *tableName);

////////// OLD IMPLEMENTATION ///////////////////////////
/*
 * Make a connection to MySQL database
 * Create a table named 'yourname' if the table does not exist
 * If the table existed, clear up the existing data if clear_up_flag is set to 1
 * return the connection for success, NULL if an error occurs
 */
mysql_conn_pt init_connection(int clear_up_flag);

/*
 * Reconnect to MySQL database
 */
int reconnect(mysql_conn_pt conn);

/*
 * Disconnect MySQL database
 */
void disconnect(mysql_conn_pt conn);

/*
 * Checks whether the connection to the server is working
 * return zero for success, and non-zero if an error occurs
 */
int check_connection(mysql_conn_pt conn);

/*
 * Write an INSERT query to insert a single sensor measurement
 * return zero for success, and non-zero if an error occurs
 */

int insert_sensor(mysql_conn_pt *conn, sensor_id_t id, sensor_value_t value, sensor_ts_t ts);

/*
 * Write an INSERT query to insert all sensor measurements existed in the file pointed by sensor_data
 * return zero for success, and non-zero if an error occurs
 */

int insert_sensor_from_file(mysql_conn_pt conn, FILE *sensor_data_file);

/*
 * Write a SELECT query to return all sensor measurements existed in the table
 * return MYSQL_RES with all the results
 */

mysql_res_pt find_sensor_all(mysql_conn_pt conn);

/*
 * Write a SELECT query to return all sensor measurements containing 'value_t'
 * return MYSQL_RES with all the results
 */

mysql_res_pt find_sensor_by_value(mysql_conn_pt conn, sensor_value_t value_t);

/*
 * Write a SELECT query to return all sensor measurement that its value exceeds 'value_t'
 * return MYSQL_RES with all the results
 */
mysql_res_pt find_sensor_exceed_value(mysql_conn_pt conn, sensor_value_t value_t);

/*
 * Write a SELECT query to return all sensor measurement containing timestamp 'ts_t'
 * return MYSQL_RES with all the results
 */
mysql_res_pt find_sensor_by_timestamp(mysql_conn_pt conn, sensor_ts_t ts_t);

/*
 * Write a SELECT query to return all sensor measurement recorded later than timestamp 'ts_t'
 * return MYSQL_RES with all the results
 */
mysql_res_pt find_sensor_later_timestamp(mysql_conn_pt conn, sensor_ts_t ts_t);

/*
 * Return the number of records contained in the result
 */
int get_result_size(mysql_res_pt result);

/*
 * Print all the records contained in the result
 */
void print_result(mysql_res_pt result);

/*
 * Free the records contained in the result
 */
void free_sensor_data(mysql_res_pt result);



#endif /* _SENSOR_DB_H_ */

