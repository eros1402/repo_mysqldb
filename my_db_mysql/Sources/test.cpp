//============================================================================
// Name        : test.cpp
// Author      : Pham Hoang Chi
// Version     :
// Copyright   : Copyright from Pham Hoang Chi
// Description : Test mylist.cpp
//============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>

#include "sensor_db.h"

#define DATA_FILE 	"sensor_data"
#define DB_SERVER_NAME "localhost"
#define USER_NAME "root"
#define PASSWORD "7777"
#define DB_NAME "testdb"

void test_create_mysql_db() {
	
	mysql_conn_pt con = NULL;
	if(create_mysql_db(con, DB_SERVER_NAME, USER_NAME, PASSWORD, "MY_DB")) {
		printf("DB not created!\n");
	}
	else printf("DB created\n");

	disconnect(con);
}

void test_create_mysql_table() {
	mysql_conn_pt con = connect_mysql_db(DB_SERVER_NAME, USER_NAME, PASSWORD, "MY_DB");
	if(con == NULL) {
		printf("Error in connection!\n");
	}
	else{
		printf("Connect successfully!\n");
		printf("Host %s - User %s - pw %s -db %s\n", con->host, con->user, con->passwd, con->db);
	}

	if(create_mysql_table(con, 0, "my_table2"))
		printf("Could not create table 'my_table2'\n");
	else
		printf("Create table 'my_table2'successfully\n");

	if(create_mysql_table(con, 1, "my_table2", "ss"))
		printf("Could not create table 'my_table2'\n");
	else
		printf("Create table 'my_table2'successfully\n");
	disconnect(con);
}

void test_insert_mysql_item() {
	item_timestamp_t ts1 = 0, ts2 = 0, ts3 = 0, ts4 = 0;
	mysql_conn_pt con = connect_mysql_db(DB_SERVER_NAME, USER_NAME, PASSWORD, "MY_DB");
	if(con == NULL) {
		printf("Error in connection!\n");
	}
	else{
		printf("Connect successfully!\n");
		printf("Host %s - User %s - pw %s -db %s\n", con->host, con->user, con->passwd, con->db);
	}
	printf("ts1 %ld sec\n", (long)ts1);
	//=============================================//
	ts1 = (item_timestamp_t)time(NULL);
	insert_mysql_item(con, "my_table", 11, 20.02, ts1);
	sleep(1);
	ts2 = (item_timestamp_t)time(NULL);
	insert_mysql_item(con, "my_table", 22, 11.02, ts2);
	sleep(1);
	ts3 = (item_timestamp_t)time(NULL);
	insert_mysql_item(con, "my_table", 33, 11.02, ts3);
	sleep(1);
	ts4 = (item_timestamp_t)time(NULL);
	insert_mysql_item(con, "my_table", 44, 22.02, ts4);

	disconnect_mysql_db(con);
}

//void test_1()
//{
//	int clear_up_flag = 1;
//	sensor_ts_t ts1, ts2, ts3, ts4;
//
//	//=============================================//
//	MYSQL *conn = init_connection(clear_up_flag);
//
//	//=============================================//
//	ts1 = (sensor_ts_t)time(NULL);
//	insert_sensor(conn, 11, 20.02, ts1);
//	sleep(1);
//	ts2 = (sensor_ts_t)time(NULL);
//	insert_sensor(conn, 22, 11.02, ts2);
//	sleep(1);
//	ts3 = (sensor_ts_t)time(NULL);
//	insert_sensor(conn, 33, 11.02, ts3);
//	sleep(1);
//	ts4 = (sensor_ts_t)time(NULL);
//	insert_sensor(conn, 44, 22.02, ts4);
//
//	//=============================================//
//	printf("----- Test function: find_sensor_all -----\n");
//	MYSQL_RES *result = find_sensor_all(conn);
//	if(result == NULL) {
//		fprintf(stderr, "Null result!\n");
//	}
//	print_result(result);
//	int size = get_result_size(result);
//  	printf("Result size: %d\n", size);
//  	free_sensor_data(result);
//  	//=============================================//
//	printf("----- Test function: find_sensor_by_value -----\n");
//  	result = find_sensor_by_value(conn, 11.02);
//  	if(result == NULL) {
//		fprintf(stderr, "Null result!\n");
//	}
//	print_result(result);
//  	size = get_result_size(result);
//  	printf("Result size: %d\n", size);
//  	free_sensor_data(result);
//  	//=============================================//
//	printf("----- Test function: find_sensor_by_timestamp -----\n");
//  	result = find_sensor_by_timestamp(conn, ts3);
//  	if(result == NULL) {
//		fprintf(stderr, "Null result!\n");
//	}
//	print_result(result);
//  	size = get_result_size(result);
//  	printf("Result size: %d\n", size);
//  	free_sensor_data(result);
//  	//=============================================//
//  	printf("----- Test function: find_sensor_exceed_value -----\n");
//  	result = find_sensor_exceed_value(conn, 11.5);
//  	if(result == NULL) {
//		fprintf(stderr, "Null result!\n");
//	}
//	print_result(result);
//  	size = get_result_size(result);
//  	printf("Result size: %d\n", size);
//  	free_sensor_data(result);
//
////  	//=============================================//
////	printf("----- Test function: insert_sensor_from_file -----\n");
////  	FILE *fp;
////  	fp = fopen( DATA_FILE, "r" );
////  	int status = insert_sensor_from_file(conn, fp);
////  	if(status == -1) {
////		fprintf(stderr, "Insert data failed!\n");
////	}
////	result = find_sensor_all(conn);
////
////	print_result(result);
////  	size = get_result_size(result);
////  	printf("Result size: %d\n", size);
////  	free_sensor_data(result);
////
////  	//=============================================//
////	printf("----- Test function: find_sensor_later_timestamp -----\n");
////  	result = find_sensor_later_timestamp(conn, ts2);
////  	if(result == NULL) {
////		fprintf(stderr, "Null result!\n");
////	}
////	print_result(result);
////  	size = get_result_size(result);
////  	printf("Result size: %d\n", size);
////  	free_sensor_data(result);
//
//  	//=============================================//
//	disconnect(conn);
//}

int main(void)
{
	//============
//	test_create_mysql_db();

//	test_create_mysql_table();

	test_insert_mysql_item();

	return 0;
}

