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

int main(void)
{
	int clear_up_flag = 1;
	sensor_ts_t ts1, ts2, ts3, ts4;
	
	//=============================================//
	MYSQL *conn = init_connection(clear_up_flag);
	
	//=============================================//
	ts1 = (sensor_ts_t)time(NULL);
	insert_sensor(conn, 11, 20.02, ts1);
	sleep(1);
	ts2 = (sensor_ts_t)time(NULL);
	insert_sensor(conn, 22, 11.02, ts2);
	sleep(1);
	ts3 = (sensor_ts_t)time(NULL);
	insert_sensor(conn, 33, 11.02, ts3);
	sleep(1);
	ts4 = (sensor_ts_t)time(NULL);
	insert_sensor(conn, 44, 22.02, ts4);
	
	//=============================================//
	printf("----- Test function: find_sensor_all -----\n");
	MYSQL_RES *result = find_sensor_all(conn);
	if(result == NULL) {
		fprintf(stderr, "Null result!\n");
	}	
	print_result(result);	
	int size = get_result_size(result);  	
  	printf("Result size: %d\n", size);
  	free_sensor_data(result);
  	//=============================================//
	printf("----- Test function: find_sensor_by_value -----\n");
  	result = find_sensor_by_value(conn, 11.02);
  	if(result == NULL) {
		fprintf(stderr, "Null result!\n");
	}	
	print_result(result);
  	size = get_result_size(result);  	
  	printf("Result size: %d\n", size); 
  	free_sensor_data(result);
  	//=============================================//
	printf("----- Test function: find_sensor_by_timestamp -----\n");
  	result = find_sensor_by_timestamp(conn, ts3);
  	if(result == NULL) {
		fprintf(stderr, "Null result!\n");
	}	
	print_result(result);
  	size = get_result_size(result);  	
  	printf("Result size: %d\n", size); 
  	free_sensor_data(result);
  	//=============================================//
  	printf("----- Test function: find_sensor_exceed_value -----\n");
  	result = find_sensor_exceed_value(conn, 11.5);
  	if(result == NULL) {
		fprintf(stderr, "Null result!\n");
	}	
	print_result(result);
  	size = get_result_size(result);  	
  	printf("Result size: %d\n", size); 
  	free_sensor_data(result);
  	  	  	
  	//=============================================//
	printf("----- Test function: insert_sensor_from_file -----\n");
  	FILE *fp;
  	fp = fopen( DATA_FILE, "r" );  	
  	int status = insert_sensor_from_file(conn, fp);  	
  	if(status == -1) {
		fprintf(stderr, "Insert data failed!\n");
	}	
	result = find_sensor_all(conn);
	
	print_result(result);
  	size = get_result_size(result);  	
  	printf("Result size: %d\n", size);  	  	
  	free_sensor_data(result);
  	
  	//=============================================//
	printf("----- Test function: find_sensor_later_timestamp -----\n");
  	result = find_sensor_later_timestamp(conn, ts2);
  	if(result == NULL) {
		fprintf(stderr, "Null result!\n");
	}	
	print_result(result);
  	size = get_result_size(result);  	
  	printf("Result size: %d\n", size);  
  	free_sensor_data(result);
  	
  	//=============================================//
	disconnect(conn);
	return 0;
}

