#ifndef MQTT_CC_H
#define MQTT_CC_H

#include <sqlite3.h>

struct mqttcc{
    char* incoming_topic; 
    int incoming_max_latencies[10];
    double incoming_accuracy[10];
    double incoming_frequencies[10];
    char incoming_tasks[10][20];
    double incoming_energy [10];
    //nico below
    int incoming_lat_qos;
    char* incoming_sub_clientid;
    bool latChange;
    bool incoming_publishing;
};



struct mqttcc_db{
    const char* db_path;
    sqlite3 *db;
    sqlite3_stmt *insert_new_topic; // INSERT INTO subscriptions (topic, latency_req, max_allowed_latency) VALUES (?1, ?2, ?3)
    sqlite3_stmt *update_latency_req_max_allowed; // UPDATE subscriptions SET latency_req = ?1, max_allowed_latency = ?2 WHERE topic = ?3
    sqlite3_stmt *insert_topic; // SELECT * FROM subscriptions WHERE topic = ?1
    sqlite3_stmt *find_whatever;
    sqlite3_stmt *insert_subscribers;
    sqlite3_stmt *insert_into_publishers;
};

#endif