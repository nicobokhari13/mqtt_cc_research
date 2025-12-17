
#include "config.h"

#include <string.h>

#include "mosquitto_broker_internal.h"

void log_sub(char *sub){
    log__printf(NULL, MOSQ_LOG_DEBUG, "\t %s", sub);
}

bool has_tasks_qos(char *sub){
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t in has_lat_qos");
    char *tasksStr = "tasks";
    char* result = strstr(sub, tasksStr);
    if(result == NULL){
        //log__printf(NULL, MOSQ_LOG_DEBUG, "\t in has_lat_qos if statement");
        return false;
    }
    return true;
}

bool has_colon(char* sub){
    char * colon = ":";
    char* result = strstr(sub,colon);
    if (result == NULL){
        return false;
    }
    return true;
}
void printStmtResults(sqlite3_stmt *stmt){
    int rc;
    int i; 
    int columnNum = sqlite3_column_count(stmt);
    for (i = 0; i < columnNum; i++){
            log__printf(NULL, MOSQ_LOG_DEBUG, "%s = %s \n", sqlite3_column_name(stmt, i), sqlite3_column_text(stmt,i));
    }
    while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
        int columnNum = sqlite3_column_count(stmt);
        for (i = 0; i < columnNum; i++){
            log__printf(NULL, MOSQ_LOG_DEBUG, "%s = %s \n", sqlite3_column_name(stmt, i), sqlite3_column_text(stmt,i));
        }
        log__printf(NULL, MOSQ_LOG_DEBUG, "\n");
    }
}


void get_qos_metrics_helper_func(struct mosquitto *context, const char *key, const char* val)
{
    char* temp = strdup(key);
    if (!temp) {
        return;
    }

    char *value = strtok((char*)val, ",");
    int i = 0; // Index variable for arrays
    
    while (value != NULL) {
        if (strstr(temp, "tasks") != NULL) {
            strcpy(context->mqtt_cc.incoming_tasks[i], value);
        }
        else if (strstr(temp, "Max_Latency") != NULL) {
            context->mqtt_cc.incoming_max_latencies[i] = atoi(value);
        }
        else if (strstr(temp, "Accuracy") != NULL) {
            context->mqtt_cc.incoming_accuracy[i] = atof(value);
        }
        else if (strstr(temp, "Min_Frequency") != NULL) {
            context->mqtt_cc.incoming_frequencies[i] = atof(value);
        }
        else if (strstr(temp, "Energy") != NULL) {
            context->mqtt_cc.incoming_energy[i] = atof(value);
        }

        value = strtok(NULL, ","); // Get next value
        i++; // Move to next index
    }
    //sentinel value to terminate the strings.
    if (strstr(temp, "Max_Latency") != NULL) {
        context->mqtt_cc.incoming_max_latencies[i] = -1;
    }
    else if (strstr(temp, "Accuracy") != NULL) {
        context->mqtt_cc.incoming_accuracy[i] = -1;
    }
    else if (strstr(temp, "Min_Frequency") != NULL) {
        context->mqtt_cc.incoming_frequencies[i] = -1;
    }
    else if (strstr(temp,"Energy")!=NULL){
        context->mqtt_cc.incoming_energy[i]=-1;
    }
    else if (strstr(temp,"tasks")!=NULL){
        context->mqtt_cc.incoming_tasks[i][0]='\0';
    }
    free(temp);
}


void get_first_publish(struct mosquitto *context, struct mosquitto_msg_store * msg){
    char * myptr = (char*) msg->payload;
    cJSON *json = cJSON_Parse(myptr);
    if (json == NULL) {
        printf("Error parsing JSON\n");
        return 1;
    }

    // Extract values
    cJSON *mac_address = cJSON_GetObjectItem(json, "mac_address");
    cJSON *temperature = cJSON_GetObjectItem(json, "temperature");
    cJSON *humidity = cJSON_GetObjectItem(json, "humidity");
    cJSON *voltage = cJSON_GetObjectItem(json, "voltage");
    cJSON *capacity = cJSON_GetObjectItem(json, "capacity");

    // Extract nested "accuracy"
    cJSON *accuracy = cJSON_GetObjectItem(json, "accuracy");
    cJSON *accuracy_plus_minus = cJSON_GetObjectItem(accuracy, "+-");
    cJSON *accuracy_minus_plus = cJSON_GetObjectItem(accuracy, "-+");

    // Extract nested "tasks"
    cJSON *tasks = cJSON_GetObjectItem(json, "tasks");
    cJSON *task1 = cJSON_GetObjectItem(tasks, "Task 1");
    cJSON *task2 = cJSON_GetObjectItem(tasks, "Task 2");

    context->mqtt_cc.incoming_topic = msg->topic;
    context->mqtt_cc.incoming_max_latencies[0] = 0;
    context->mqtt_cc.incoming_max_latencies[1] = 0;
    context->mqtt_cc.incoming_accuracy[0] = accuracy->valuedouble;
    context->mqtt_cc.incoming_accuracy[1] = accuracy->valuedouble;
    context->mqtt_cc.incoming_frequencies[0]= 0;
    context->mqtt_cc.incoming_frequencies[1]= 0;
    strncpy(context->mqtt_cc.incoming_tasks[0], task1->valuestring,strlen(task1->valuestring));
    strncpy(context->mqtt_cc.incoming_tasks[1], task2->valuestring, strlen(task2->valuestring));
    context->mqtt_cc.incoming_energy[0]= voltage->valueint;
    context->mqtt_cc.incoming_energy[1]= voltage->valueint;


    cJSON_Delete(json);

}

void get_qos_metrics(struct mosquitto *context, const char *passed_insub) {
    char buffer[256];

    // Safe string copy
    snprintf(buffer, sizeof(buffer), "%s", passed_insub);

    int slash_count = 0;
    const char *second_slash = NULL;

    // Find the second '/'
    for (const char *p = buffer; *p != '\0'; p++) {
        if (*p == '/') {
            slash_count++;
            if (slash_count == 2) {
                second_slash = p; // Pointer to the second '/'
                break;
            }
        }
    }

    // Check if the second slash was found
    if (second_slash != NULL) {
        // Calculate the length of the substring before the second '/'
        size_t len = second_slash - buffer;

        // Allocate memory for the substring (+1 for null terminator)
        char *result = (char *)malloc(len + 1);
        if (!result) {
            fprintf(stderr, "Error: Memory allocation failed\n");
            return;
        }

        // Copy the substring before the second '/' into the allocated memory
        strncpy(result, buffer, len);
        result[len] = '\0'; // Null-terminate the string

        // Free the previously allocated memory (if any)
        if (context->mqtt_cc.incoming_topic) {
            free(context->mqtt_cc.incoming_topic);
        }

        // Assign the new substring to incoming_topic
        context->mqtt_cc.incoming_topic = result;

        // Process the remainder of the string (after the second '/')
            size_t remaining_length = strlen(second_slash + 1);
        memmove(buffer, second_slash + 1, remaining_length + 1); // +1 to include the null terminator

        // Tokenize the remainder of the string
        char *saveptr_for_semicolons;
        char *pair = strtok_r(buffer, ";", &saveptr_for_semicolons);
        while (pair != NULL) {
            char *saveptr_for_equals;
            char *key = strtok_r(pair, "=", &saveptr_for_equals);
            char *value = strtok_r(NULL, "=", &saveptr_for_equals);
            if (key && value) {
                get_qos_metrics_helper_func(context, key, value);
            } else {
                break; // Exit if key or value is missing
            }
            pair = strtok_r(NULL, ";", &saveptr_for_semicolons);
        }
    } else {
        printf("Second '/' not found.\n");
    }
}

char *concat_strings(char *str1, char *str2) {
    // Allocate memory for the concatenated string
    size_t len1 = strlen(str1);
    size_t len2 = strlen(str2);
    char *result = malloc(len1 + len2 + 1); // +1 for the null terminator
    if (result == NULL) {
        perror("Memory allocation failed");
        return NULL;
    }

    // Copy str1 and str2 into the result buffer
    strcpy(result, str1);
    strcat(result, str2);

    return result;
}

void prepare_DB() {
    char *err_msg = NULL;
    int rc;
    prototype_db.db_path = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db";
    // Open database connection /mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-mqtt-cc/mosquitto/db/database.db"
    rc = sqlite3_open(prototype_db.db_path, &prototype_db.db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        return;
    }

    // Create subscribers table
    const char *create_subscribers_table = 
    "CREATE TABLE IF NOT EXISTS Subscribers ("
    "DeviceMac TEXT PRIMARY KEY, Tasks TEXT, Min_Frequency TEXT, MaxAllowedLatency TEXT, Accuracy TEXT);";
    // Create devices table
    const char *create_publishers_table = 
     "CREATE TABLE IF NOT EXISTS Publishers ("
        "DeviceMac TEXT PRIMARY KEY, Tasks TEXT, Max_Frequency TEXT, Max_Latency TEXT, Accuracy TEXT, Energy TEXT);";
    // Create topics table
    const char *create_topics_table = 
"CREATE TABLE IF NOT EXISTS Topics ("
        "DeviceMac TEXT, TopicName TEXT, Publishing BOOLEAN, "
        "PRIMARY KEY(TopicName, Publishing));";

    rc = sqlite3_exec(prototype_db.db, create_subscribers_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create subscribers table: %s\n", err_msg);
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(prototype_db.db, create_publishers_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create devices table: %s\n", err_msg);
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(prototype_db.db, create_topics_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create topics table: %s\n", err_msg);
        sqlite3_free(err_msg);
    } 
    printf("Created Tables\n");

    const char* insert_topic_cmd = "INSERT INTO Topics (DeviceMac, TopicName, Publishing) VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(prototype_db.db, insert_topic_cmd, -1, &prototype_db.insert_topic, 0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 1: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
    const char* find_whatever_cmd = "SELECT * FROM Topics WHERE TopicName = ?;";
    rc = sqlite3_prepare_v2(prototype_db.db, find_whatever_cmd, -1, &prototype_db.find_whatever,0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 1: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
    const char* insert_subscribers_cmd = "INSERT INTO Subscribers (DeviceMac,Tasks,Min_Frequency,MaxAllowedLatency,Accuracy) VALUES (?, ?, ?, ?, ?);";
    rc = sqlite3_prepare_v2(prototype_db.db, insert_subscribers_cmd, -1, &prototype_db.insert_subscribers,0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 1: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
    const char* insert_into_publishers_cmd = "INSERT INTO Publishers (DeviceMac,Tasks, Max_Frequency, Max_Latency, Accuracy, Energy) VALUES (?,?,?,?,?,?);";
    rc = sqlite3_prepare_v2(prototype_db.db, insert_into_publishers_cmd, -1, &prototype_db.insert_into_publishers,0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 5: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
}

bool topic_search(struct mosquitto *context, char* sub){
    log__printf(NULL, MOSQ_LOG_DEBUG, "Entering topic_search");
    log__printf(NULL, MOSQ_LOG_DEBUG, sub);

    if (prototype_db.find_whatever == NULL) {
        log__printf(NULL, MOSQ_LOG_ERR, "Error: prototype_db.find_whatever is NULL");
        return false;
    }       
    int rc;
    if (!sub) {
        log__printf(NULL, MOSQ_LOG_ERR, "Error: Topic is NULL");
        return false;
    }
    
    sqlite3_reset(prototype_db.find_whatever);
    sqlite3_clear_bindings(prototype_db.find_whatever);

    // Bind the topic name to the prepared statement
    rc = sqlite3_bind_text(prototype_db.find_whatever, 1, sub, -1,SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Error binding topic to statement: %s", sqlite3_errmsg(prototype_db.db));
        return false;
    }
    
    // Execute the query
    rc = sqlite3_step(prototype_db.find_whatever);
    
    if (rc == SQLITE_ROW) {
        // The topic exists in the database
        log__printf(NULL, MOSQ_LOG_DEBUG, "Topic '%s' found in database.", sub);
        sqlite3_reset(prototype_db.find_whatever);
        return true;
    } else {
        // No matching topic found
        log__printf(NULL, MOSQ_LOG_DEBUG, "Topic '%s' NOT found in database.", sub);
        sqlite3_reset(prototype_db.find_whatever);
        return false;}
}

char* create_latency_str(char *clientid, int latencyNum){

    cJSON *json = cJSON_CreateObject();
    cJSON_AddNumberToObject(json, clientid, latencyNum);
    return cJSON_Print(json);
}

void insert_into_topics_table(struct mosquitto *context, char * sub){
    int rc;
    sqlite3_reset(prototype_db.insert_topic);
    sqlite3_clear_bindings(prototype_db.insert_topic);

    log__printf(NULL, MOSQ_LOG_INFO, "I am in insert_into_topics_table");

    // Safe copy of sub before mutation
    char topic_copy[256];
    strncpy(topic_copy, sub, sizeof(topic_copy));
    topic_copy[sizeof(topic_copy) - 1] = '\0';

    // Bind full topic name
    sqlite3_bind_text(prototype_db.insert_topic, 2, sub, -1, SQLITE_TRANSIENT);

    // Extract DeviceMac by truncating at '/'
    char *slash_pos = strchr(topic_copy, '/');
    if (slash_pos) {
        *slash_pos = '\0';
    }
    sqlite3_bind_text(prototype_db.insert_topic, 1, topic_copy, -1, SQLITE_STATIC);

    // Determine if this is a publisher
    int is_publisher = strstr(sub, "publisher") != NULL;
    sqlite3_bind_text(prototype_db.insert_topic, 3, is_publisher ? "1" : "0", -1, SQLITE_STATIC);

    rc = sqlite3_step(prototype_db.insert_topic);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert into Topics: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_reset(prototype_db.insert_topic);
        return;
    }

    sqlite3_reset(prototype_db.insert_topic);
}


void insert_into_subscribers_table(struct mosquitto* context){
    int rc;
    //
    sqlite3_reset(prototype_db.insert_subscribers);
    sqlite3_clear_bindings(prototype_db.insert_subscribers);
    char *temp = context->mqtt_cc.incoming_topic;
    char *slash = strchr(temp, '/');
    if (slash != NULL) {
        *slash = '\0';  // Truncate at the first '/'
    }
    sqlite3_bind_text(prototype_db.insert_subscribers, 1,temp, -1, SQLITE_TRANSIENT);
    //topic name
    cJSON * array1= cJSON_CreateArray();
    if (array1 == NULL){
        return NULL;
    }
   // int numthings1= 2;//sizeof(context->mqtt_cc.incoming_tasks);//sizeof(context->mqtt_cc.incoming_tasks[0]);
    for (int i =0; context->mqtt_cc.incoming_tasks[i][0]!='\0'; i++){

        cJSON_AddItemToArray(array1,cJSON_CreateString(context->mqtt_cc.incoming_tasks[i]));

    }
    char *jsonString = cJSON_PrintUnformatted(array1);
    cJSON_Delete(array1);
    sqlite3_bind_text(prototype_db.insert_subscribers, 2, jsonString,-1,SQLITE_TRANSIENT);  // 1 for true, 0 for false json for tasks?
    free(jsonString);
    //adding frequencies
    cJSON * array2= cJSON_CreateArray();
    if (array2 == NULL){
        return NULL;
    }
    //int numthings2= 2;//sizeof(context->mqtt_cc.incoming_frequencies)/sizeof(context->mqtt_cc.incoming_frequencies[0]);
    for (int i =0; context->mqtt_cc.incoming_frequencies[i]!=-1; i++){

        cJSON_AddItemToArray(array2,cJSON_CreateNumber(context->mqtt_cc.incoming_frequencies[i]));

    }
    char *jsonString2 = cJSON_PrintUnformatted(array2);
    cJSON_Delete(array2);
    
    sqlite3_bind_text(prototype_db.insert_subscribers, 3, jsonString2,-1,SQLITE_TRANSIENT);  // 1 for true, 0 for false
    free(jsonString2);
    
    cJSON * array3= cJSON_CreateArray();
    if (array3 == NULL){
        return NULL;
    }
    //int numthings3= 2;//sizeof(context->mqtt_cc.incoming_max_latencies)/sizeof(context->mqtt_cc.incoming_max_latencies[0]);
    for (int i =0; context->mqtt_cc.incoming_max_latencies[i]!=-1; i++){

        cJSON_AddItemToArray(array3,cJSON_CreateNumber(context->mqtt_cc.incoming_max_latencies[i]));

    }
    char *jsonString3 = cJSON_PrintUnformatted(array3);
    cJSON_Delete(array3);
    sqlite3_bind_text(prototype_db.insert_subscribers, 4, jsonString3, -1,SQLITE_TRANSIENT);  // 1 for true, 0 for false
    free(jsonString3);
    cJSON * array4= cJSON_CreateArray();
    if (array4 == NULL){
        return NULL;
    }
    //int numthings4= 2;//sizeof(context->mqtt_cc.incoming_accuracy)/sizeof(context->mqtt_cc.incoming_accuracy[0]);
    for (int i =0; context->mqtt_cc.incoming_accuracy[i]!=-1; i++){

        cJSON_AddItemToArray(array4,cJSON_CreateNumber(context->mqtt_cc.incoming_accuracy[i]));

    }
    char *jsonString4 = cJSON_PrintUnformatted(array4);
    cJSON_Delete(array4);
    sqlite3_bind_text(prototype_db.insert_subscribers, 5, jsonString4, -1,SQLITE_TRANSIENT);
    free(jsonString4);

    rc = sqlite3_step(prototype_db.insert_subscribers);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert into Topics: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_finalize(prototype_db.insert_subscribers);
        return ;
    }
    sqlite3_reset(prototype_db.insert_subscribers);

}

void insert_into_publisher_table(struct mosquitto * context){
    int rc;
    sqlite3_reset(prototype_db.insert_into_publishers);
    //
    char * temp = context->mqtt_cc.incoming_topic;
    char *slash_pos = strchr(temp, '/'); // Find the first occurrence of '/'
    if (slash_pos) {
        *slash_pos = '\0'; // Replace '/' with null terminator to truncate the string
    }
    uint8_t len = strlen(temp);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 1,temp, -1, SQLITE_STATIC);//devicemac
    cJSON * array1= cJSON_CreateArray();
    if (array1 == NULL){
        return NULL;
    }  

    //Sentinel value for accuracy
    for (int j=0; context->mqtt_cc.incoming_accuracy[j]!=-1;++j){
        cJSON_AddItemToArray(array1,cJSON_CreateNumber(context->mqtt_cc.incoming_accuracy[j]));
    }
    char *jsonString = cJSON_PrintUnformatted(array1);
    cJSON_Delete(array1);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 5, jsonString,-1,SQLITE_TRANSIENT);  // 1 for true, 0 for false json for tasks?
    free(jsonString);
    //adding frequencies
    cJSON * array2= cJSON_CreateArray();
    if (array2 == NULL){
        return NULL;
    }

    for (int i = 0; context->mqtt_cc.incoming_tasks[i][0] != '\0'; i++) {
            cJSON_AddItemToArray(array2,cJSON_CreateString(context->mqtt_cc.incoming_tasks[i]));
    }
    char *jsonString2 = cJSON_PrintUnformatted(array2);
    cJSON_Delete(array2);
    
    sqlite3_bind_text(prototype_db.insert_into_publishers, 2, jsonString2,-1,SQLITE_TRANSIENT);  // 1 for true, 0 for false
    free(jsonString2);
    
    cJSON * array3= cJSON_CreateArray();
    if (array3 == NULL){
        return NULL;
    }
    for (int i =0;context->mqtt_cc.incoming_frequencies[i]!=-1; i++){

        cJSON_AddItemToArray(array3,cJSON_CreateNumber(context->mqtt_cc.incoming_frequencies[i]));

    }
    char *jsonString3 = cJSON_PrintUnformatted(array3);
    cJSON_Delete(array3);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 3, jsonString3, -1,SQLITE_TRANSIENT);  // 1 for true, 0 for false
    free(jsonString3);
    cJSON * array4= cJSON_CreateArray();
    if (array4 == NULL){
        return NULL;
    }
    for (int i =0;context->mqtt_cc.incoming_energy[i]!=-1; i++){
        cJSON_AddItemToArray(array4,cJSON_CreateNumber(context->mqtt_cc.incoming_energy[i]));
    }
    char *jsonString4 = cJSON_PrintUnformatted(array4);
    cJSON_Delete(array4);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 6, jsonString4, -1,SQLITE_TRANSIENT);
    free(jsonString4);

    cJSON * array5= cJSON_CreateArray();
    if (array5 == NULL){
        return NULL;
    }
    for (int i =0;context->mqtt_cc.incoming_max_latencies[i]!=-1; i++){
        cJSON_AddItemToArray(array5,cJSON_CreateNumber(context->mqtt_cc.incoming_max_latencies[i]));
    }
    char *jsonString5 = cJSON_PrintUnformatted(array5);
    cJSON_Delete(array5);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 4, jsonString5, -1,SQLITE_TRANSIENT);
    free(jsonString5);

    rc = sqlite3_step(prototype_db.insert_into_publishers);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert into Topics: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_finalize(prototype_db.insert_into_publishers);
        return ;
    }
    sqlite3_reset(prototype_db.insert_into_publishers);
}
 
    