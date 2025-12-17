import proto_db as db
import json

def getImpactedSubscription(topic) -> list:
    # get database
    database = db.Database()
    database.openDB()
    # call DB select query
    results = database.selectSubscriptionsWithTopic(topic)
    return results

def updateSubscription(topic, new_latency_req, new_max_allowed):
    # get database
    database = db.Database()
    database.openDB()
    # call DB update 
    database.updateSubscriptionWithLatency(topic, new_latency_req, new_max_allowed)
    database.closeDB()

def deleteSubscription(topic):
    database = db.Database()
    database.openDB()
    database.deleteSubscription(topic)
    database.closeDB()


def calculateNewMaxLatency(latency_req_json:dict):
    # If no latencyQoS left, return None
    if not latency_req_json: 
        return None
    # else, return the min of the dictionary values
    min_value = min(latency_req_json.values())
    return min_value

# A subscriber can subscribe to many topics with different latency qos
def updateDB(willMsg):
    update_values = list()
    willMsg_json = json.loads(willMsg)
    # Extract Values
    clientid = willMsg_json["clientid"]
    topics_list = willMsg_json["topics"]
    # For each topic in the topic list
    for i in range(len(topics_list)):
        # Remove %latency%
        latStringIndex = topics_list[i].rindex("%latency%")
        topics_list[i] = topics_list[i][:latStringIndex]
        # Get row from DB (array of 1 tuple since 1 row per topic)
        impacted_sub = getImpactedSubscription(topics_list[i])
        impacted_sub = impacted_sub[0] # only 1 row, convert from array to single tuple 

        # Convert latency_req to json dictionary
        latency_req_json = json.loads(impacted_sub[1])


        # Remove item with clientid key
        if(clientid in latency_req_json):
            del latency_req_json[clientid]

        # Recalculate max_allowed_latency from json dictionary
        new_max_allowed = calculateNewMaxLatency(latency_req_json)
        
        # Convert back to string
        new_latency_req_str = json.dumps(latency_req_json)

        # Print new values to console
        if new_max_allowed:
            updateSubscription(topic=topics_list[i], new_latency_req=new_latency_req_str, new_max_allowed=new_max_allowed)
        else:
            deleteSubscription(topics_list[i])
            


#Hash Map (topic: latency_req)

#List of tuples with the update values
    # [(newLatencyReq, newMax, willTopic), (newLatencyReq, newMax, willTopic), ...]
    # use executemany

# First: get the latency_req for a topic in the will_msg topics
    # SELECT * WHERE topic = willTopic
    # Convert the latency_req to a json object
    # remove their username from the latency_req object
    # Add to hash map (topic: dictionary latency_req)

# Second: Recalculate max_allowed_latency
    # For each key in the hash map
    # Loop over the keys in the latency_req dictionary, find the new max latency
    # Convert latency_req to string
    # Save willTopic, string latency_req, max_allowed_latency as a new tuple to the UpdateValues array of tuples

# Third: Update the DB
    # Convert the latency_req back to a string
    # UPDATE subscriptions SET latencyreq = ?, max_allowed_latency = ? WHERE topic = ?
    # values = ()

# Edge Cases:
    # Only 1 sub in a topic, when the subscriber disconnects, return an empty json string
        # set max_allowed_latency to NULL


