# Planning 

- use Devices() and Publishing Units() from algo_utils
- topic container contains dictionary with
  - topic: max_allowed_latency
  - default set to -1
  - if min is < 0, then just set the new value to a latency
  - else, check if the new value is less than the current value
  - update accordingly
- subscriber container used to
  - generate subscriber lat Qos based on num topics and num subs
    - if num subs 0, use default 
    - else, use number given
  - lat Qos is in miliseconds ? (100 - 10,000) (0.1 seconds to 10 seconds)
    - 10^-1, 10^0, 10^1
  - a for loop that goes as long as the number of subscribers
    - for each subscriber, generate random num between 1 - numtopics
    - use random.sample to choose topic from topic container.keys()
      - for each topic in sample
      - random generate frequency between (100 - 10,000) miliseconds
    -  update Topic Container accordingly
- publisher container 
  - generates device Macs
  - generate device capabilities (similar to subscirber container)
    - assignments will use topic:freq where freq initialize to -1
      - if -1, set max_allowed QoS
  - Devices() and Publishing Units() from algo_utils + pub_utils to
    - track topic assignment
    - track energy consumption + battery