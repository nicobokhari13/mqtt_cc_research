# Frequently Ran Commands
- To delete all *.o files
  - `find . -type f -name '*.o' -delete`
- Find mosquitto pid and kill process
  - `ps aux | grep mosquitto`
- Simulated Publishers
  - `python3 sensor.py sim 100 d8:3a:dd:90:ee:62 0.1`
  - `python3 sensor.py sim 100 d8:3a:dd:90:ee:38 0.1`
- Simulated Subscribers
  - `python3 subscriber.py sensor/temperature%latency%10,sensor/humidity%latency%30,sensor/airquality%latency%50`
  - `python3 subscriber.py sensor/temperature%latency%28,sensor/humidity%latency%45`
  - `python3 subscriber.py sensor/humidity%latency%12,sensor/airquality%latency%20`
- Raspberry Pi
  ```bash
  cd repos/research/mqtt_cc_research
  source prototype/dev/bin/activate
  cd testbed/devs
  ```
- TCP Dump
  - `sudo tcpdump -i wlp0s20f3 port 1883 -w filname.pcap`
  - `sudo tcpdump -i lo port 1883 -w filname.pcap`