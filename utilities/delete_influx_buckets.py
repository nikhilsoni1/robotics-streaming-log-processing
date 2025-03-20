#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# In[ ]:


influx_url = "http://localhost:8086"
influx_org = "myorg"
influx_bucket = "bucket01"
influx_token = "WA9QeACIozPJm1NKnSIg5o91Iq_Er1a-_dm8oPcdP8gtNNfc8yg32g7rpRWNgzKP91TVaDRolk3cfHugSYrLhQ=="


# In[ ]:


client = influxdb_client.InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)


# In[ ]:


bucket_api = client.buckets_api()


# In[ ]:


a = bucket_api.find_bucket_by_name(influx_bucket)


# In[ ]:


a.id


# In[ ]:


bucket_api.delete_bucket(a.id)


# In[ ]:


bucket_api.create_bucket(bucket_name="bucket01",)


# In[ ]:




