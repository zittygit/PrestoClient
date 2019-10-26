# PrestoClient
presto client for python

### example

 pc = prestoclient(host='xxx.xxx.xxx.xxxx', port=8080, user='presto', catalog='mysql', schema='ziyezhang')
 query = pc.create_query("select * from table")
 for data in pc.get_query_result(query.get('next_uri')):
     print(data)
