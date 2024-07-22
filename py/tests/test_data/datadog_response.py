good_response = \
    {
       "status": "ok",
       "res_type": "time_series",
       "resp_version": 1,
       "query": "avg: redis.mem.peak{app_feature: shared}by{shared_resource_id}.rollup(5)",
       "from_date": 1721083881000,
       "to_date": 1721083891000,
       "series": [
          {
             "unit": [
                {
                   "family": "bytes",
                   "id": 2,
                   "name": "byte",
                   "short_name": "B",
                   "plural": "bytes",
                   "scale_factor": 1.0
                },
                None
             ],
             "query_index": 0,
             "aggr": "avg",
             "metric": "redis.mem.peak",
             "tag_set": ["shared_resource_id: rc_long_redis"],
             "expression": "avg: redis.mem.peak{shared_resource_id: rc_long_redis,app_feature: shared}.rollup(5)",
             "scope": "app_feature: shared, shared_resource_id: rc_long_redis",
             "interval": 5,
             "length": 2,
             "start": 1721083885000,
             "end": 1721083894000,
             "pointlist":[
                [1721083885000.0, 2.5],
                [1721083890000.0, 3.6]
             ],
             "display_name": "redis.mem.peak",
             "attributes": {}
          }
       ],
       "values": [],
       "times": [],
       "message": "",
       "group_by": ["shared_resource_id"]
    }

bad_response = {
  "status": "error",
  "res_type": "time_series",
  "resp_version": 1,
  "query": "avg: redis.mem.peak{app_feature: shared}by{shared_resource_id}.rollup(5)",
  "from_date": 1721083891000,
  "to_date": 1721083881000,
  "series": [],
  "values": [],
  "times": [],
  "error": "Invalidqueryinput: \nqueryendisbeforestart",
  "group_by": [
    "shared_resource_id"
  ]
}
