from websim.starcluster import tasks as startcluster

default_config_url = "http://0.0.0.0:8080/api/v1/file/544e41b6ff34c706dd4f79bc/download"
api_url = 'http://0.0.0.0:8080/api/v1'
cluster_id = '544e66baff34c74de4497a47'

log_write_url = '%s/clusters/%s/log' % (api_url, cluster_id)

startcluster.start_cluster.delay('default_cluster', 'cjh3cluster', log_write_url=log_write_url)
