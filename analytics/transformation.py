from analytics.om_requests import om_requests
from analytics.yv_requests import yv_requests

def transform_data(olap):
    om_requests(olap)
    yv_requests(olap)