from analytics.om_requests import om_requests
from analytics.yv_requests import yv_requests
from analytics.iy_queries import iy_queries
from analytics.yh_requests import yh_requests

def transform_data(olap):
    om_requests(olap)
    yv_requests(olap)
    iy_queries(olap)
    yh_requests(olap)