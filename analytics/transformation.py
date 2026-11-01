from analytics.om_requests import om_requests
from analytics.yv_requests import yv_requests
from analytics.iy_queries import iy_requests
from analytics.yh_requests import yh_requests
from analytics.oy_requests import oy_requests

def transform_data(olap):
    om_requests(olap)
    yv_requests(olap)
    iy_requests(olap)
    yh_requests(olap)
    oy_requests(olap)
    