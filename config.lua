local C = {}

local es_direct_connection = "https://elasticsearch.bitshares-kibana.info/"
local rest_bitshares_api = "http://185.208.208.184:5000/"

local from = "now-24h"
local to = "now"

C.es_direct_connection = es_direct_connection
C.rest_bitshares_api = rest_bitshares_api
C.from = from
C.to = to

return C