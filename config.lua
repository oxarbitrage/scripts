local C = {}

local es_direct_connection = "https://elasticsearch.bitshares-kibana.info/"
local rest_bitshares_api = "http://185.208.208.184:5000/"

local from = "now-5m"
local to = "now"

-- uncomment to only get markets in quote or base that match this id
-- local match_id = "1.3.121"

C.es_direct_connection = es_direct_connection
C.rest_bitshares_api = rest_bitshares_api
C.from = from
C.to = to
C.match_id = match_id

return C