requests = require('requests')

local queries = require('queries')
local functions = require('functions')
local config = require('config')

headers = {['Content-Type'] = 'application/json'}

response = requests.get{config.es_direct_connection .. '_search?scroll=1m', data=queries.first_query(config.from, config.to), headers = headers}

json_body, error = response.json()

markets = {}

total = json_body.hits.total
scroll_id = json_body._scroll_id
-- print(total)

functions.loop_fills(json_body.hits.hits)

while total > 0 do

    scroll_query = [[
    {
      "scroll" : "1m",
      "scroll_id" : "]] .. scroll_id .. [["
    }

    ]]
    response = requests.get{config.es_direct_connection .. '_search/scroll', data=scroll_query, headers = headers}

    json_body, error = response.json()

    functions.loop_fills(json_body.hits.hits)

end

-- DeepPrint(markets)

-- need to make this more effcient as i am querying each record to just get an asset symbol
for k, v in pairs(markets) do

    quote_query=queries.asset_query(v.quote)
    response = requests.get{config.es_direct_connection .. '_search', data=quote_query, headers = headers}

    json_body, error = response.json()

    quote_name = json_body.hits.hits[1]._source.symbol
    --quote_name = ""

    base_query=queries.asset_query(v.base)
    response = requests.get{config.es_direct_connection .. '_search', data=base_query, headers = headers}

    json_body, error = response.json()

    -- DeepPrint(json_body.hits.hits[2])

    base_name = json_body.hits.hits[1]._source.symbol


    response = requests.get{config.rest_bitshares_api .. 'get_ticker?base=BTS&quote=' .. quote_name .. '', headers = headers}
    json_body, error = response.json()

    print(quote_name .. "/" .. base_name .. "," .. v.quote_amount/v.base_amount .. "," .. json_body.latest .. "," .. (v.quote_amount/v.base_amount)*json_body.latest)

    -- print(base_name)
    -- print(quote_name)

end
