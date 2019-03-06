requests = require('requests')

local queries = require('queries')
local functions = require('functions')
local config = require('config')

headers = {['Content-Type'] = 'application/json'}

markets = {} -- our table to store quote_id, base_id, quote_amount and base_amount

-- queries ES with all fill orders taker side in period of time
response = requests.get{config.es_direct_connection .. '_search?scroll=1m', data=queries.first_query(config.from, config.to), headers = headers}
json_body, error = response.json()

total = json_body.hits.total -- total fill orders in period
scroll_id = json_body._scroll_id -- We use scroll to query ES as it cant retrive by default more than 10000 results

-- parse first 1000 results from first query
functions.loop_fills(json_body.hits.hits)

-- use scroll api to get the rest of the hits until total == 0
while total > 0 do

    scroll_query = [[
    {
      "scroll" : "1m",
      "scroll_id" : "]] .. scroll_id .. [["
    }

    ]]
    response = requests.get{config.es_direct_connection .. '_search/scroll', data=scroll_query, headers = headers}
    json_body, error = response.json()

    -- load markets with data
    functions.loop_fills(json_body.hits.hits)
end

-- loop collected data in markets table
for k, v in pairs(markets) do

    -- get quote name
    quote_query=queries.asset_query(v.quote)
    response = requests.get{config.es_direct_connection .. '_search', data=quote_query, headers = headers}
    json_body, error = response.json()
    quote_name = json_body.hits.hits[1]._source.symbol

    -- get base name
    base_query=queries.asset_query(v.base)
    response = requests.get{config.es_direct_connection .. '_search', data=base_query, headers = headers}
    json_body, error = response.json()
    base_name = json_body.hits.hits[1]._source.symbol

    -- get ticker data
    response = requests.get{config.rest_bitshares_api .. 'get_ticker?base=BTS&quote=' .. quote_name .. '', headers = headers}
    json_body, error = response.json()

    -- print csv
    print(quote_name .. "/" .. base_name .. "," .. v.quote_amount/v.base_amount .. "," .. json_body.latest ..
            "," .. (v.quote_amount/v.base_amount)*json_body.latest)
end
