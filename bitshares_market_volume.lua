function DeepPrint (e)
    -- if e is a table, we should iterate over its elements
    if type(e) == "table" then
        for k,v in pairs(e) do -- for every element in the table
            print(k)
            DeepPrint(v)       -- recursively repeat the same procedure
        end
    else -- if not, we can just print it
        print(e)
    end
end


requests = require('requests')

headers = {['Content-Type'] = 'application/json'}

first_query = [[

{
  "version": true,
  "size": 1000,
  "sort": [
    {
      "block_data.block_time": {
        "order": "desc",
        "unmapped_type": "boolean"
      }
    }
  ],
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [
    "block_data.block_time",
    "operation_history.op_object.expiration",
    "operation_history.op_object.expiration_time"
  ],
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "operation_type: 4 AND operation_history.op_object.is_maker: false",
            "analyze_wildcard": true,
            "default_field": "*"
          }
        },

        {
          "range": {
            "block_data.block_time": {
              "gte": "now-1h",
              "lt": "now"
            }
          }
        }
      ],
      "filter": [],
      "should": [],
      "must_not": []
    }
  }
}

]]

response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search?scroll=1m', data=first_query, headers = headers}

json_body, error = response.json()

markets = {}

total = json_body.hits.total
-- print(total)

for k, v in pairs(json_body.hits.hits) do

    total = total - 1
    --print("first: " .. total)

    new = true;
    scroll_id = json_body._scroll_id


    if k > 1 then

        for k2, v2 in pairs(markets) do
            if v2.quote == v._source.operation_history.op_object.pays.asset_id and
                    v2.base == v._source.operation_history.op_object.receives.asset_id then

                markets[k2].quote_amount = markets[k2].quote_amount + v._source.operation_history.op_object.pays.amount
                markets[k2].base_amount = markets[k2].base_amount + v._source.operation_history.op_object.receives.amount
                new = false

                break
            end
        end
    end

    if new then
        markets[k] = {
            quote = v._source.operation_history.op_object.pays.asset_id,
            base = v._source.operation_history.op_object.receives.asset_id,
            quote_amount = v._source.operation_history.op_object.pays.amount,
            base_amount = v._source.operation_history.op_object.receives.amount
        }
	end
end

while total > 0 do

    scroll_query = [[
        {
            "scroll" : "1m",
            "scroll_id" : "]] .. scroll_id .. [["
        }

    ]]
    response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search/scroll', data=scroll_query, headers = headers}

    json_body, error = response.json()

    -- duplicated code
    for k, v in pairs(json_body.hits.hits) do

        total = total - 1
        -- print("scroll: " .. total)

        new = true;
        scroll_id = json_body._scroll_id

        if k > 1 then

            for k2, v2 in pairs(markets) do
                if v2.quote == v._source.operation_history.op_object.pays.asset_id and
                        v2.base == v._source.operation_history.op_object.receives.asset_id then

                    markets[k2].quote_amount = markets[k2].quote_amount + v._source.operation_history.op_object.pays.amount
                    markets[k2].base_amount = markets[k2].base_amount + v._source.operation_history.op_object.receives.amount
                    new = false

                    break
                end
            end
        end

        if new then
            markets[k] = {
                quote = v._source.operation_history.op_object.pays.asset_id,
                base = v._source.operation_history.op_object.receives.asset_id,
                quote_amount = v._source.operation_history.op_object.pays.amount,
                base_amount = v._source.operation_history.op_object.receives.amount
            }
        end
    end
end


-- DeepPrint(markets)
for k, v in pairs(markets) do

    quote_query = [[

        {
          "version": true,
          "_source": {
            "excludes": []
          },
          "stored_fields": [
            "*"
          ],
          "script_fields": {},
          "query": {
            "bool": {
              "must": [
                {
                  "query_string": {
                    "query": "_index:\"objects-asset\" _id: ]] .. v.quote .. [[",
                    "analyze_wildcard": true,
                    "default_field": "*"
                  }
                },
                {
                  "range": {
                    "block_time": {
                      "gte": "now-5y",
                      "lt": "now"
                    }
                  }
                }
              ],
              "filter": [],
              "should": [],
              "must_not": []
            }
          }
        }

    ]]

    response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search', data=quote_query, headers = headers}

    json_body, error = response.json()


    quote_name = json_body.hits.hits[1]._source.symbol
    --quote_name = ""

    base_query = [[

        {
          "version": true,
          "_source": {
            "excludes": []
          },
          "stored_fields": [
            "*"
          ],
          "script_fields": {},
          "query": {
            "bool": {
              "must": [
                {
                  "query_string": {
                    "query": "_index:\"objects-asset\" _id: ]] .. v.base .. [[",
                    "analyze_wildcard": true,
                    "default_field": "*"
                  }
                },
                {
                  "range": {
                    "block_time": {
                      "gte": "now-5y",
                      "lt": "now"                    }
                  }
                }
              ],
              "filter": [],
              "should": [],
              "must_not": []
            }
          }
        }

    ]]

    response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search', data=base_query, headers = headers}

    json_body, error = response.json()

    -- DeepPrint(json_body.hits.hits[2])

    base_name = json_body.hits.hits[1]._source.symbol


    print(quote_name .. "/" .. base_name .. "," .. v.quote_amount/v.base_amount)



    -- print(base_name)
    -- print(quote_name)





end
