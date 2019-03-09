local Q = {}

local function first_query(from, to, match_id)

    if match_id then
        match_id_query  = " AND (operation_history.op_object.pays.asset_id: " .. match_id .. " OR operation_history.op_object.receives.asset_id: " .. match_id .. ")"
    else
        match_id_query = ""
    end

return [[

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
                "query": "operation_type: 4 AND operation_history.op_object.is_maker: false]] .. match_id_query .. [[",
                "analyze_wildcard": true,
                "default_field": "*"
              }
            },

            {
              "range": {
                "block_data.block_time": {
                  "gte": "]] .. from .. [[",
                  "lt": "]] .. to .. [["
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
end

local function asset_query(asset_id)

    return [[

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
                    "query": "_index:\"objects-asset\" _id: ]] .. asset_id .. [[",
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
end

Q.first_query = first_query
Q.asset_query = asset_query

return Q