from flask import Flask, jsonify, request
import elasticsearch

ES = elasticsearch.Elasticsearch("http://sibils-es.lan.text-analytics.ch:9200")
SIBILS_VERSION = "v4.0.5.1"

app = Flask(__name__)


@app.route('/')
def index():
    return """<h1>Welcome to this app</h1>

<h2>Entry point /facets/&lt;field&gt;</h2>

<h3>Parameters</h3>
<ul>
<li><pre>offset</pre></li>
<li><pre>limits</pre></li>
</ul>

<h3>Examples</h3>

<ul>
<li><pre>/facets/journal</pre></li>
<li><pre>/facets/affiliations</pre></li>
<li><pre>/facets/pubyear?offset=200&limit=10</pre></li>
</ul>


<h2>Entry point /collections</h2>

Return the collections per SIBiLS versions
"""


@app.route("/collections")
def collections():
    # get all ES indexes
    indices = ES.indices.get_alias(index="*")
    # keep only the index names
    collections = list(indices.keys())
    # keep only the index "sibils_<collection>_<version>". Example: "sibils_med24_v4.0.5.1"
    result = {}
    for collection in collections:
        if not collection.startswith("sibils_"):
            continue
        # "sibils_med24_v4.0.5.1" --> ("sibils", "med24", "v4.5.0.1")
        collection_part = collection.split("_")
        # fetch from the ES the document count in this index
        count = ES.count(index=collection)['count']
        # following the example above: result["v4.5.0.1"]["med24"] = count
        result.setdefault(collection_part[2], {})[collection_part[1]] = count
    # return a reponse in the JSON format
    return jsonify(result)


@app.route('/facets/<field>')
def aggregate(field):
    # retreive the query parameters from the URL
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 10))
    # hardcoded
    collection = "med24"
    # search the values on ES
    values = search_field_values(collection, field)
    # truncated the values so Firefox display the results quickly
    truncated_values = values[offset:offset+limit]
    # return the values
    return jsonify({
        "offset": offset,
        "limit": limit,
        "size": len(values),
        "values": truncated_values,
    })


def search_field_values(collection, field):
    """Fetch from ElasticSearch all the values for a field.
    Return something like this:

    ```json
    [
        {
            "doc_count": 292823,
            "key": "PloS one",
        },
        {
            "doc_count": 205106,
            "key": "Scientific reports",
        }
    ]
    ```
    """
    query = {
        "size": 0,
        "aggs": {
            "unique_values": {
                "terms": {
                    "field": f"{field}.keyword",
                    "size": 10000
                }
            }
        }
    }
    response = ES.search(index=f"sibils_{collection}_{SIBILS_VERSION}", body=query)
    return response['aggregations']['unique_values']['buckets']


if __name__ == '__main__':
    app.run(debug=True)
    