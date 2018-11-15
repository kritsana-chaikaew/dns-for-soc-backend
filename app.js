const express = require('express')
const cors = require('cors')
const app = express()
const port = 3000

const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
  host: 'localhost:9200',
  //log: 'trace'
});

app.use(cors());

async function getNX (interval) {
	if (!interval) {
		interval = '1h';
	}
  try {
    const response = await client.search({
      index: 'dns-for-soc',
      type: '_doc',
      body: {
			  "aggs": {
			    "nx": {
			      "date_histogram": {
			        "field": "timestamp_s",
			        "interval": interval,
			        "time_zone": "Asia/Bangkok",
			        "min_doc_count": 1
			      }
			    }
			  },
			  "size": 0,
			  "query": {
			    "bool": {
			      "must": [
			        {
			          "match_all": {}
			        },
			        {
			          "range": {
			            "timestamp_s": {
			              "gte": 1509692400000,
			              "lte": 1510038000000,
			              "format": "epoch_millis"
			            }
			          }
			        },
			        {
			          "match_phrase": {
			            "answer": {
			              "query": "NXDOMAIN"
			            }
			          }
			        }
			      ],
			    }
			  }
      }
    })
    return response;
  } catch (err) {
    return null;
  }
}

async function getNormal (interval) {
	if (!interval) {
		interval = '1h';
	}
  try {
    const response = await client.search({
      index: 'dns-for-soc',
      type: '_doc',
			body: {
				  "aggs": {
				    "normal": {
				      "date_histogram": {
				        "field": "timestamp_s",
				        "interval": interval,
				        "time_zone": "Asia/Bangkok",
				        "min_doc_count": 1
				      }
				    }
				  },
				  "size": 0,
				  "query": {
				    "bool": {
				      "must": [
				        {
				          "match_all": {}
				        },
				        {
				          "range": {
				            "timestamp_s": {
				              "gte": 1509692400000,
				              "lte": 1510038000000,
				              "format": "epoch_millis"
				            }
				          }
				        }
				      ],
				      "must_not": [
				        {
				          "bool": {
				            "should": [
				              {
				                "match_phrase": {
				                  "answer": "NXDOMAIN"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "FORMERR"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "NOTAUTH"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "REFUSED"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "SERVFAIL"
				                }
				              }
				            ],
				            "minimum_should_match": 1
				          }
				        }
				      ]
				    }
				  }
				}
    })
    return response;
  } catch (err) {
    return null;
  }
}

async function getError (interval) {
	if (!interval) {
		interval = '1h';
	}
  try {
    const response = await client.search({
      index: 'dns-for-soc',
      type: '_doc',
			body: {
				  "aggs": {
				    "error": {
				      "date_histogram": {
				        "field": "timestamp_s",
				        "interval": interval,
				        "time_zone": "Asia/Bangkok",
				        "min_doc_count": 1
				      }
				    }
				  },
				  "size": 0,
				  "query": {
				    "bool": {
				      "must": [
				        {
				          "match_all": {}
				        },
				        {
				          "range": {
				            "timestamp_s": {
				              "gte": 1509692400000,
				              "lte": 1510038000000,
				              "format": "epoch_millis"
				            }
				          }
				        },
				        {
				          "bool": {
				            "minimum_should_match": 1,
				            "should": [
				              {
				                "match_phrase": {
				                  "answer": "NXDOMAIN"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "FORMERR"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "NOTAUTH"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "REFUSED"
				                }
				              },
				              {
				                "match_phrase": {
				                  "answer": "SERVFAIL"
				                }
				              }
				            ]
				          }
				        }
				      ]
				    }
				  }
				}			
		});
    return response;
  } catch (err) {
    return null;
  }
}

async function getTopClient (size) {
	if (!size) {
		size = 10;
	}
	try {
    const response = await client.search({
      index: 'dns-for-soc',
      type: '_doc',
			body: {
				"aggs": {
					"topClient": {
						"terms": {
							"field": "client",
							"size": size,
							"order": {
								"_count": "desc"
							}
						}
					}
				},
				"query": {
					"bool": {
						"must": [
							{
								"match_all": {}
							},
							{
								"range": {
									"timestamp_s": {
										"gte": 1509728400000,
										"lte": 1509814800000,
										"format": "epoch_millis"
									}
								}
							}
						]
					}
				}
			}
		});
		return response;
	} catch (err) {
		return null;
	}
}

async function getType(type) {
	try {
    const response = await client.search({
      index: 'dns-for-soc',
      type: '_doc',
			body: {
				"aggs": {
					"result": {
						"terms": {
							"field": "query",
							"size": 20,
							"order": {
								"_count": "desc"
							}
						}
					}
				},
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"match_all": {}
							},
							{
								"range": {
									"timestamp_s": {
										"gte": 1509692400000,
										"lte": 1510038000000,
										"format": "epoch_millis"
									}
								}
							},
							{
								"match_phrase": {
									"type": {
										"query": type.toUpperCase()
									}
								}
							}
						]
					}
				}
			}
		});
		return response;
	}
	catch (err) {
		console.log(err.message);
		return null;
	}
}

app.get('/', (req, res) => res.send('Hello'));
app.get('/nx', (req, res) => {
	const interval = req.query.interval;
  getNX(interval).then((nx) => {
		getNormal(interval).then((normal) => {
    	res.setHeader('Content-Type', 'application/json');
    	res.send([
				nx.aggregations.nx.buckets,
				normal.aggregations.normal.buckets,
			]);
		});
  });
});
app.get('/error', (req, res) => {
	const interval = req.query.interval;
	getError(interval).then((error) => {
    res.setHeader('Content-Type', 'application/json');
		res.send(error.aggregations.error.buckets);
	});
});

app.get('/normal', (req, res) => {
	const interval = req.query.interval;
	getNormal(interval).then((normal) => {
    res.setHeader('Content-Type', 'application/json');
		res.send(normal.aggregations.normal.buckets);
	});
});

app.get('/topclient', (req, res) => {
	getTopClient().then((topClient) => {
		var clients =  [];
		clients = topClient.aggregations.topClient.buckets.map((x) => x.key);
		var condition = {};

		res.send(clients);
	});
});

app.get('/type', (req, res) => {
	const type = req.query.type;

	getType(type).then((data) => {
		res.setHeader('Content-Type', 'application/json');
		res.send(
			data.aggregations.result.buckets,
		);
	});
});

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
