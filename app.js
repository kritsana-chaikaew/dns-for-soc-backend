const express = require('express')
const cors = require('cors')
const app = express()
const port = 3000
const http = require('http').Server(app);
const io = require('socket.io')(http, { origins: '*:*'});
const fs = require('fs');

const START_TIME_STAMP = 1509693769000;
const END_TIME_STAMP = 1510125409000;
var isStart = false;
var previousData = [];
let dga = [];

fs.readFile('20-dga.csv', "utf8", function(err, data) {
	var dgas = data.split('\n')
	for (var i=0;i<dgas.length;i++) {
		if (dgas[i] !== '') {
			dga.push(dgas[i].split(','))
		}
	}
});

const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
  host: 'localhost:9200',
});

let typeCountStartTime = START_TIME_STAMP;
let typeCountEndTime = END_TIME_STAMP;

app.use(cors());

function getPrettyTypeCountInWindow (result, ts) {
	try {
		var total = result.responses[0].hits.total;
		var response = result.responses[1].aggregations.count.buckets.reduce((o, x) => ({...o, [x.key]: x.doc_count}), {});
		response['timestamp'] = ts;
		response['NXDOMAIN'] += 0;
		response['FORMERR'] += 0;
		response['NOTAUTH'] += 0;
		response['REFUSED'] += 0;
		response['SERVFAIL'] += 0;
		response['NORMAL'] = total - result.responses[1].hits.total
		return response
	} catch {
		return null;
	}
}

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
			              "gte": START_TIME_STAMP,
			              "lte": END_TIME_STAMP,
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
				              "gte": START_TIME_STAMP,
				              "lte": END_TIME_STAMP,
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
				              "gte": START_TIME_STAMP,
				              "lte": END_TIME_STAMP,
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

async function getType (type) {
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
										"gte": START_TIME_STAMP,
										"lte": END_TIME_STAMP,
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

async function getTypeCountInWindow (startTime, endTime) {
	try {
    const response = await client.msearch({
			body: [
				{index: "dns-for-soc", type: "_doc"},
				{
					"size": 0,
					"query" : {
						"range": {
							"timestamp_s": {
								"gte": startTime,
								"lte": endTime,
								"format": "epoch_millis"
							}
						}
					}
				},
				{index: "dns-for-soc", type: "_doc"},
				{
				"size": 0,
				"aggs": {
					"count": {
						"terms": {
							"field": "answer"
						}
					}
				},  
				"query": {
					"bool": {
						"must": [
							{
								"range": {
									"timestamp_s": {
										"gte": startTime,
										"lte": endTime,
										"format": "epoch_millis"
									}
								}
							},
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
									]
								}
							}
						]
					}
				}
			}]
		});
		return response;
	}
	catch (err) {
		console.log(err.message);
		return null;
	}
}

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

app.get('/type', (req, res) => {
	const type = req.query.type;

	getType(type).then((data) => {
		res.setHeader('Content-Type', 'application/json');
		res.send(
			data.aggregations.result.buckets,
		);
	});
});

app.get('/type-count', (req, res) => {
	var startTime = START_TIME_STAMP;
	var endTime = END_TIME_STAMP;
	getTypeCountInWindow(startTime, endTime).then((result) => {
		var response = getPrettyTypeCountInWindow(result, endTime);
		res.send(
			response
		);
	});
});

io.on('connection', (client) => {
	client.emit('prev', previousData);
	client.on('subscribeToStream', (setting) => {
		if (!isStart) {
			isStart = true;

			const startTime = setting.startTime;
			const queryInterval = setting.queryInterval;
			const interval = setting.interval;
	
			if (typeCountStartTime === START_TIME_STAMP) {
				typeCountStartTime = startTime;
				typeCountEndTime = startTime + queryInterval;
			}
	
			setInterval(() => {
				getTypeCountInWindow(typeCountStartTime, typeCountEndTime).then((result) => {
					var response = getPrettyTypeCountInWindow(result, typeCountEndTime);
					io.sockets.emit('stream', response);
					previousData.push(response);
					if (previousData.length > 1000) {
						previousData.shift();
					}
				});
	
				typeCountStartTime += queryInterval;
				typeCountEndTime += queryInterval;
				if (typeCountStartTime > typeCountEndTime) {
					typeCountStartTime = START_TIME_STAMP;
					typeCountEndTime = START_TIME_STAMP + queryInterval;
				}
			}, interval);
		}
	});
});

app.get('/dga', (req, res) => {
	res.send(
		dga
	);

});

http.listen(port, () => console.log(`Example app listening on port ${port}!`))