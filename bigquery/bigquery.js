module.exports = function (RED) {
    "use strict";

    function BigQueryNode(n) {
        RED.nodes.createNode(this, n);
        if (this.credentials
                && this.credentials.keyFile
                && this.credentials.projectId) {

            var BigQuery = require('@google-cloud/bigquery');
            this.bigquery = new BigQuery({
                projectId: this.credentials.projectId,
                keyFilename: this.credentials.keyFile
            });
        }
    }

    RED.nodes.registerType("bigquery-config", BigQueryNode, {
        credentials: {
            keyFile: { type: "text" },
            projectId: { type: "text" }
        }
    });

    function BigQueryQueryNode(n) {
        RED.nodes.createNode(this, n);
        this.bigquery_node = RED.nodes.getNode(n.bigquery);
        this.query = n.query;
        var bigquery = this.bigquery_node.bigquery || null,
            node = this;
        if (!bigquery) {
            node.warn("gcp.warn.missing-credentials");
            return;
        }
        node.on("input", function (msg) {
            node.status({ fill: "blue", shape: "dot", text: "gcp.status.querying" });
            bigquery.query(node.query, function (err, rows) {
                if (err) {
                    node.error("gcp.error.query-failed: " + JSON.stringify(err));
                }
                msg.payload = rows;
                node.status({});
                node.send(msg);
            });
        });
    }
    RED.nodes.registerType("bigquery query", BigQueryQueryNode);

    function BigQueryInsertNode(n) {
        RED.nodes.createNode(this, n);
        this.bigquery_node = RED.nodes.getNode(n.bigquery);
        this.dataset = n.dataset;
        this.table = n.table;
        var node = this,
            bigquery = this.bigquery_node.bigquery;
        if (!bigquery) {
            node.warn("gcp.warn.missing-credentials");
            return;
        }
        if (!this.dataset && !this.table) {
            node.warn("gcp.warn.no-dataset-table-specified");
            return;
        }

        node.updateNodeStatus = function(color, msg) {
            node.status({
                fill: color, shape: "dot", text: msg
            });
            setTimeout(function() {
                node.status({});
            }, 1000);
        };

        node.test = function() {
            // Imports the Google Cloud client library
            const BigQuery = require('@google-cloud/bigquery');

            // Your Google Cloud Platform project ID
            const projectId = 'villa-akerfeldt';
            const file = '/root/.node-red/google-cloud-auth.json'

            // Creates a client
            const bigquery = new BigQuery({
              projectId: projectId,
              keyFilename: file
            });

            const datainsert = {
            	"water_total": 42994873,
            	"power_vp": 3460651,
            	"power_ftx": 4401819,
            	"time": "2018-01-23T09:50:17.672Z",
            	"power_total": 5737457,
            	"power_garage_radiator_south": 182521,
            	"power_garage_radiator_north": 76388
            };
            // The name for the new dataset
            const dataset = bigquery.dataset('consumption');
            const table = dataset.table('consumption_test');
            table.insert(datainsert, function(err, response) {
                console.log(JSON.stringify(err));
                console.log(JSON.stringify(response));
            });
        };

        node.on("input", function (msg) {
            node.test();
            if (msg.payload !== null && (typeof msg.payload === 'object')
                || (typeof msg.payload === 'string')) {
                    var dataset = bigquery.dataset(node.dataset),
                        table = dataset.table(node.table),
                        insert_data = (typeof msg.payload === 'string') ? JSON.parse(msg.payload) : msg.payload;
                        table.insert(insert_data, { raw: false }).then((data) => {
                          let insertErrors = data[1];
                          // --------------------
                          // data[1] should be undefined, we only executed this with one argument, the apiResponse
                          // --------------------

                          if (insertErrors) {
                              node.updateNodeStatus("red", 'Error');
                              node.error('BigQuery response: ' + JSON.stringify(insertErrors));
                            // Some rows failed to insert, while others may have succeeded.
                            insertErrors.map((insertError) => {
                              insertError.errors.map((error) => {
                                  node.error(`PartialFailureError: BigQuery insert failed due to: ${JSON.stringify(error)}`);
                              });
                            });
                          }
                          return message;
                        }).catch((error) => {
                            node.error(`Error inserting into bigQuery for id: ?: ${JSON.stringify(error)}`);
                        });
                        /*
                        table.insert(insert_data, function (err, apiResponse) {
                            if (err === null && apiResponse !== null
                                && apiResponse.kind === "bigquery#tableDataInsertAllResponse") {
                                    node.updateNodeStatus("green", 'Published');
                                } else {
                                    node.updateNodeStatus("red", 'Error');
                                    node.error('BigQuery response: ' + JSON.stringify(apiResponse) + ', error: ' + JSON.stringify(err));
                                }
                            if (err) {
                                node.error("gcp.error.general-error: " + JSON.stringify(err));
                                return;
                            }
                        });
                        */
            } else {
                node.error("Unrecognized input type");
            }
        });
    }
    RED.nodes.registerType("bigquery insert", BigQueryInsertNode);
};
