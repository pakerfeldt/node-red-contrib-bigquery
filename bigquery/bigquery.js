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

        node.on("input", function (msg) {
            var dataset = bigquery.dataset(node.dataset),
                table = dataset.table(node.table),
                insert_data = JSON.parse(msg.payload);

            table.insert(insert_data, function (err, apiResponse) {
                if (err) {
                    node.error("gcp.error.general-error: " + JSON.stringify(err));
                    node.error("gcp.error.general-response: " + JSON.stringify(apiResponse));
                    return;
                }
            });
        });
    }
    RED.nodes.registerType("bigquery insert", BigQueryInsertNode);
};
