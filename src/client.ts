import { Client, ClientConfig } from "pg";

const sub = new Subscriber();

class GenericClient<T> {
    protected schedulerConfig : T;
    protected client! : Client;
    protected table : string;

    private connectionConfig ?: ClientConfig;
    private schema : string;

    constructor (table : string, schema : string, connectionConfig : ClientConfig, schedulerConfig : T) {
        this.table = table;
        this.connectionConfig = connectionConfig;
        this.schedulerConfig = schedulerConfig;
        this.schema = schema;
    }

    protected async init () {
        try {
            this.client = new Client(this.connectionConfig);
            await this.client.connect();
        } catch (e) {
            throw new Error("Error connecting to DB!");
        }

        const indexName = this.client.escapeIdentifier(`job_scheduler_${this.table}_idx`);
        this.table = this.client.escapeIdentifier(this.table);
        this.schema = this.client.escapeIdentifier(this.schema);

        try {
            await this.client.query(`CREATE SCHEMA IF NOT EXISTS ${this.schema}`, []);
            await this.client.query(`SET search_path TO ${this.schema};`, []);

            await this.client.query(
                `CREATE TABLE IF NOT EXISTS ${this.table} (
                    order_id      BIGSERIAL,
                    job_id        TEXT        NOT NULL UNIQUE,
                    data          JSONB       NOT NULL,
                    taken_by      TEXT        NOT NULL DEFAULT '',
                    taken_until   TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
                    attempts      INT         NOT NULL DEFAULT 0
                );`,
                []
            );

            await this.client.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${this.table} (order_id ASC, taken_until ASC);`, []);
        } catch (e) {
            console.error(e);
            throw new Error("Error initializing table!");
        }
    }
}

export default GenericClient;
