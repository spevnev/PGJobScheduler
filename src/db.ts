import {Client} from "pg";

export const initDB = async (client: Client, table: string) => {
	try {
		await client.query(
			`CREATE TABLE IF NOT EXISTS ${table} (
                order_id      BIGSERIAL,
                job_id        TEXT      NOT NULL UNIQUE,
                data          JSONB     NOT NULL,
                taken_by      TEXT      NOT NULL DEFAULT '',
                taken_until   TIMESTAMP NOT NULL DEFAULT '-infinity',
                attempts      INT       NOT NULL DEFAULT 0
            );`,
			[]
		);

		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_idx ON ${table} (order_id ASC, taken_until ASC);`, []);
	} catch (e) {
		throw new Error("Error initializing table!");
	}
};
