import {Client} from "pg";

export const initDB = async (client: Client, table: string) => {
	try {
		await client.query(
			`CREATE TABLE IF NOT EXISTS ${table} (
                id            BIGSERIAL,
                uuid          TEXT      NOT NULL UNIQUE,
                data          JSONB     NOT NULL,
                taken_by      TEXT      NOT NULL DEFAULT '',
                taken_until   TIMESTAMP NOT NULL DEFAULT '-infinity',
                finished      BOOLEAN   NOT NULL DEFAULT false,
                failures      INT       NOT NULL DEFAULT 0
            );`,
			[]
		);

		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_id_idx ON ${table} (id ASC);`, []);
		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_finished_idx ON ${table} (finished);`, []);
		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_taken_until_idx on ${table} (taken_until);`, []);
	} catch (e) {
		throw new Error("Error initializing table!");
	}
};
