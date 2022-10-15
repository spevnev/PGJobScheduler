import {Client} from "pg";

export const initDB = async (client: Client, table: string) => {
	try {
		await client.query(
			`CREATE TABLE IF NOT EXISTS ${table} (
                uuid          TEXT      NOT NULL UNIQUE,
                id            BIGSERIAL,
                data          JSONB     NOT NULL,
                claimed_until TIMESTAMP NOT NULL DEFAULT '-infinity',
                finished      BOOLEAN   NOT NULL DEFAULT false,
                failures      INT       NOT NULL DEFAULT 0
            );`,
			[]
		);

		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_id_idx ON ${table} (id ASC);`, []);
		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_finished_idx ON ${table} (finished);`, []);
	} catch (e) {
		throw new Error("Error initializing table!");
	}
};
