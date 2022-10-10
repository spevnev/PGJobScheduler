import {Client} from "pg";

export const initDB = async (client: Client, table: string) => {
	try {
		await client.query(`
            DO $$ BEGIN
                CREATE TYPE states AS ENUM ('READY', 'FINISHED', 'FAILED');
            EXCEPTION
                WHEN DUPLICATE_OBJECT THEN null;
            END $$;
        `);

		await client.query(
			`CREATE TABLE IF NOT EXISTS ${table} (
                uuid          TEXT      NOT NULL UNIQUE,
                id            BIGSERIAL,
                data          jsonb     NOT NULL,
                claimed_until TIMESTAMP NOT NULL DEFAULT '-infinity',
                state         states    NOT NULL DEFAULT 'READY',
                retries       INT       NOT NULL DEFAULT 0
            );`,
			[]
		);

		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_id_idx ON ${table} (id ASC);`, []);
		await client.query(`CREATE INDEX IF NOT EXISTS job_scheduler_state_idx ON ${table} (state);`, []);
	} catch (e) {
		throw new Error("Error initializing table!");
	}
};
