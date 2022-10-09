import {Client, ClientConfig} from "pg";
import {v4 as generateUUID} from "uuid";

type DataObject = {[key: string]: any};

type SubCallback = (data: any) => Promise<void>;

type Job = {
	data: DataObject;
	uuid: string;
	state: string;
	claimed_until: number;
	retries: number;
};

export type SchedulerConfig = {
	poll_delay?: number;
	sub_batch_size?: number;
	pub_batch_size?: number;
	pub_batching_threshold?: number;
	job_length?: number;
	retries_num?: number;
};

const DEFAULT_CONFIG: SchedulerConfig = {
	poll_delay: 750,
	job_length: 2500,
	sub_batch_size: 1024,
	pub_batch_size: 1024,
	pub_batching_threshold: 500,
	retries_num: 3,
};

class JobScheduler {
	private connectionConfig?: ClientConfig;
	private pubClient!: Client;
	private subClient!: Client;
	private table!: string;
	private schedulerConfig: SchedulerConfig;
	private batch: {uuid: string; data: DataObject}[] = [];
	private batchTimeout: NodeJS.Timeout | null = null;

	constructor(table: string, connectionConfig: ClientConfig, schedulerConfig: SchedulerConfig = {}) {
		this.table = table;
		this.connectionConfig = connectionConfig;
		this.schedulerConfig = {...DEFAULT_CONFIG, ...schedulerConfig};
	}

	private async init_db() {
		try {
			await this.pubClient.query(`
                DO $$ BEGIN
                    CREATE TYPE states AS ENUM ('READY', 'FINISHED', 'FAILED');
                EXCEPTION
                    WHEN DUPLICATE_OBJECT THEN null;
                END $$;
            `);

			await this.pubClient.query(
				`CREATE TABLE IF NOT EXISTS ${this.table} (
                    uuid          TEXT      NOT NULL UNIQUE,
                    id            BIGSERIAL,
                    data          jsonb     NOT NULL,
                    claimed_until TIMESTAMP NOT NULL DEFAULT '-infinity',
                    state         states    NOT NULL DEFAULT 'READY',
                    retries       INT       NOT NULL DEFAULT 0
                );`,
				[]
			);

			await this.pubClient.query(`CREATE INDEX IF NOT EXISTS job_scheduler_id_idx ON ${this.table} (id ASC);`, []);
			await this.pubClient.query(`CREATE INDEX IF NOT EXISTS job_scheduler_state_idx ON ${this.table} (state);`, []);
		} catch (e) {
			throw new Error("Error initializing table!");
		}
	}

	private async init_clients() {
		this.pubClient = new Client(this.connectionConfig);
		this.subClient = new Client(this.connectionConfig);

		try {
			await this.pubClient.connect();
			await this.subClient.connect();
		} catch (e) {
			throw new Error("Error connecting to DB!");
		}
	}

	private async init() {
		await this.init_clients();
		this.table = this.pubClient.escapeIdentifier(this.table);
		await this.init_db();
	}

	private async poll(callback: SubCallback): Promise<void> {
		let jobs: Job[];
		try {
			await this.subClient.query(`BEGIN;`);

			const query = await this.subClient.query(
				`SELECT * FROM ${this.table} 
                WHERE claimed_until < NOW() AND (state = 'READY' or (state = 'FAILED' and retries < $1)) 
                ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED;`,
				[this.schedulerConfig.retries_num, this.schedulerConfig.sub_batch_size]
			);

			jobs = query.rows;

			const expired_uuids = jobs.filter(({claimed_until}) => claimed_until !== -Infinity && claimed_until.valueOf() < Date.now()).map(({uuid}) => uuid);
			await this.subClient.query(`UPDATE ${this.table} SET state = 'FAILED', retries = retries + 1 WHERE uuid = ANY($1);`, [expired_uuids]);

			const uuids = jobs.map(({uuid}) => uuid);
			await this.subClient.query(`UPDATE ${this.table} SET claimed_until = (select now() + $1::INT * interval '1 millisecond') WHERE uuid = ANY($2);`, [
				this.schedulerConfig.job_length,
				uuids,
			]);

			await this.subClient.query(`COMMIT;`);
		} catch (e) {
			console.error(e);
			await this.subClient.query(`ROLLBACK;`);
			return;
		}

		await Promise.all(
			jobs.map(async ({uuid, data, state}) => {
				try {
					await callback(data);
					await this.subClient.query(`UPDATE ${this.table} SET state = 'FINISHED' WHERE uuid = $1;`, [uuid]);
				} catch (e) {
					if (state === "FAILED") await this.subClient.query(`UPDATE ${this.table} SET retries = retries + 1 WHERE uuid = $1;`, [uuid]);
					else await this.subClient.query(`UPDATE ${this.table} SET state = 'FAILED' WHERE uuid = $1;`, [uuid]);
				}
			})
		);

		if (jobs.length === this.schedulerConfig.sub_batch_size) this.poll(callback);
		else setTimeout(() => this.poll(callback), this.schedulerConfig.poll_delay);
	}

	private async _pub() {
		console.log(`Published ${this.batch.length}`);
		const values = this.batch.map(({data, uuid}) => `('${uuid}', ${this.pubClient.escapeLiteral(JSON.stringify(data))})`).join(", ");
		this.batch = [];
		this.batchTimeout = null;

		this.pubClient.query(`INSERT INTO ${this.table}(uuid, data) VALUES ${values};`, []);
	}

	async pub(data: DataObject): Promise<string> {
		if (!this.pubClient) await this.init();

		const uuid = generateUUID();
		this.batch.push({uuid, data});

		if (this.batch.length === this.schedulerConfig.pub_batch_size) {
			if (this.batchTimeout) clearTimeout(this.batchTimeout);
			this._pub();
		}
		if (!this.batchTimeout) this.batchTimeout = setTimeout(this._pub.bind(this), this.schedulerConfig.pub_batching_threshold);

		return uuid;
	}

	async sub(callback: SubCallback) {
		if (!this.subClient) await this.init();
		this.poll(callback);
	}
}

export default JobScheduler;
