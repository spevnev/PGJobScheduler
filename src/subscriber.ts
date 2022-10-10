import {Client, ClientConfig} from "pg";
import {initDB} from "./db";

type DataObject = {[key: string]: any};

type SubCallback = (data: any) => Promise<void>;

type Job = {
	data: DataObject;
	uuid: string;
	state: string;
	claimed_until: number;
	retries: number;
};

export type SubscriberConfig = {
	poll_delay?: number;
	batch_size?: number;
	job_length?: number;
	retries_num?: number;
};

const DEFAULT_CONFIG: SubscriberConfig = {
	poll_delay: 1000,
	job_length: 3000,
	batch_size: 5000,
	retries_num: 3,
};

class Subscriber {
	private connectionConfig?: ClientConfig;
	private schedulerConfig: SubscriberConfig;

	private client!: Client;
	private table!: string;

	private callback: SubCallback;

	constructor(callback: SubCallback, table: string, connectionConfig: ClientConfig, schedulerConfig: SubscriberConfig = {}) {
		this.callback = callback;
		this.table = table;
		this.connectionConfig = connectionConfig;
		this.schedulerConfig = {...DEFAULT_CONFIG, ...schedulerConfig};
	}

	private async init() {
		try {
			this.client = new Client(this.connectionConfig);
			await this.client.connect();
		} catch (e) {
			throw new Error("Error connecting to DB!");
		}

		this.table = this.client.escapeIdentifier(this.table);
		await initDB(this.client, this.table);
	}

	private async poll(): Promise<void> {
		let jobs: Job[];
		try {
			await this.client.query(`BEGIN;`);

			const query = await this.client.query(
				`SELECT * FROM ${this.table} 
                WHERE claimed_until < NOW() AND (state = 'READY' or (state = 'FAILED' and retries < $1)) 
                ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED;`,
				[this.schedulerConfig.retries_num, this.schedulerConfig.batch_size]
			);

			jobs = query.rows;

			const expired_uuids = jobs.filter(({claimed_until}) => claimed_until !== -Infinity && claimed_until.valueOf() < Date.now()).map(({uuid}) => uuid);
			await this.client.query(`UPDATE ${this.table} SET state = 'FAILED', retries = retries + 1 WHERE uuid = ANY($1);`, [expired_uuids]);

			const uuids = jobs.map(({uuid}) => uuid);
			await this.client.query(`UPDATE ${this.table} SET claimed_until = (select now() + $1::INT * interval '1 millisecond') WHERE uuid = ANY($2);`, [
				this.schedulerConfig.job_length,
				uuids,
			]);

			await this.client.query(`COMMIT;`);
		} catch (e) {
			console.error(e);
			await this.client.query(`ROLLBACK;`);
			return;
		}

		await Promise.all(
			jobs.map(async ({uuid, data, state}) => {
				try {
					await this.callback(data);
					await this.client.query(`UPDATE ${this.table} SET state = 'FINISHED' WHERE uuid = $1;`, [uuid]);
				} catch (e) {
					if (state === "FAILED") await this.client.query(`UPDATE ${this.table} SET retries = retries + 1 WHERE uuid = $1;`, [uuid]);
					else await this.client.query(`UPDATE ${this.table} SET state = 'FAILED' WHERE uuid = $1;`, [uuid]);
				}
			})
		);

		if (jobs.length === this.schedulerConfig.batch_size) this.poll();
		else setTimeout(this.poll.bind(this), this.schedulerConfig.poll_delay);
	}

	async sub() {
		if (!this.client) await this.init();
		this.poll();
	}
}

export default Subscriber;
