import {Client, ClientConfig} from "pg";
import {initDB} from "./db";

type SubCallback = (data: any) => Promise<void>;

type Job = {
	data: any;
	uuid: string;
	failures: number;
};

export type SubscriberConfig = {
	poll_delay?: number;
	poll_batch_size?: number;
	execution_batch_size?: number;
	job_length?: number;
	retries_num?: number;
};

const DEFAULT_CONFIG: SubscriberConfig = {
	poll_delay: 1000,
	job_length: 3000,
	poll_batch_size: 300,
	execution_batch_size: 75,
	retries_num: 5,
};

class Subscriber {
	private connectionConfig?: ClientConfig;
	private schedulerConfig: SubscriberConfig;

	private client!: Client;
	private table: string;

	private callback: SubCallback;

	constructor(callback: SubCallback, table: string, connectionConfig: ClientConfig, schedulerConfig: SubscriberConfig = {}) {
		this.callback = callback;
		this.table = table;
		this.connectionConfig = connectionConfig;
		this.schedulerConfig = {...DEFAULT_CONFIG, ...schedulerConfig};

		if (
			this.schedulerConfig.execution_batch_size &&
			this.schedulerConfig.poll_batch_size &&
			this.schedulerConfig.execution_batch_size > this.schedulerConfig.poll_batch_size
		)
			throw new Error("Execution batch size can't be less than poll batch size!");
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
		if (!this.schedulerConfig.execution_batch_size || !this.schedulerConfig.poll_batch_size) throw new Error("Error: no execution/poll batch size!");

		const jobs: Job[] = (
			await this.client.query(
				`WITH 
                e as (SELECT * FROM ${this.table} WHERE claimed_until < NOW()::TIMESTAMP AND finished = false AND failures <= $1 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED), 
                _ as (UPDATE ${this.table} t SET claimed_until = (SELECT NOW()::TIMESTAMP + $3::INT * INTERVAL '1 millisecond') FROM e WHERE t.uuid = e.uuid), 
                f as (SELECT uuid FROM e WHERE claimed_until != '-infinity' AND claimed_until < NOW()::TIMESTAMP), 
                __ as (UPDATE ${this.table} t SET failures = t.failures + 1 FROM f WHERE t.uuid = f.uuid) 
                SELECT data, uuid, failures FROM e;`,
				[this.schedulerConfig.retries_num, this.schedulerConfig.poll_batch_size, this.schedulerConfig.job_length]
			)
		).rows;

		for (let i = 0; i < Math.ceil(jobs.length / this.schedulerConfig.execution_batch_size); i++) {
			const failed: Job[] = [];
			const finished: Job[] = [];

			const job_batch = jobs.slice(i * this.schedulerConfig.execution_batch_size, (i + 1) * this.schedulerConfig.execution_batch_size);
			await Promise.all(
				job_batch.map(async job => {
					try {
						await this.callback(job.data);
						finished.push(job);
					} catch (e) {
						failed.push(job);
					}
				})
			);

			const values: string[] = [];
			for (let j = 0; j < finished.length; j++) values.push(`('${finished[j].uuid}', true, ${finished[j].failures})`);
			for (let j = 0; j < failed.length; j++) values.push(`('${failed[j].uuid}', false, ${failed[j].failures + 1})`);

			const string = values.join(",");
			try {
				await this.client.query(
					`UPDATE ${this.table} t SET failures = v.failures, finished = v.finished FROM (VALUES ${string}) as v(uuid, finished, failures) WHERE t.uuid = v.uuid;`
				);
			} catch (e) {
				console.log();
			}
		}

		if (jobs.length === this.schedulerConfig.poll_batch_size) this.poll();
		else setTimeout(this.poll.bind(this), this.schedulerConfig.poll_delay);
	}

	async sub() {
		if (!this.client) await this.init();
		this.poll();
	}
}

export default Subscriber;
