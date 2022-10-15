import {Client, ClientConfig} from "pg";
import {initDB} from "./db";
import {v4 as generateUUID} from "uuid";

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

// poll | exec | rate
// 100    50     ~11k/s
// 200    50     ~17k/s
// 300    75     ~19k/s
// 500    199    ~22k/s
// 1000   100    ~27k/s
// 5000   100    ~35k/s
// 10000  2500   ~40k/s
// 50000  10000  ~60k/s

const DEFAULT_CONFIG: SubscriberConfig = {
	poll_delay: 1000,
	job_length: 3000,
	poll_batch_size: 200,
	execution_batch_size: 50,
	retries_num: 5,
};

class Subscriber {
	private connectionConfig?: ClientConfig;
	private schedulerConfig: SubscriberConfig;

	private client!: Client;
	private table: string;

	private id!: string;
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
                e as (SELECT * FROM ${this.table} WHERE taken_until < NOW()::TIMESTAMP AND finished = false AND failures <= $1 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED), 
                _ as (UPDATE ${this.table} t SET taken_until = (SELECT NOW()::TIMESTAMP + $3::INT * INTERVAL '1 millisecond'), taken_by = $4 FROM e WHERE t.uuid = e.uuid), 
                f as (SELECT uuid FROM e WHERE taken_until != '-infinity' AND taken_until < NOW()::TIMESTAMP), 
                __ as (UPDATE ${this.table} t SET failures = t.failures + 1 FROM f WHERE t.uuid = f.uuid) 
                SELECT data, uuid, failures FROM e;`,
				[this.schedulerConfig.retries_num, this.schedulerConfig.poll_batch_size, this.schedulerConfig.job_length, this.id]
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
			await this.client.query(
				`UPDATE ${this.table} t SET failures = v.failures, finished = v.finished FROM (VALUES ${string}) AS v(uuid, finished, failures) WHERE t.uuid = v.uuid AND t.taken_by = $1;`,
				[this.id]
			);
		}

		if (jobs.length === this.schedulerConfig.poll_batch_size) this.poll();
		else setTimeout(this.poll.bind(this), this.schedulerConfig.poll_delay);
	}

	async sub() {
		if (!this.client) await this.init();
		if (!this.id) this.id = generateUUID();
		this.poll();
	}
}

export default Subscriber;
