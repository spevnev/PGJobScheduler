import {ClientConfig} from "pg";
import {v4 as generateUUID} from "uuid";
import GenericClient from "./client";

type SubCallback = (data: any) => Promise<void>;

type Job = {
	data: any;
	job_id: string;
};

export type SubscriberConfig = {
	poll_delay?: number;
	poll_batch_size?: number;
	execution_batch_size?: number;
	job_length?: number;
	max_attempts?: number;
};

const DEFAULT_CONFIG: SubscriberConfig = {
	poll_delay: 1000,
	job_length: 3000,
	poll_batch_size: 200,
	execution_batch_size: 50,
	max_attempts: 5,
};

const POLL_JOBS_QUERY = (table: string) => `
WITH ready_jobs AS (
    SELECT *, (attempts < $1) as is_valid FROM ${table}
    WHERE taken_until < NOW()
    ORDER BY order_id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
), updating_jobs AS (
    UPDATE ${table} t
	SET attempts = (
        CASE jobs.taken_until != '-infinity' AND jobs.taken_until < NOW()
        WHEN true THEN jobs.attempts + 1
        ELSE jobs.attempts
        END
    ), 
    taken_until = (
        CASE jobs.is_valid
        WHEN true THEN NOW() + $3::INT * INTERVAL '1 millisecond'
        ELSE 'infinity'
        END
    ),
    order_id = (
        CASE jobs.is_valid
        WHEN true THEN jobs.order_id
        ELSE ${MAX_BIGINT}::BIGINT
        END
    ),
    taken_by = $4
	FROM ready_jobs AS jobs
	WHERE t.job_id = jobs.job_id
) 
SELECT job_id, data FROM ready_jobs 
WHERE is_valid = true;
`;

const UPDATE_JOBS_QUERY = (table: string, values: string) => `
UPDATE ${table} t
SET order_id = (
    CASE v.finished OR t.attempts = $1
    WHEN true THEN ${MAX_BIGINT}::BIGINT
    ELSE t.order_id
    END
), taken_until = (
    CASE v.finished OR t.attempts = $1
    WHEN true THEN 'infinity'
    ELSE t.taken_until
    END
)
FROM (VALUES ${values}) AS v(job_id, finished) 
WHERE t.job_id = v.job_id AND t.taken_by = $2;
`;

const MAX_BIGINT = "9223372036854775807";

class Subscriber extends GenericClient<SubscriberConfig> {
	private id: string;
	private callback: SubCallback;

	constructor(callback: SubCallback, table: string, connectionConfig: ClientConfig, schedulerConfig: SubscriberConfig = {}, schema = "public") {
		super(table, schema, connectionConfig, {...DEFAULT_CONFIG, ...schedulerConfig});
		this.callback = callback;
		this.id = generateUUID();

		if (
			this.schedulerConfig.execution_batch_size &&
			this.schedulerConfig.poll_batch_size &&
			this.schedulerConfig.execution_batch_size > this.schedulerConfig.poll_batch_size
		)
			throw new Error("Execution batch size can't be less than poll batch size!");
	}

	private async poll(): Promise<void> {
		const {execution_batch_size, poll_batch_size, poll_delay, max_attempts, job_length} = this.schedulerConfig;
		if (!execution_batch_size || !poll_batch_size) throw new Error("Error: no execution/poll batch size!");

		const jobs: Job[] = (await this.client.query(POLL_JOBS_QUERY(this.table), [max_attempts, poll_batch_size, job_length, this.id])).rows;

		for (let i = 0; i < Math.ceil(jobs.length / execution_batch_size); i++) {
			const finishedJobs = new Set<string>();

			const job_batch = jobs.slice(i * execution_batch_size, (i + 1) * execution_batch_size);
			await Promise.all(
				job_batch.map(async ({data, job_id}) => {
					try {
						await this.callback(data);
						finishedJobs.add(job_id);
					} catch (e) {
						return;
					}
				})
			);

			const values = jobs.map(({job_id}) => `('${job_id}', ${finishedJobs.has(job_id)})`).join(",");
			await this.client.query(UPDATE_JOBS_QUERY(this.table, values), [max_attempts, this.id]);
		}

		if (jobs.length > 0) this.poll();
		else setTimeout(this.poll.bind(this), poll_delay);
	}

	async sub() {
		if (!this.client) await this.init();
		this.poll();
	}
}

export default Subscriber;
