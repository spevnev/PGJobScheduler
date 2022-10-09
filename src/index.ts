import {ClientConfig} from "pg";
import JobScheduler, {SchedulerConfig} from "./client";

const CONNECTION_CONFIG: ClientConfig = {
	user: "root",
	host: "localhost",
	database: "job_scheduler",
	port: 5432,
};

const SCHEDULER_CONFIG: SchedulerConfig = {
	sub_batch_size: 2,
	poll_delay: 1000,
};

const main = async () => {
	const client = new JobScheduler("test", CONNECTION_CONFIG, SCHEDULER_CONFIG);
};

main();
