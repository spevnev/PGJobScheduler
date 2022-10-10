import {ClientConfig} from "pg";
import Subscriber from "./src/subscriber";

const CONNECTION_CONFIG: ClientConfig = {
	user: "root",
	host: "localhost",
	database: "job_scheduler",
	port: 5432,
};

const main = async () => {
	const callback = async (data: any) => {
		await new Promise(res => setTimeout(() => res(null), 50));
		console.log(`Processed ${data.num}`);
	};

	const sub = new Subscriber(callback, "test", CONNECTION_CONFIG, {});
	sub.sub();
};

main();
