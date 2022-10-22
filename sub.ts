import {ClientConfig} from "pg";
import Subscriber from "./src/subscriber";

const CONNECTION_CONFIG: ClientConfig = {
	user: "root",
	host: "localhost",
	database: "job_scheduler",
	port: 5432,
};

const main = async () => {
	let i = 0;
	const callback = async (data: any) => {
		if (i === 0) console.time("sub");
		// if (data.num > 0.7 && Math.random() > 0.5) throw new Error("error");
		if (++i % 1000 === 0) console.timeLog("sub", i);
	};

	new Subscriber(callback, "test", CONNECTION_CONFIG).sub();
};

main();
