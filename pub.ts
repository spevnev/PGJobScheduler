import {ClientConfig} from "pg";
import Publisher from "./src/publisher";

const CONNECTION_CONFIG: ClientConfig = {
	user: "root",
	host: "localhost",
	database: "job_scheduler",
	port: 5432,
};

const main = async () => {
	const pub = new Publisher("test", CONNECTION_CONFIG, {});

	const arr: Promise<string>[] = [];
	for (let i = 0; i < 10 ** 5; i++) arr.push(pub.pub({num: Math.random()}));
};

main();
