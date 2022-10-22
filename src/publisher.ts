import {Client, ClientConfig} from "pg";
import {v4 as generateUUID} from "uuid";
import {initDB} from "./db";

export type PublisherConfig = {
	batch_size?: number;
	batch_threshold?: number;
};

const DEFAULT_CONFIG: PublisherConfig = {
	batch_size: 100,
	batch_threshold: 333,
};

class Publisher {
	private schedulerConfig: PublisherConfig;
	private connectionConfig?: ClientConfig;

	private client!: Client;
	private table: string;

	private batch: {uuid: string; data: any}[] = [];
	private batchTimeout: NodeJS.Timeout | null = null;

	constructor(table: string, connectionConfig: ClientConfig, schedulerConfig: PublisherConfig = {}) {
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

	private async _pub() {
		const values = this.batch.map(({data, uuid}) => `('${uuid}', ${this.client.escapeLiteral(JSON.stringify(data))})`).join(", ");
		this.batch = [];
		this.batchTimeout = null;

		await this.client.query(`INSERT INTO ${this.table} (job_id, data) VALUES ${values};`, []);
	}

	async pub(data: any): Promise<string> {
		if (!this.client) await this.init();

		const uuid = generateUUID();
		this.batch.push({uuid, data});

		if (this.batch.length === this.schedulerConfig.batch_size) {
			if (this.batchTimeout) clearTimeout(this.batchTimeout);
			await this._pub();
		}
		if (!this.batchTimeout) this.batchTimeout = setTimeout(this._pub.bind(this), this.schedulerConfig.batch_threshold);

		return uuid;
	}
}

export default Publisher;
