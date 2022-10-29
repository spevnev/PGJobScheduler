import {ClientConfig} from "pg";
import {v4 as generateUUID} from "uuid";
import GenericClient from "./client";
import dateToString from "./utils";

export type PublisherConfig = {
	batch_size?: number;
	batch_threshold?: number;
};

type Entry = {
	uuid: string;
	data: any;
	scheduleAt?: Date;
};

const DEFAULT_CONFIG: PublisherConfig = {
	batch_size: 100,
	batch_threshold: 333,
};

class Publisher extends GenericClient<PublisherConfig> {
	private batch: Entry[] = [];
	private batchTimeout: NodeJS.Timeout | null = null;

	constructor(table: string, connectionConfig: ClientConfig, schedulerConfig: PublisherConfig = {}, schema = "public") {
		super(table, schema, connectionConfig, {...DEFAULT_CONFIG, ...schedulerConfig});
	}

	private async _pub() {
		const values = this.batch
			.map(({data: _data, uuid, scheduleAt: _scheduleAt}) => {
				const data = this.client.escapeLiteral(JSON.stringify(_data));
				const scheduleAt = _scheduleAt ? `'${dateToString(_scheduleAt)}'` : "'-infinity'";
				return `('${uuid}',${data},${scheduleAt})`;
			})
			.join(", ");

		this.batch = [];
		this.batchTimeout = null;

		await this.client.query(`INSERT INTO ${this.table} (job_id, data, taken_until) VALUES ${values};`, []);
	}

	async pub(data: any, scheduleAt?: Date): Promise<string> {
		if (!this.client) await this.init();

		const uuid = generateUUID();
		this.batch.push({uuid, data, scheduleAt});

		if (this.batch.length === this.schedulerConfig.batch_size) {
			if (this.batchTimeout) clearTimeout(this.batchTimeout);
			await this._pub();
		}
		if (!this.batchTimeout) this.batchTimeout = setTimeout(this._pub.bind(this), this.schedulerConfig.batch_threshold);

		return uuid;
	}
}

export default Publisher;
