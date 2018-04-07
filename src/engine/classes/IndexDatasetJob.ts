import { IJob } from "../services/job-queue";
import uuid = require("uuid/v1");

export class IndexDatasetJob implements IJob {
  public type: string;
  public messageId: string;
  public datasets: any[];

  constructor(datasets: any[]) {
    this.type = "IndexDataset";
    this.messageId = uuid();
    this.datasets = datasets;
  }
}
